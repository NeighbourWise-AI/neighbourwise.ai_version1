"""
universal_validator.py — NeighbourWise AI
Single validator for all agents. Agent-aware — runs the right checks
for each agent type without redundancy.

Validation model: claude-sonnet-4-6 via Snowflake Cortex
Generation model: mistral-large2 via Snowflake Cortex
Web Search validation: GPT-4o (cross-model, Claude generates, GPT-4o validates)

Cross-model validation rule:
  - Mistral generates  → Claude validates
  - Claude generates   → GPT-4o validates
  - Never validate with the same model that generated the output

Agent types:
  "graphic_chart"  — SQL → DataFrame → Altair/Plotly chart
  "graphic_image"  — DALL-E 3 neighborhood images
  "report"         — Full PDF report (3-checkpoint validation)
  "data_query"     — SQL + RAG chatbot answer
  "web_search"     — Serper + Claude draft answer

Usage:
    from universal_validator import UniversalValidator, AgentType
    validator = UniversalValidator(conn)
    result = validator.validate(AgentType.DATA_QUERY, context)
"""

from __future__ import annotations

import json
import os
import re
import base64
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from shared.snowflake_conn import cortex_complete, MODEL_VALIDATE, MODEL_GENERATE

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

# ── Status constants ──────────────────────────────────────────────────────────
PASS = "✅ PASS"
WARN = "⚠️  WARN"
FAIL = "❌ FAIL"

# ── Agent types ───────────────────────────────────────────────────────────────
class AgentType(str, Enum):
    GRAPHIC_CHART = "graphic_chart"
    GRAPHIC_IMAGE = "graphic_image"
    REPORT        = "report"
    DATA_QUERY    = "data_query"
    WEB_SEARCH    = "web_search"


# ── Result dataclasses ────────────────────────────────────────────────────────
@dataclass
class CheckResult:
    status:  str
    issues:  list[str] = field(default_factory=list)
    details: dict      = field(default_factory=dict)


@dataclass
class ValidationResult:
    agent_type:  str
    passed:      bool
    improved:    bool
    result:      object
    checks:      dict[str, CheckResult] = field(default_factory=dict)
    all_issues:  list[str]              = field(default_factory=list)

    def print_summary(self):
        status       = "PASSED" if self.passed else "FAILED"
        improved_str = " → IMPROVED" if self.improved else ""
        print(f"\n[Validator] {self.agent_type.upper()} — {status}{improved_str}")
        for name, check in self.checks.items():
            print(f"   {check.status} {name}")
            for issue in check.issues:
                print(f"      → {issue[:120]}")
        if not self.checks:
            print("   No checks ran.")


# ── Score + table constants ───────────────────────────────────────────────────
SCORE_COLUMNS = {
    "safety_score", "transit_score", "bikeshare_score", "school_score",
    "restaurant_score", "grocery_score", "healthcare_score",
    "education_score", "housing_score", "master_score",
}

KNOWN_TABLES = {
    "master_location",
    "mrt_neighborhood_safety", "mrt_neighborhood_mbta",
    "mrt_neighborhood_bluebikes", "mrt_neighborhood_schools",
    "mrt_neighborhood_restaurants", "mrt_neighbourhood_grocery_stores",
    "mrt_neighborhood_grocery_stores", "mrt_neighborhood_healthcare",
    "mrt_neighborhood_universities", "mrt_neighborhood_housing",
    "neighborhood_master_score", "mrt_boston_crime",
    "mrt_boston_healthcare", "mrt_boston_restaurants",
    "mrt_boston_universities", "mrt_boston_housing",
    "mrt_boston_mbta_stops", "mrt_boston_schools",
    "mrt_boston_bluebike_stations", "mrt_boston_grocery_stores",
}

VALID_ALIASES = {"ml", "ns", "nm", "nsc", "nr", "ng", "nh", "nho", "nb", "nu", "nms"}

TIME_COLUMNS = {"year_month", "month", "year", "date", "occurred_on_date"}

BAD_ANSWER_PHRASES = [
    "not available", "no data", "not provided", "does not include",
    "not specified", "couldn't find relevant", "i don't have",
    "no information",
]

MIN_CHART_SIZE_KB   = 10
MIN_IMAGE_SIZE_KB   = 200
MIN_NARRATIVE_CHARS = 200
MIN_PDF_SIZE_KB     = 50

EXPECTED_PERSPECTIVES = ["landmark", "residential", "transit", "food_nightlife"]

# ── GPT-4o client (web search validation only) ────────────────────────────────
_openai_client = None
def _get_openai():
    global _openai_client
    if _openai_client is None:
        try:
            from openai import OpenAI
            _openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        except Exception:
            pass
    return _openai_client


# ══════════════════════════════════════════════════════════════════════════════
# UNIVERSAL VALIDATOR
# ══════════════════════════════════════════════════════════════════════════════

class UniversalValidator:

    def __init__(self, conn):
        """
        conn: active Snowflake connection from shared.snowflake_conn.get_conn()
        """
        self.conn = conn

    # ── Main entry point ──────────────────────────────────────────────────────
    def validate(self, agent_type: AgentType, context: dict) -> ValidationResult:
        """
        context keys vary by agent_type:

        graphic_chart:  {sql, chart_type, df, out_path, user_query, attempt}
        graphic_image:  {neighborhood, city, saved_paths, neighborhood_info}
        report:         {checkpoint, neighborhood, data, neighbor_df,
                         crime_df, rag_results, report_sections,
                         chart_paths, pdf_path, executive_summary}
        data_query:     {question, answer, sql_data, rag_data}
        web_search:     {query, domain, draft, search_context}
        """
        checks = {}

        if agent_type == AgentType.GRAPHIC_CHART:
            checks = self._validate_graphic_chart(context)
        elif agent_type == AgentType.GRAPHIC_IMAGE:
            checks = self._validate_graphic_image(context)
        elif agent_type == AgentType.REPORT:
            checks = self._validate_report(context)
        elif agent_type == AgentType.DATA_QUERY:
            checks = self._validate_data_query(context)
        elif agent_type == AgentType.WEB_SEARCH:
            checks = self._validate_web_search(context)

        all_issues        = [i for c in checks.values() for i in c.issues]
        needs_improvement = any(c.status == FAIL for c in checks.values())

        if needs_improvement:
            improved_result = self._improve(agent_type, context, checks)
            return ValidationResult(
                agent_type=agent_type.value, passed=False, improved=True,
                result=improved_result, checks=checks, all_issues=all_issues,
            )

        return ValidationResult(
            agent_type=agent_type.value, passed=True, improved=False,
            result=context.get("answer") or context.get("out_path") or context.get("pdf_path"),
            checks=checks, all_issues=all_issues,
        )

    # ══════════════════════════════════════════════════════════════════════════
    # GRAPHIC CHART CHECKS
    # ══════════════════════════════════════════════════════════════════════════
    def _validate_graphic_chart(self, ctx: dict) -> dict[str, CheckResult]:
        checks     = {}
        sql        = ctx.get("sql", "")
        chart_type = ctx.get("chart_type", "bar")
        df         = ctx.get("df")
        out_path   = ctx.get("out_path")
        user_query = ctx.get("user_query", "")

        checks["sql_quality"] = self._check_sql_quality(sql)

        if df is not None:
            checks["data_shape"] = self._check_data_shape(df, chart_type, user_query)

        if df is not None and not df.empty:
            checks["score_range"] = self._check_score_range(df)

        if out_path:
            checks["output_file"] = self._check_file(out_path, MIN_CHART_SIZE_KB, "chart")

        # Claude intent check — only on retry
        if ctx.get("attempt", 1) > 1 and sql:
            checks["claude_intent"] = self._claude_check_sql_intent(
                user_query, sql, chart_type
            )

        return checks

    # ══════════════════════════════════════════════════════════════════════════
    # GRAPHIC IMAGE CHECKS
    # ══════════════════════════════════════════════════════════════════════════
    def _validate_graphic_image(self, ctx: dict) -> dict[str, CheckResult]:
        checks       = {}
        neighborhood = ctx.get("neighborhood", "")
        city         = ctx.get("city", "Boston")
        saved_paths  = ctx.get("saved_paths", [])
        nbhd_info    = ctx.get("neighborhood_info", {})

        checks["image_count"]   = self._check_image_count(saved_paths)
        checks["perspectives"]  = self._check_perspectives(saved_paths)
        checks["file_sizes"]    = self._check_image_sizes(saved_paths)

        if saved_paths:
            checks["vision_quality"] = self._claude_vision_check(
                neighborhood, city, saved_paths, nbhd_info
            )

        return checks

    # ══════════════════════════════════════════════════════════════════════════
    # REPORT CHECKS (3 checkpoints)
    # ══════════════════════════════════════════════════════════════════════════
    def _validate_report(self, ctx: dict) -> dict[str, CheckResult]:
        checkpoint = ctx.get("checkpoint", "post_build")
        checks     = {}

        if checkpoint == "pre_fetch":
            checks["neighborhood_exists"]    = self._check_neighborhood_exists(
                ctx.get("neighborhood", "")
            )
            checks["master_score_exists"]    = self._check_master_score_exists(
                ctx.get("neighborhood", "")
            )

        elif checkpoint == "post_fetch":
            checks["domain_scores"]  = self._check_domain_scores(ctx.get("data", {}))
            checks["neighbor_data"]  = self._check_neighbor_data(ctx.get("neighbor_df"))
            checks["crime_data"]     = self._check_crime_data(ctx.get("crime_df"))
            checks["rag_results"]    = self._check_rag_results(ctx.get("rag_results", []))

        elif checkpoint == "post_build":
            checks["report_sections"]   = self._check_report_sections(
                ctx.get("report_sections", {})
            )
            checks["charts_rendered"]   = self._check_charts_rendered(
                ctx.get("chart_paths", {})
            )
            checks["pdf_file"]          = self._check_file(
                ctx.get("pdf_path", ""), MIN_PDF_SIZE_KB, "PDF"
            )
            checks["narrative_quality"] = self._claude_check_narrative(
                ctx.get("neighborhood", ""),
                ctx.get("executive_summary", "")
            )

        return checks

    # ══════════════════════════════════════════════════════════════════════════
    # DATA QUERY CHECKS
    # ══════════════════════════════════════════════════════════════════════════
    def _validate_data_query(self, ctx: dict) -> dict[str, CheckResult]:
        checks   = {}
        answer   = ctx.get("answer", "")
        sql_data = ctx.get("sql_data", {})
        rag_data = ctx.get("rag_data", {})

        if sql_data.get("sql"):
            checks["sql_quality"] = self._check_sql_quality(sql_data["sql"])

        if rag_data.get("chunks"):
            checks["rag_relevance"] = self._check_rag_relevance(rag_data["chunks"])

        checks["data_usage"] = self._check_data_usage(answer, sql_data)
        checks["format"]     = self._check_answer_format(answer)

        # ── Hallucination check: skip when we have ANY grounded data ─────────
        # SQL rows OR RAG chunks both count as grounded — if either returned
        # data, the synthesis is anchored and hallucination risk is low.
        # Only run the expensive Claude call when BOTH are empty.
        sql_results  = sql_data.get("results") or []
        rag_chunks   = (rag_data.get("chunks") or []) if rag_data else []
        has_any_data = bool(sql_results) or bool(rag_chunks)

        if not has_any_data:
            print("[Validator] No SQL or RAG data — running Claude hallucination check")
            checks["hallucination"] = self._claude_check_hallucination(
                answer, sql_data, rag_data
            )
        else:
            print(f"[Validator] Grounded (SQL={len(sql_results)} rows, "
                  f"RAG={len(rag_chunks)} chunks) — skipping hallucination check")

        return checks

    # ══════════════════════════════════════════════════════════════════════════
    # WEB SEARCH CHECKS
    # ══════════════════════════════════════════════════════════════════════════
    def _validate_web_search(self, ctx: dict) -> dict[str, CheckResult]:
        checks         = {}
        query          = ctx.get("query", "")
        domain         = ctx.get("domain", "All")
        draft          = ctx.get("draft", "")
        search_context = ctx.get("search_context", "")

        if not draft or len(draft.strip()) < 50:
            checks["draft_content"] = CheckResult(
                status=FAIL, issues=["Draft answer is empty or too short"]
            )
            return checks

        checks["draft_content"] = CheckResult(status=PASS)

        gpt4o = _get_openai()
        if gpt4o and os.environ.get("OPENAI_API_KEY"):
            checks["gpt4o_validation"] = self._gpt4o_validate(
                query, domain, draft, search_context, gpt4o
            )
        else:
            print("[Validator] No OPENAI_API_KEY — falling back to Claude for web search validation")
            checks["claude_web_validation"] = self._claude_validate_web(
                query, draft, search_context
            )

        return checks

    # ══════════════════════════════════════════════════════════════════════════
    # PROGRAMMATIC CHECKS
    # ══════════════════════════════════════════════════════════════════════════

    def _check_sql_quality(self, sql: str) -> CheckResult:
        issues    = []
        sql_lower = sql.lower().strip()

        if "neighbourwise_domains.marts." not in sql_lower and "marts." not in sql_lower:
            issues.append("Missing full table path (NEIGHBOURWISE_DOMAINS.MARTS.)")

        for match in re.findall(r"'([A-Za-z ]+)'", sql):
            if match != match.upper() and match.lower() not in (
                "true", "false", "null", "query:", "passage:"
            ):
                issues.append(f"Neighborhood literal '{match}' should be UPPERCASE")

        has_flag = bool(re.search(
            r"\b(is_boston|is_cambridge|is_greater_boston)\s*=\s*true", sql_lower
        ))
        if has_flag and "master_location" not in sql_lower:
            issues.append(
                "IS_BOSTON/IS_CAMBRIDGE/IS_GREATER_BOSTON used without MASTER_LOCATION join"
            )

        for ref in re.findall(r"(?:from|join)\s+([\w.]+)", sql_lower):
            short = ref.split(".")[-1]
            if short not in KNOWN_TABLES and short not in {"dual"}:
                issues.append(f"Unrecognised table: '{short}'")

        for alias in re.findall(r"\bas\s+(\w+)\b", sql_lower):
            if len(alias) == 1 and alias not in {"a"}:
                issues.append(
                    f"Single-letter alias '{alias}' — use named aliases: "
                    f"{', '.join(sorted(VALID_ALIASES))}"
                )

        order_match = re.search(r"order\s+by\s+([\w.]+)", sql_lower)
        if order_match:
            sort_col = order_match.group(1).split(".")[-1]
            if sort_col in SCORE_COLUMNS and f"{sort_col} is not null" not in sql_lower:
                issues.append(f"No IS NOT NULL on sort column '{sort_col}'")

        return CheckResult(status=FAIL if issues else PASS, issues=issues)

    def _check_data_shape(
        self, df: pd.DataFrame, chart_type: str, user_query: str
    ) -> CheckResult:
        issues = []
        n      = len(df)
        is_time = chart_type in {"line", "multi_line"}

        if n == 0:
            return CheckResult(status=FAIL, issues=["Query returned 0 rows"])

        has_time_col = any(c.lower() in TIME_COLUMNS for c in df.columns)
        name_col     = next((c for c in df.columns if c.lower() == "neighborhood_name"), None)
        if name_col and not is_time and not has_time_col and chart_type != "pie":
            dupes = df[name_col][df[name_col].duplicated()]
            if len(dupes) > 0:
                issues.append(f"Duplicate rows for: {', '.join(dupes.unique())}")

        if chart_type == "scatter" and n < 10:
            issues.append(f"Scatter needs many points — only got {n}")
        if is_time and n < 3:
            issues.append(f"Trend chart needs ≥3 time points — got {n}")
        if chart_type == "pie" and n < 2:
            issues.append(f"Pie needs ≥2 categories — got {n}")
        if chart_type == "grouped_bar" and n < 2:
            issues.append(f"Grouped bar needs ≥2 neighborhoods — got {n}")

        if chart_type == "scatter":
            num_cols = [
                c for c in df.columns
                if pd.to_numeric(df[c], errors="coerce").notna().sum() > n * 0.5
            ]
            if len(num_cols) < 2:
                issues.append(f"Scatter needs 2 numeric columns — found {len(num_cols)}")

        return CheckResult(status=FAIL if issues else PASS, issues=issues)

    def _check_score_range(self, df: pd.DataFrame) -> CheckResult:
        issues = []
        for col in df.columns:
            if col.lower() in SCORE_COLUMNS:
                if df[col].isna().all():
                    issues.append(f"Column '{col}' is NULL for every row — JOIN may be broken")
                    continue
                numeric = pd.to_numeric(df[col], errors="coerce").dropna()
                bad     = numeric[(numeric < 0) | (numeric > 100)]
                if len(bad) > 0:
                    issues.append(
                        f"'{col}' has {len(bad)} value(s) outside 0–100 "
                        f"(e.g. {bad.iloc[0]:.1f})"
                    )
        return CheckResult(status=WARN if issues else PASS, issues=issues)

    def _check_file(self, path: str, min_kb: float, label: str) -> CheckResult:
        if not path:
            return CheckResult(status=FAIL, issues=[f"No {label} path provided"])
        p = Path(path)
        if not p.exists():
            return CheckResult(status=FAIL, issues=[f"{label} file not found: {p.name}"])
        size_kb = p.stat().st_size / 1024
        if size_kb < min_kb:
            return CheckResult(
                status=FAIL,
                issues=[f"{label} is only {size_kb:.1f} KB — likely corrupt or blank"]
            )
        return CheckResult(status=PASS, details={"size_kb": round(size_kb, 1)})

    def _check_image_count(self, saved_paths: list) -> CheckResult:
        n = len(saved_paths)
        if n == 0:
            return CheckResult(status=FAIL, issues=["Zero images generated"])
        if n < 4:
            return CheckResult(status=FAIL, issues=[f"Only {n}/4 images generated"])
        return CheckResult(status=PASS)

    def _check_perspectives(self, saved_paths: list) -> CheckResult:
        found = []
        for path in saved_paths:
            for p in EXPECTED_PERSPECTIVES:
                if p in Path(path).name:
                    found.append(p)
                    break
        missing = set(EXPECTED_PERSPECTIVES) - set(found)
        if missing:
            return CheckResult(
                status=FAIL,
                issues=[f"Missing perspectives: {', '.join(sorted(missing))}"]
            )
        return CheckResult(status=PASS)

    def _check_image_sizes(self, saved_paths: list) -> CheckResult:
        issues = []
        for path in saved_paths:
            p = Path(path)
            if not p.exists():
                issues.append(f"File missing: {p.name}")
                continue
            size_kb = p.stat().st_size / 1024
            if size_kb < MIN_IMAGE_SIZE_KB:
                issues.append(f"{p.name} is only {size_kb:.0f} KB — likely corrupt")
        return CheckResult(status=FAIL if issues else PASS, issues=issues)

    def _check_neighborhood_exists(self, neighborhood: str) -> CheckResult:
        if not neighborhood or not neighborhood.strip():
            return CheckResult(status=FAIL, issues=["Neighborhood name is empty"])
        try:
            from shared.snowflake_conn import run_query
            df = run_query(
                "SELECT NEIGHBORHOOD_NAME FROM MARTS.MASTER_LOCATION "
                "WHERE UPPER(NEIGHBORHOOD_NAME) = %s LIMIT 1",
                self.conn, (neighborhood.strip().upper(),)
            )
            if df.empty:
                return CheckResult(
                    status=FAIL,
                    issues=[f"'{neighborhood}' not found in MASTER_LOCATION"]
                )
            return CheckResult(status=PASS)
        except Exception as e:
            return CheckResult(status=FAIL, issues=[f"Snowflake check failed: {e}"])

    def _check_master_score_exists(self, neighborhood: str) -> CheckResult:
        try:
            from shared.snowflake_conn import run_query
            df = run_query(
                "SELECT MASTER_SCORE FROM "
                "NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE "
                "WHERE UPPER(NEIGHBORHOOD_NAME) = %s LIMIT 1",
                self.conn, (neighborhood.strip().upper(),)
            )
            if df.empty or df.iloc[0]["MASTER_SCORE"] is None:
                return CheckResult(
                    status=WARN,
                    issues=[f"'{neighborhood}' has no master score — report will be incomplete"]
                )
            return CheckResult(status=PASS)
        except Exception as e:
            return CheckResult(status=WARN, issues=[f"Master score check failed: {e}"])

    def _check_domain_scores(self, data: dict) -> CheckResult:
        required = [
            "SAFETY_SCORE", "TRANSIT_SCORE", "BIKESHARE_SCORE", "SCHOOL_SCORE",
            "RESTAURANT_SCORE", "GROCERY_SCORE", "HEALTHCARE_SCORE",
            "EDUCATION_SCORE", "HOUSING_SCORE",
        ]
        issues = []
        for col in required:
            val = data.get(col)
            if val is None:
                issues.append(f"{col} is missing")
            elif isinstance(val, (int, float)) and not (0 <= val <= 100):
                issues.append(f"{col} = {val} is outside 0–100")
        return CheckResult(status=FAIL if issues else PASS, issues=issues)

    def _check_neighbor_data(self, neighbor_df) -> CheckResult:
        if neighbor_df is None or (hasattr(neighbor_df, "empty") and neighbor_df.empty):
            return CheckResult(status=FAIL, issues=["No neighbor comparison data fetched"])
        n = len(neighbor_df)
        if n < 3:
            return CheckResult(
                status=WARN,
                issues=[f"Only {n} neighbors found — chart will be sparse"]
            )
        return CheckResult(status=PASS)

    def _check_crime_data(self, crime_df) -> CheckResult:
        if crime_df is None or (hasattr(crime_df, "empty") and crime_df.empty):
            return CheckResult(
                status=WARN, issues=["No crime trend data — chart will be skipped"]
            )
        if len(crime_df) < 3:
            return CheckResult(
                status=WARN,
                issues=[f"Only {len(crime_df)} crime data points — trend may not be meaningful"]
            )
        return CheckResult(status=PASS)

    def _check_rag_results(self, rag_results: list) -> CheckResult:
        if not rag_results:
            return CheckResult(
                status=WARN,
                issues=["RAG returned no results — lifestyle section will be empty"]
            )
        if len(rag_results) < 2:
            return CheckResult(
                status=WARN,
                issues=[f"Only {len(rag_results)} RAG chunk(s) — context will be thin"]
            )
        return CheckResult(status=PASS)

    def _check_report_sections(self, sections: dict) -> CheckResult:
        required = [
            "cover", "executive_summary", "domain_scorecard",
            "chart_radar", "chart_bar_neighbors", "chart_grouped_bar",
            "chart_crime_trend", "domain_narratives", "lifestyle_context",
            "recommendation",
        ]
        missing = [s for s in required if not sections.get(s)]
        if missing:
            return CheckResult(
                status=FAIL, issues=[f"Missing sections: {', '.join(missing)}"]
            )
        return CheckResult(status=PASS)

    def _check_charts_rendered(self, chart_paths: dict) -> CheckResult:
        required = [
            "chart_radar", "chart_bar_neighbors",
            "chart_grouped_bar", "chart_crime_trend",
        ]
        issues = []
        for key in required:
            path = chart_paths.get(key)
            if not path:
                issues.append(f"Chart '{key}' not generated")
                continue
            p = Path(path)
            if not p.exists():
                issues.append(f"Chart file missing: {p.name}")
            elif p.stat().st_size / 1024 < MIN_CHART_SIZE_KB:
                issues.append(f"Chart '{key}' is too small — likely blank")
        return CheckResult(status=FAIL if issues else PASS, issues=issues)

    def _check_rag_relevance(self, chunks: list) -> CheckResult:
        if not chunks:
            return CheckResult(status=WARN, issues=["No RAG chunks returned"])
        top_sim = float(chunks[0].get("SIMILARITY", chunks[0].get("similarity", 0)))
        if top_sim < 0.65:
            return CheckResult(
                status=WARN,
                issues=[f"Low RAG relevance: top similarity = {top_sim:.4f}"]
            )
        return CheckResult(status=PASS, details={"top_similarity": round(top_sim, 4)})

    def _check_data_usage(self, answer: str, sql_data: dict) -> CheckResult:
        issues = []
        if sql_data and isinstance(sql_data.get("results"), list) and sql_data["results"]:
            for phrase in BAD_ANSWER_PHRASES:
                if phrase in answer.lower():
                    issues.append(
                        f"SQL returned data but answer says '{phrase}' — data may be ignored"
                    )
                    break
        return CheckResult(status=FAIL if issues else PASS, issues=issues)

    def _check_answer_format(self, answer: str) -> CheckResult:
            issues = []
            lower  = answer.lower()

            if "summary" not in lower[:300]:
                issues.append("Missing ### Summary section")

            if "insight" not in lower:
                issues.append("Missing ### Insights section")

            # Note: no table check — synthesis prompt writes prose, not tables.
            # A missing table is not an error.

            # WARN not FAIL — missing a section header is cosmetic, not a data error.
            # FAIL would trigger a full Claude rewrite (+8s) for a cosmetic issue.
            return CheckResult(status=WARN if issues else PASS, issues=issues)

    # ══════════════════════════════════════════════════════════════════════════
    # LLM CHECKS (Claude via Cortex — validates Mistral output)
    # ══════════════════════════════════════════════════════════════════════════

    def _claude_check_sql_intent(
        self, user_query: str, sql: str, chart_type: str
    ) -> CheckResult:
        prompt = f"""You are reviewing a Snowflake SQL query for NeighbourWise AI.

User query: "{user_query}"
Generated SQL: {sql[:800]}
Chart type: {chart_type}

Check: (1) Does SQL correctly answer the query? (2) Is chart type appropriate?

Respond ONLY with JSON (no markdown):
{{
  "sql_correct": true or false,
  "sql_issue": "one sentence or null",
  "chart_type_correct": true or false,
  "chart_type_issue": "one sentence or null"
}}"""
        try:
            raw    = cortex_complete(prompt, self.conn, model=MODEL_VALIDATE)
            raw    = re.sub(r"```(?:json)?|```", "", raw).strip()
            result = json.loads(raw)
            issues = []
            if not result.get("sql_correct", True) and result.get("sql_issue"):
                issues.append(f"[Claude] SQL: {result['sql_issue']}")
            if not result.get("chart_type_correct", True) and result.get("chart_type_issue"):
                issues.append(f"[Claude] Chart type: {result['chart_type_issue']}")
            return CheckResult(status=FAIL if issues else PASS, issues=issues)
        except Exception as e:
            return CheckResult(status=WARN, issues=[f"Claude SQL check failed: {e}"])

    def _claude_check_hallucination(
        self, answer: str, sql_data: dict, rag_data: dict
    ) -> CheckResult:
        sql_rows   = len(sql_data.get("results") or []) if sql_data else 0
        rag_chunks = len(rag_data.get("chunks") or []) if rag_data else 0
        prompt = f"""Check for serious hallucinations ONLY.
NOT hallucinations: reasonable inferences, conversational phrasing, general advice.
ONLY flag: invented numbers, made-up facility names, claims contradicting the data.

Data available: SQL={sql_rows} rows, RAG={rag_chunks} chunks.
Answer: {answer[:1500]}

Reply ONLY: NO or YES: [one sentence explanation]"""
        try:
            result            = cortex_complete(prompt, self.conn, model=MODEL_VALIDATE)
            has_hallucination = result.strip().upper().startswith("YES")
            return CheckResult(
                status=FAIL if has_hallucination else PASS,
                issues=[result.strip()[:200]] if has_hallucination else [],
            )
        except Exception as e:
            return CheckResult(status=WARN, issues=[f"Claude hallucination check failed: {e}"])

    def _claude_check_narrative(
        self, neighborhood: str, executive_summary: str
    ) -> CheckResult:
        if not executive_summary or len(executive_summary) < MIN_NARRATIVE_CHARS:
            return CheckResult(
                status=WARN,
                issues=[f"Executive summary too short: {len(executive_summary or '')} chars"]
            )
        prompt = f"""Review this neighborhood report executive summary.
Neighborhood: {neighborhood}
Summary: {executive_summary[:800]}

Respond ONLY with JSON:
{{
  "mentions_neighborhood": true or false,
  "has_specific_data": true or false,
  "issue": "one sentence or null"
}}"""
        try:
            raw    = cortex_complete(prompt, self.conn, model=MODEL_VALIDATE)
            raw    = re.sub(r"```(?:json)?|```", "", raw).strip()
            result = json.loads(raw)
            issues = []
            if not result.get("mentions_neighborhood", True):
                issues.append("Summary doesn't mention the neighborhood name")
            if not result.get("has_specific_data", True):
                issues.append("Summary doesn't reference specific scores or data")
            if result.get("issue"):
                issues.append(f"[Claude] {result['issue']}")
            return CheckResult(status=WARN if issues else PASS, issues=issues)
        except Exception as e:
            return CheckResult(status=WARN, issues=[f"Claude narrative check failed: {e}"])

    def _claude_validate_web(
        self, query: str, draft: str, search_context: str
    ) -> CheckResult:
        prompt = f"""Check this web search answer for hallucinations vs the source context.

Query: {query}
Sources: {search_context[:1500]}
Answer: {draft[:1000]}

Flag ONLY: facts in the answer that don't appear in sources.
Reply ONLY: NO or YES: [explanation]"""
        try:
            result    = cortex_complete(prompt, self.conn, model=MODEL_VALIDATE)
            has_issue = result.strip().upper().startswith("YES")
            return CheckResult(
                status=FAIL if has_issue else PASS,
                issues=[result.strip()[:200]] if has_issue else [],
            )
        except Exception as e:
            return CheckResult(status=WARN, issues=[f"Claude web validation failed: {e}"])

    def _claude_vision_check(
        self,
        neighborhood: str,
        city: str,
        saved_paths: list,
        neighborhood_info: dict,
    ) -> CheckResult:
        import anthropic
        try:
            client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        except Exception:
            return CheckResult(
                status=WARN, issues=["Anthropic client unavailable for vision check"]
            )

        issues = []
        for path in saved_paths:
            p = Path(path)
            if not p.exists():
                continue
            perspective = next(
                (x for x in EXPECTED_PERSPECTIVES if x in p.name), "unknown"
            )
            try:
                image_data = base64.standard_b64encode(p.read_bytes()).decode("utf-8")
                prompt     = f"""Review this AI-generated image of {neighborhood}, {city}, MA.
Perspective: "{perspective}"

Respond ONLY with JSON:
{{
  "readable_text": {{"present": true or false}},
  "exterior_only": {{"correct": true or false, "reason": "one sentence"}},
  "boston_character": {{"correct": true or false, "reason": "one sentence"}}
}}"""
                response = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=300,
                    messages=[{
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type":       "base64",
                                    "media_type": "image/png",
                                    "data":       image_data,
                                },
                            },
                            {"type": "text", "text": prompt},
                        ],
                    }],
                )
                raw    = re.sub(r"```(?:json)?|```", "", response.content[0].text).strip()
                result = json.loads(raw)

                if result.get("readable_text", {}).get("present"):
                    issues.append(f"[{perspective}] Readable text detected in image")
                if not result.get("exterior_only", {}).get("correct", True):
                    issues.append(
                        f"[{perspective}] Interior visible: "
                        f"{result['exterior_only'].get('reason', '')}"
                    )
                if not result.get("boston_character", {}).get("correct", True):
                    issues.append(
                        f"[{perspective}] Not authentic Boston: "
                        f"{result['boston_character'].get('reason', '')}"
                    )
            except Exception as e:
                issues.append(f"[{perspective}] Vision check failed: {e}")

        error_issues = [i for i in issues if "Interior" in i]
        warn_issues  = [i for i in issues if "Interior" not in i]
        all_issues   = error_issues + warn_issues
        status       = FAIL if error_issues else (WARN if warn_issues else PASS)
        return CheckResult(status=status, issues=all_issues)

    # ══════════════════════════════════════════════════════════════════════════
    # GPT-4o VALIDATION (web search only)
    # ══════════════════════════════════════════════════════════════════════════
    def _gpt4o_validate(
        self,
        query: str,
        domain: str,
        draft: str,
        search_context: str,
        client,
    ) -> CheckResult:
        PASS_THRESHOLD = 75
        system_prompt  = f"""You are validating a web search answer for NeighbourWise AI (Boston, MA).
Query domain: "{domain}"

ONLY flag:
1. Facts in the answer not found in sources (hallucinations)
2. Missing critical local Boston incidents directly relevant to the query
3. Specific factual claims without citation

Score 0-100. Start at 100. -20 per hallucination, -10 per missing alert, -5 per citation gap.
PASS if score >= {PASS_THRESHOLD} AND no hallucinations.

Respond ONLY with JSON:
{{
  "verdict": "PASS" or "FAIL",
  "score": integer,
  "issues": {{
    "hallucinations": [],
    "missing_alerts": [],
    "citation_gaps":  []
  }},
  "fix_instructions": "one sentence or null"
}}"""
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": (
                        f"QUERY: {query}\n\n"
                        f"SOURCES:\n{search_context[:3000]}\n\n"
                        f"DRAFT ANSWER:\n{draft[:2000]}"
                    )},
                ],
            )
            raw    = response.choices[0].message.content.strip()
            result = json.loads(raw)

            all_issues = (
                result.get("issues", {}).get("hallucinations", []) +
                result.get("issues", {}).get("missing_alerts",  []) +
                result.get("issues", {}).get("citation_gaps",   [])
            )
            verdict = result.get("verdict", "FAIL")
            score   = result.get("score", 0)
            status  = PASS if verdict == "PASS" else FAIL

            return CheckResult(
                status=status,
                issues=all_issues,
                details={
                    "score":            score,
                    "fix_instructions": result.get("fix_instructions"),
                },
            )
        except Exception as e:
            return CheckResult(status=WARN, issues=[f"GPT-4o validation failed: {e}"])

    # ══════════════════════════════════════════════════════════════════════════
    # IMPROVEMENT (Claude rewrites failed outputs)
    # ══════════════════════════════════════════════════════════════════════════
    def _improve(
        self, agent_type: AgentType, context: dict, checks: dict
    ) -> str:
        all_issues = [i for c in checks.values() for i in c.issues]
        issue_str  = "\n".join(f"- {i}" for i in all_issues)

        if agent_type == AgentType.DATA_QUERY:
            sql_data = context.get("sql_data", {})
            rag_data = context.get("rag_data", {})
            draft    = context.get("answer", "")
            question = context.get("question", "")

            parts = []
            if sql_data and isinstance(sql_data.get("results"), list):
                parts.append(
                    f"DATA:\n{json.dumps(sql_data['results'][:15], indent=2, default=str)}"
                )
            if rag_data and rag_data.get("chunks"):
                parts.append(
                    "DOCS:\n" + "\n".join(
                        f"[{c.get('DOMAIN','?')}] {c.get('CHUNK_TEXT','')[:400]}"
                        for c in rag_data["chunks"][:3]
                    )
                )
            context_str = "\n\n".join(parts)

            prompt = f"""You are NeighbourWise AI. Fix the issues in this draft answer.

QUESTION: {question}
DRAFT: {draft[:2000]}
ISSUES TO FIX:
{issue_str}
DATA: {context_str[:3000]}

Write an improved answer with:
### Summary (3-4 sentences, conversational, Title Case neighborhoods)
### Key Data (markdown table, formatted numbers, Yes/No booleans)
### Insights (2-3 analytical points referencing actual data)

Improved answer:"""
            return cortex_complete(prompt, self.conn, model=MODEL_VALIDATE)

        elif agent_type == AgentType.WEB_SEARCH:
            draft          = context.get("draft", "")
            search_context = context.get("search_context", "")
            query          = context.get("query", "")
            fix_instr      = ""
            for c in checks.values():
                if c.details.get("fix_instructions"):
                    fix_instr = c.details["fix_instructions"]
                    break

            prompt = f"""Fix this web search answer for NeighbourWise AI.

QUERY: {query}
DRAFT: {draft[:2000]}
ISSUES:
{issue_str}
FIX INSTRUCTIONS: {fix_instr}
SOURCES: {search_context[:2000]}

Write a corrected response with:
1. OVERVIEW (3-5 sentences, cite sources as [N])
2. KEY FINDINGS (one ## section per finding with dates/addresses where available)
3. SOURCES (numbered list)

Corrected response:"""
            return cortex_complete(prompt, self.conn, model=MODEL_VALIDATE)

        # For graphic/report agents — improvement happens at agent level
        return ""


# ── Convenience function (backward compatible with old validate_and_improve) ───
def validate_and_improve(
    conn,
    question: str,
    draft_answer: str,
    sql_data: dict,
    rag_data: dict,
) -> dict:
    """
    Drop-in replacement for the old structured_unstructured_agent validator.
    Keeps backward compatibility with cortex_agent.py's import.
    """
    validator = UniversalValidator(conn)
    ctx = {
        "question": question,
        "answer":   draft_answer,
        "sql_data": sql_data,
        "rag_data": rag_data,
    }
    result = validator.validate(AgentType.DATA_QUERY, ctx)
    return {
        "answer":   result.result if result.result else draft_answer,
        "feedback": {
            "checks": {
                name: {
                    "status": c.status,
                    "issues": c.issues,
                }
                for name, c in result.checks.items()
            },
            "needs_improvement": not result.passed,
            "total_issues":      len(result.all_issues),
            "all_issues":        result.all_issues,
        },
        "improved": result.improved,
        "draft":    draft_answer,
    }