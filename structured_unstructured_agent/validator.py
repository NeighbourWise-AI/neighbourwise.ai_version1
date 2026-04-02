"""
NeighbourWise AI — Validator Agent
══════════════════════════════════
Can be:
  1. IMPORTED by neighbourwise_app.py as: from neighbourwise_validator import validate_and_improve
  2. RUN STANDALONE: python neighbourwise_validator.py --all

When imported, the app calls validate_and_improve() which:
  - Takes the question, draft answer, and agent outputs
  - Validates across 5 checks
  - Returns improved answer if issues found
"""

import argparse
import json
import os
import re
import time
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

load_dotenv()
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

SNOWFLAKE_CONFIG = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "user": os.environ.get("SNOWFLAKE_USER", ""),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "NEIGHBOURWISE_AI"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "NEIGHBOURWISE_DOMAINS"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "TRAINING_ROLE"),
}

LLM_MODEL = "mistral-large2"
VALIDATOR_MODEL = "claude-3-5-sonnet"
PASS = "✅ PASS"
WARN = "⚠️  WARN"
FAIL = "❌ FAIL"


# ═══════════════════════════════════════════════════════════════
# SNOWFLAKE HELPERS
# ═══════════════════════════════════════════════════════════════

def sf_connect():
    return snowflake.connector.connect(
        **SNOWFLAKE_CONFIG,
        insecure_mode=True,
        network_timeout=120,
        login_timeout=60,
    )


def run_sql(cur, query):
    try:
        cur.execute(query)
        if cur.description:
            columns = [col[0] for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        return []
    except Exception as e:
        return {"error": str(e)}


def cortex_complete(cur, prompt):
    """Snowflake Cortex Mistral — used by standalone SQL/RAG agents."""
    try:
        safe = prompt.replace("'", "''")[:8000]
        cur.execute(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', '{safe}')")
        return cur.fetchone()[0]
    except Exception as e:
        return f"Error: {e}"


def claude_complete(cur, prompt):
    """Snowflake Cortex Claude — used by the validator for checking and improving answers."""
    try:
        safe = prompt.replace("'", "''")[:8000]
        cur.execute(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{VALIDATOR_MODEL}', '{safe}')")
        return cur.fetchone()[0]
    except Exception as e:
        return f"Error (Claude): {e}"


# ═══════════════════════════════════════════════════════════════
# VALIDATION CHECKS
# ═══════════════════════════════════════════════════════════════

def check_sql_quality(sql_data):
    """Check if SQL agent produced valid queries."""
    if not sql_data or not sql_data.get("sql"):
        return None

    issues = []
    sql = sql_data["sql"]

    if "NEIGHBOURWISE_DOMAINS.MARTS." not in sql:
        issues.append("Missing full table path (NEIGHBOURWISE_DOMAINS.MARTS.)")

    quoted = re.findall(r"'([A-Za-z ]+)'", sql)
    for q in quoted:
        if q != q.upper() and q.lower() not in ("true", "false", "null", "query:"):
            issues.append(f"Neighborhood '{q}' should be UPPERCASE '{q.upper()}'")

    if sql_data.get("error"):
        issues.append(f"SQL error: {sql_data['error'][:150]}")

    if isinstance(sql_data.get("results"), list) and len(sql_data["results"]) == 0 and not sql_data.get("error"):
        issues.append("SQL returned 0 rows — query may be too restrictive")

    return {
        "status": PASS if not issues else FAIL,
        "issues": issues,
        "rows": len(sql_data["results"]) if isinstance(sql_data.get("results"), list) else 0,
    }


def check_rag_relevance(rag_data):
    """Check if RAG agent returned relevant chunks."""
    if not rag_data or not rag_data.get("chunks"):
        return None

    chunks = rag_data["chunks"]
    top_sim = float(chunks[0].get("SIMILARITY", chunks[0].get("similarity", 0))) if chunks else 0
    issues = []

    if top_sim < 0.65:
        issues.append(f"Low relevance — top similarity {top_sim:.4f} < 0.65")
    if len(chunks) < 2:
        issues.append(f"Too few chunks: {len(chunks)}")

    return {
        "status": PASS if not issues else (WARN if top_sim >= 0.55 else FAIL),
        "issues": issues,
        "top_similarity": round(top_sim, 4),
        "chunks": len(chunks),
    }


def check_data_usage(answer, sql_data):
    """Check if the answer actually used the SQL data it received."""
    issues = []
    if sql_data and isinstance(sql_data.get("results"), list) and len(sql_data["results"]) > 0:
        bad_phrases = [
            "not available", "no data", "not provided", "does not include",
            "not specified", "additional data would be needed", "not explicitly stated",
            "couldn't find relevant"
        ]
        for phrase in bad_phrases:
            if phrase in answer.lower():
                issues.append(f"SQL returned {len(sql_data['results'])} rows but answer says '{phrase}'")
                break

    return {
        "status": PASS if not issues else FAIL,
        "issues": issues,
    }


def check_format(answer):
    """Check if answer has the required 3-section format."""
    issues = []
    lower = answer.lower()

    has_summary = "### summary" in lower or "**summary" in lower
    has_table = "|" in answer and "---" in answer
    has_recs = "### recommendation" in lower or "**recommendation" in lower

    if not has_summary:
        issues.append("Missing ### Summary section")
    if not has_table:
        issues.append("Missing Key Data table")
    if not has_recs:
        issues.append("Missing ### Recommendations section")

    return {
        "status": PASS if not issues else WARN,
        "issues": issues,
    }


def check_hallucination(cur, answer, sql_data, rag_data):
    """Check if answer makes claims not supported by data. Uses GPT-4o for better judgment."""
    sql_rows = len(sql_data["results"]) if sql_data and isinstance(sql_data.get("results"), list) else 0
    rag_chunks = len(rag_data["chunks"]) if rag_data and rag_data.get("chunks") else 0

    prompt = f"""Check this answer for SERIOUS hallucinations only.

IMPORTANT: These are NOT hallucinations:
- Reasonable inferences from data (e.g. "good availability" from a high count)
- Conversational phrasing of data values
- General recommendations based on the data
- Describing what the data means

ONLY flag as hallucination if the answer:
- Invents specific numbers NOT in the data
- Names specific facilities, people, or programs NOT in the data
- Claims facts about topics the data doesn't cover at all

Data context: SQL returned {sql_rows} rows, RAG returned {rag_chunks} chunks.
Answer: {answer[:1500]}

Reply with ONLY: NO (if no serious hallucinations) or YES: [brief explanation]"""

    result = claude_complete(cur, prompt)
    has_hallucination = result.strip().upper().startswith("YES")

    return {
        "status": FAIL if has_hallucination else PASS,
        "issues": [result.strip()[:200]] if has_hallucination else [],
    }


# ═══════════════════════════════════════════════════════════════
# CORE VALIDATOR FUNCTION — called by app.py
# ═══════════════════════════════════════════════════════════════

def validate_answer(cur, question, answer, sql_data, rag_data):
    """
    Run all validation checks on a draft answer.
    Returns dict with checks, issues, and whether improvement is needed.
    """
    checks = {}

    # Run each check
    sql_check = check_sql_quality(sql_data)
    if sql_check:
        checks["sql_quality"] = sql_check

    rag_check = check_rag_relevance(rag_data)
    if rag_check:
        checks["rag_relevance"] = rag_check

    checks["data_usage"] = check_data_usage(answer, sql_data)
    checks["format"] = check_format(answer)
    checks["hallucination"] = check_hallucination(cur, answer, sql_data, rag_data)

    all_issues = []
    for c in checks.values():
        all_issues.extend(c.get("issues", []))

    needs_improvement = any(c["status"] == FAIL for c in checks.values())

    return {
        "checks": checks,
        "needs_improvement": needs_improvement,
        "total_issues": len(all_issues),
        "all_issues": all_issues,
    }


def generate_improved_answer(cur, question, draft, feedback, sql_data, rag_data):
    """Use GPT-4o with validator feedback to produce a better answer."""
    context_parts = []
    if sql_data and isinstance(sql_data.get("results"), list):
        context_parts.append(f"STRUCTURED DATA:\n{json.dumps(sql_data['results'][:20], indent=2, default=str)}")
    if rag_data and rag_data.get("chunks"):
        chunks_text = "\n\n".join([
            f"[{c.get('DOMAIN', c.get('domain', '?'))}] {c.get('CHUNK_TEXT', c.get('chunk_text', ''))[:500]}"
            for c in rag_data["chunks"][:5]
        ])
        context_parts.append(f"DOCUMENT CONTEXT:\n{chunks_text}")

    context = "\n\n".join(context_parts)
    issues_text = "\n".join(f"- {i}" for i in feedback["all_issues"])

    prompt = f"""You are NeighbourWise AI, a Boston neighborhood intelligence assistant. Your first draft had issues. Fix them.

QUESTION: {question}
DRAFT: {draft[:2000]}
ISSUES TO FIX:
{issues_text}

AVAILABLE DATA:
{context[:3000]}

RULES:
- MUST have exactly ### Summary, ### Key Data (markdown table), ### Recommendations
- Summary: 2-3 conversational sentences. Lead with key insight. Title Case for neighborhoods.
- Key Data: Clean markdown table. Booleans as Yes/No. Numbers with commas. No IDs/timestamps.
- Recommendations: 2-3 specific, actionable suggestions based on the data.
- USE the SQL data if it exists. Never say "not available" when data was returned.
- Do NOT invent numbers or facility names not in the data.

Improved answer:"""

    return claude_complete(cur, prompt)


def validate_and_improve(cur, question, draft_answer, sql_data, rag_data):
    """
    THE MAIN FUNCTION CALLED BY APP.PY
    
    Takes a draft answer, validates it, and returns either:
    - The original draft (if it passes all checks)
    - An improved version (if issues were found)
    
    Also returns the validation feedback for display in the UI.
    """
    feedback = validate_answer(cur, question, draft_answer, sql_data, rag_data)

    if feedback["needs_improvement"]:
        final_answer = generate_improved_answer(
            cur, question, draft_answer, feedback, sql_data, rag_data
        )
        improved = True
    else:
        final_answer = draft_answer
        improved = False

    return {
        "answer": final_answer,
        "feedback": feedback,
        "improved": improved,
        "draft": draft_answer,
    }


# ═══════════════════════════════════════════════════════════════
# STANDALONE MODE — for testing independently
# ═══════════════════════════════════════════════════════════════

def sql_agent(cur, question):
    """SQL agent for standalone testing."""
    schema_prompt = f"""You are a SQL expert. Database: NEIGHBOURWISE_DOMAINS, Schema: MARTS.
CRITICAL: Always use NEIGHBOURWISE_DOMAINS.MARTS.<table>. NEIGHBORHOOD_NAME is ALWAYS UPPERCASE.
Tables: MRT_BOSTON_HEALTHCARE, MRT_BOSTON_RESTAURANTS, MRT_BOSTON_UNIVERSITIES, MRT_BOSTON_CRIME,
MRT_BOSTON_HOUSING, MRT_BOSTON_MBTA_STOPS, MRT_BOSTON_SCHOOLS, MRT_BOSTON_BLUEBIKE_STATIONS,
MRT_BOSTON_GROCERY_STORES, MRT_NEIGHBORHOOD_HEALTHCARE, MRT_NEIGHBORHOOD_SAFETY,
MRT_NEIGHBORHOOD_HOUSING, MRT_NEIGHBORHOOD_MBTA, MRT_NEIGHBORHOOD_RESTAURANTS,
MRT_NEIGHBORHOOD_SCHOOLS, MRT_NEIGHBORHOOD_BLUEBIKES, MRT_NEIGHBORHOOD_GROCERY_STORES,
MRT_NEIGHBORHOOD_UNIVERSITIES. All joinable via LOCATION_ID.
Generate ONLY SQL, no explanation, no backticks.
Question: {question}
SQL:"""
    sql_text = cortex_complete(cur, schema_prompt).strip().replace("```sql", "").replace("```", "").strip()
    if not sql_text or sql_text.startswith("Error"):
        return {"sql": None, "results": None, "error": sql_text}
    results = run_sql(cur, sql_text)
    if isinstance(results, dict) and "error" in results:
        return {"sql": sql_text, "results": None, "error": results["error"]}
    return {"sql": sql_text, "results": results}


def rag_agent(cur, question, top_k=5):
    """RAG agent for standalone testing."""
    safe_q = question.replace("'", "''")[:2000]
    sql = f"""SELECT chunk_text, domain, source_file,
        VECTOR_COSINE_SIMILARITY(chunk_embedding,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'query: {safe_q}')
        ) AS similarity
    FROM NEIGHBOURWISE_DOMAINS.RAW_UNSTRUCTURED.RAW_DOMAIN_CHUNKS
    ORDER BY similarity DESC LIMIT {top_k}"""
    results = run_sql(cur, sql)
    if isinstance(results, dict) and "error" in results:
        return {"chunks": [], "error": results["error"]}
    return {"chunks": results}


def classify_question(cur, question):
    """Classifier for standalone testing."""
    prompt = f"""Classify: SQL (numbers/counts/grades), RAG (context/reports/policies), BOTH (numbers+context).
Question: {question}
Reply ONLY: SQL or RAG or BOTH"""
    r = cortex_complete(cur, prompt).strip().upper()
    return "BOTH" if "BOTH" in r else "RAG" if "RAG" in r else "SQL" if "SQL" in r else "BOTH"


def synthesize(cur, question, sql_data, rag_data):
    """Synthesizer for standalone testing."""
    parts = []
    if sql_data and isinstance(sql_data.get("results"), list):
        parts.append(f"STRUCTURED DATA:\n{json.dumps(sql_data['results'][:20], indent=2, default=str)}")
    if rag_data and rag_data.get("chunks"):
        parts.append(f"DOCUMENTS:\n" + "\n".join(
            f"[{c.get('DOMAIN','?')}] {c.get('CHUNK_TEXT','')[:500]}" for c in rag_data["chunks"][:5]
        ))
    if not parts:
        return "I couldn't find relevant information to answer your question."
    prompt = f"""You are NeighbourWise AI. Answer with ### Summary, ### Key Data (table), ### Recommendations.
DATA:\n{chr(10).join(parts)[:3000]}
QUESTION: {question}
Answer:"""
    return cortex_complete(cur, prompt)


def run_single_question(cur, question, verbose=True):
    """Run full pipeline for one question."""
    if verbose:
        print(f"\n{'═' * 70}")
        print(f"  QUESTION: {question}")
        print(f"{'═' * 70}")

    classification = classify_question(cur, question)
    if verbose: print(f"\n  Step 1 — Classification: {classification}")

    sql_data = sql_agent(cur, question) if classification in ("SQL", "BOTH") else None
    rag_data = rag_agent(cur, question) if classification in ("RAG", "BOTH") else None

    if verbose and sql_data and sql_data.get("sql"):
        rows = len(sql_data["results"]) if isinstance(sql_data.get("results"), list) else 0
        print(f"  Step 2a — SQL: {rows} rows | {sql_data['sql'][:80]}...")
    if verbose and rag_data and rag_data.get("chunks"):
        print(f"  Step 2b — RAG: {len(rag_data['chunks'])} chunks | sim: {float(rag_data['chunks'][0].get('SIMILARITY',0)):.4f}")

    draft = synthesize(cur, question, sql_data, rag_data)
    if verbose: print(f"  Step 3 — Draft: {draft[:120]}...")

    result = validate_and_improve(cur, question, draft, sql_data, rag_data)

    if verbose:
        print(f"\n  {'─' * 60}")
        print(f"  VALIDATION")
        print(f"  {'─' * 60}")
        for name, data in result["feedback"]["checks"].items():
            print(f"    {data['status']} {name}")
            for issue in data.get("issues", []):
                print(f"         → {issue[:100]}")
        print(f"    Issues: {result['feedback']['total_issues']} | Improved: {'YES' if result['improved'] else 'NO'}")
        print(f"\n  {'═' * 60}")
        print(f"  FINAL ANSWER")
        print(f"  {'═' * 60}\n{result['answer']}\n")

    return result


TEST_QUESTIONS = [
    {"q": "How many hospitals are in Dorchester?", "domain": "Healthcare"},
    {"q": "What are the top rated restaurants in South Boston?", "domain": "Restaurants"},
    {"q": "Which universities are in Fenway and do they have campus housing?", "domain": "Universities"},
    {"q": "Which neighborhoods have the highest violent crime count?", "domain": "Crime"},
    {"q": "What is the average rent and housing grade for Beacon Hill?", "domain": "Housing"},
    {"q": "Which neighborhoods have no rapid transit access?", "domain": "Transit"},
    {"q": "Which neighborhood has the most Bluebike stations?", "domain": "Bluebikes"},
    {"q": "How many supermarkets are in Roxbury?", "domain": "Grocery"},
    {"q": "How many public schools are in Dorchester?", "domain": "Schools"},
    {"q": "What are the health equity challenges in Boston?", "domain": "Healthcare (RAG)"},
    {"q": "What is Boston doing about food deserts?", "domain": "Restaurants (RAG)"},
    {"q": "Tell me about healthcare in Roxbury with facility counts and challenges", "domain": "Hybrid"},
    {"q": "Compare Back Bay and Mattapan across safety, healthcare, and housing", "domain": "Cross-domain"},
    {"q": "What is the population of Dorchester?", "domain": "Trick"},
    {"q": "Who is the mayor of Boston?", "domain": "Trick"},
]


def run_test_suite():
    print("=" * 70)
    print("  NeighbourWise AI — Validator Test Suite")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    conn = sf_connect()
    cur = conn.cursor()
    results = []

    for i, test in enumerate(TEST_QUESTIONS):
        print(f"\n{'━' * 70}")
        print(f"  TEST {i+1}/{len(TEST_QUESTIONS)}: {test['domain']}")
        print(f"{'━' * 70}")
        result = run_single_question(cur, test["q"])
        result["domain"] = test["domain"]
        results.append(result)
        time.sleep(1)

    print(f"\n\n{'═' * 70}")
    print(f"  FINAL REPORT")
    print(f"{'═' * 70}\n")

    passed = sum(1 for r in results if not r["feedback"]["needs_improvement"])
    improved = sum(1 for r in results if r["improved"])

    check_stats = {}
    for r in results:
        for name, data in r["feedback"]["checks"].items():
            if name not in check_stats:
                check_stats[name] = {"pass": 0, "warn": 0, "fail": 0}
            if PASS in data["status"]:
                check_stats[name]["pass"] += 1
            elif WARN in data["status"]:
                check_stats[name]["warn"] += 1
            else:
                check_stats[name]["fail"] += 1

    print(f"  ┌──────────────────────┬───────┬───────┬───────┐")
    print(f"  │ Check                │ Pass  │ Warn  │ Fail  │")
    print(f"  ├──────────────────────┼───────┼───────┼───────┤")
    for name, s in check_stats.items():
        print(f"  │ {name:<20s} │  {s['pass']:>3}  │  {s['warn']:>3}  │  {s['fail']:>3}  │")
    print(f"  └──────────────────────┴───────┴───────┴───────┘")

    total = len(results)
    score = (passed + improved * 0.8) / total * 100
    grade = "A" if score >= 90 else "B+" if score >= 80 else "B" if score >= 70 else "C" if score >= 60 else "D"
    print(f"\n  Total: {total} | Passed: {passed} | Improved: {improved}")
    print(f"  Score: {score:.1f}% — Grade: {grade}")
    print(f"{'═' * 70}\n")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NeighbourWise Validator Agent")
    parser.add_argument("--question", "-q", help="Validate a single question")
    parser.add_argument("--all", action="store_true", help="Run full test suite")
    args = parser.parse_args()

    if args.all:
        run_test_suite()
    elif args.question:
        conn = sf_connect()
        cur = conn.cursor()
        run_single_question(cur, args.question)
        cur.close()
        conn.close()
    else:
        # Interactive mode
        print("=" * 70)
        print("  NeighbourWise Validator (Interactive)")
        print("  Type 'exit' to quit")
        print("=" * 70)
        conn = sf_connect()
        cur = conn.cursor()
        while True:
            q = input("\n  Question: ").strip()
            if not q or q.lower() in ("exit", "quit"):
                break
            run_single_question(cur, q)
        cur.close()
        conn.close()
