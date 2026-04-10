"""
graph_validator.py
==================
NeighbourWise AI — Graph Agent Validator (GPT-4o)

Flow:
    Claude (graph_agent.py) produces a draft answer
        ↓
    GPT-4o validates it against:
        • Neo4j graph context  (scores, grades, relationships)
        • Snowflake mart data  (exact numeric values)
        • RAG chunk context    (unstructured domain facts)
        ↓
    PASS → return final answer
    FAIL → Claude regenerates with targeted fix instructions
         → GPT-4o re-validates → up to MAX_RETRIES attempts

Usage (imported by graph_agent.py):
    from graph_validator import validate_and_regenerate
    result = validate_and_regenerate(query, draft, graph_ctx, struct_ctx, rag_chunks)

Usage (standalone test):
    python graph_validator.py

Requirements:
    pip install openai anthropic python-dotenv
"""

import os
import json
import time
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI
import anthropic

# ── Env ───────────────────────────────────────────────────────────────────────

def _find_env_file() -> Path:
    current = Path(__file__).resolve().parent
    for _ in range(5):
        candidate = current / ".env"
        if candidate.exists():
            return candidate
        current = current.parent
    return Path(".env")

_env_path = _find_env_file()
load_dotenv(dotenv_path=_env_path)
print(f"  [env] Loaded .env from: {_env_path}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("graph_validator")

# ── Config ────────────────────────────────────────────────────────────────────

OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

VALIDATOR_MODEL = "gpt-4o"
CLAUDE_MODEL    = "claude-sonnet-4-6"
MAX_RETRIES     = 2          # max regeneration attempts after initial FAIL
PASS_THRESHOLD  = 75         # minimum score to pass
RETRY_DELAY     = 2          # seconds between retries (rate limit buffer)

openai_client    = OpenAI(api_key=OPENAI_API_KEY)
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION CRITERIA (graph-specific — different from web search validator)
# ══════════════════════════════════════════════════════════════════════════════
#
# The web search validator checks: hallucinations vs Serper snippets.
# The graph validator checks:
#
#  1. SCORE ACCURACY    — does the draft cite scores that match the graph data?
#                         e.g. "Safety score 50.3" should match graph context
#  2. GRADE ACCURACY    — are grades correctly reported (GOOD, AFFORDABLE, etc.)?
#  3. FABRICATED DATA   — did Claude invent neighborhoods, scores, or metrics
#                         not present in the graph/mart/RAG context?
#  4. MISSING INSIGHTS  — did Claude ignore important domain scores or
#                         relationships (borders, transit, similar neighborhoods)?
#  5. COMPARISON QUALITY— if other neighborhoods were mentioned for comparison,
#                         are their scores grounded in the graph data?
#  6. RAG ALIGNMENT     — does the unstructured context usage match the chunks?
#
# ══════════════════════════════════════════════════════════════════════════════

VALIDATOR_SYSTEM_PROMPT = """You are a strict quality-control validator for NeighbourWise AI,
a Greater Boston neighborhood livability analysis system.

Your job is to audit a Claude-generated neighborhood analysis against the raw data
sources it was built from: Neo4j graph data, Snowflake mart metrics, and RAG chunks.

Check for exactly these six issues:

1. SCORE_ERRORS
   Any composite_score or domain score cited in the draft that does NOT match
   the value in the graph context. Flag the exact discrepancy.
   Example: draft says "Safety score 72" but graph shows 50.3 → flag it.

2. GRADE_ERRORS
   Any grade label (GOOD, AFFORDABLE, EXCELLENT, etc.) that contradicts
   the grade in the graph context or mart data.

3. FABRICATED_DATA
   Neighborhoods, scores, incident counts, prices, or relationships that
   appear in the draft but are NOT present anywhere in the provided context.
   This is the most serious issue — flag every invented fact.

4. MISSING_INSIGHTS
   Important data points that Claude ignored — but ONLY for the domains
   the ORIGINAL QUERY explicitly asked about.

   CRITICAL SCOPING RULE: Do NOT flag missing domains that the query did
   not ask about. If the query is "Is Allston safe and affordable?", only
   Safety and Housing are in scope. Do NOT flag MBTA, Restaurants, Grocery,
   Universities etc. as missing — those domains are irrelevant to this query.

   Only flag as missing_insights when:
   - A score or grade for a QUERIED domain was available but not mentioned
   - A direct peer comparison for a QUERIED domain was available but ignored
   - A critical metric (e.g. incident count, rent) was in the data but omitted

   Never flag: domains outside the query scope, supplementary context the
   user didn't ask for, or data that is nice-to-have but not answering
   the actual question.

5. COMPARISON_ERRORS
   When Claude compared the queried neighborhood to others, did it use the
   correct scores for the comparison neighborhoods? Flag any comparison
   where the cited score doesn't match the graph context.

6. RICHNESS_ISSUES
   Is the response under 200 words? Missing a direct answer to the query?
   Failing to use the RAG context when it was relevant and available?

Respond ONLY with a valid JSON object. No prose before or after. Schema:
{
  "verdict": "PASS" or "FAIL",
  "score": integer 0-100,
  "issues": {
    "score_errors":       ["<domain>: draft says X, graph shows Y", ...],
    "grade_errors":       ["<domain>: draft says X, graph shows Y", ...],
    "fabricated_data":    ["<specific invented claim>", ...],
    "missing_insights":   ["<important data point that was ignored>", ...],
    "comparison_errors":  ["<neighborhood>: draft says X, graph shows Y", ...],
    "richness_issues":    ["<specific gap>", ...]
  },
  "regeneration_prompt": "<3-5 sentences of specific fix instructions for Claude,
                          telling it exactly what to correct, add, or remove.
                          Reference specific scores and grades from the context.>"
}

PASS criteria: score >= 75 AND fabricated_data list is empty.
FAIL if score < 75 OR any fabricated data exists."""


def _build_validation_context(
    graph_ctx: dict,
    struct_ctx: dict,
    rag_chunks: list[dict],
) -> str:
    """Format the three data sources into a readable ground-truth block for GPT-4o."""
    parts = []

    if graph_ctx:
        parts.append("=== GRAPH CONTEXT (ground truth — Neo4j) ===")
        parts.append(json.dumps(graph_ctx, indent=2, default=str)[:4000])

    if struct_ctx:
        parts.append("\n=== STRUCTURED MART DATA (ground truth — Snowflake) ===")
        parts.append(json.dumps(struct_ctx, indent=2, default=str)[:2000])

    if rag_chunks:
        parts.append("\n=== RAG CHUNKS (available unstructured context) ===")
        for i, c in enumerate(rag_chunks[:3], 1):
            parts.append(
                f"[{i}] Domain: {c.get('domain','?')} | Score: {c.get('hybrid_score',0):.3f}\n"
                f"{str(c.get('chunk_text',''))[:400]}"
            )

    return "\n".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — GPT-4o Validation
# ══════════════════════════════════════════════════════════════════════════════

def validate_output(
    query: str,
    draft: str,
    graph_ctx: dict,
    struct_ctx: dict,
    rag_chunks: list[dict],
) -> dict:
    """
    GPT-4o validates the Claude draft against graph, mart, and RAG data.
    Returns structured verdict dict.
    """
    ground_truth = _build_validation_context(graph_ctx, struct_ctx, rag_chunks)

    user_message = f"""ORIGINAL QUERY:
{query}

GROUND TRUTH DATA SOURCES:
{ground_truth}

CLAUDE DRAFT TO VALIDATE:
{draft}

Validate the draft against the ground truth data sources and return your JSON verdict."""

    try:
        response = openai_client.chat.completions.create(
            model=VALIDATOR_MODEL,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": VALIDATOR_SYSTEM_PROMPT},
                {"role": "user",   "content": user_message},
            ],
        )
        raw     = response.choices[0].message.content.strip()
        verdict = json.loads(raw)
        return verdict

    except Exception as e:
        log.error(f"Validator call failed: {e}")
        # Non-fatal — return a default PASS so the pipeline always returns output
        return {
            "verdict": "PASS",
            "score": 70,
            "issues": {},
            "regeneration_prompt": "",
            "_validator_error": str(e),
        }


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Claude Regeneration
# ══════════════════════════════════════════════════════════════════════════════

REGEN_SYSTEM_PROMPT = """You are the NeighbourWise AI graph agent for Greater Boston
neighborhood livability analysis. You previously generated a response that failed
quality validation. You must now produce a corrected version.

Rules:
- Fix EVERY issue listed in the validator findings below
- Use ONLY the data provided in the context — do not invent scores, grades, or facts
- Quote exact scores and grades from the graph/mart data (e.g. "Safety score 50.3, GOOD grade")
- Keep response between 300–500 words
- End with: "Sources: [graph] [structured mart] [RAG chunks]" listing which contributed
- Do NOT copy the previous draft — write fresh from the context"""


def regenerate_output(
    query: str,
    draft: str,
    graph_ctx: dict,
    struct_ctx: dict,
    rag_chunks: list[dict],
    verdict: dict,
) -> str:
    """
    Passes the validator's verdict + original context back to Claude
    with targeted fix instructions. Returns a fresh draft.
    """
    issues     = verdict.get("issues", {})
    regen_prompt = verdict.get("regeneration_prompt", "Fix all identified issues.")

    # Build a human-readable issue summary for Claude
    fix_lines = []
    issue_labels = {
        "score_errors":      "Score errors to fix",
        "grade_errors":      "Grade errors to fix",
        "fabricated_data":   "REMOVE these fabricated claims",
        "missing_insights":  "ADD these missing insights",
        "comparison_errors": "Comparison errors to fix",
        "richness_issues":   "Richness improvements needed",
    }
    for key, label in issue_labels.items():
        items = issues.get(key, [])
        if items:
            fix_lines.append(f"\n{label}:")
            for item in items:
                fix_lines.append(f"  - {item}")

    fix_block = "\n".join(fix_lines) if fix_lines else "General quality improvements needed."

    # Rebuild ground truth context for Claude
    context_parts = []
    if graph_ctx:
        context_parts.append("=== GRAPH CONTEXT (Neo4j) ===")
        context_parts.append(json.dumps(graph_ctx, indent=2, default=str)[:4000])
    if struct_ctx:
        context_parts.append("\n=== STRUCTURED MART DATA ===")
        context_parts.append(json.dumps(struct_ctx, indent=2, default=str)[:2000])
    if rag_chunks:
        context_parts.append("\n=== RAG CHUNKS ===")
        for i, c in enumerate(rag_chunks[:3], 1):
            context_parts.append(
                f"[{i}] {c.get('domain','?')}: {str(c.get('chunk_text',''))[:400]}"
            )

    user_message = f"""ORIGINAL QUERY: {query}

PREVIOUS DRAFT (contains issues — do NOT copy blindly):
{draft}

VALIDATOR FINDINGS — fix ALL of these:
{fix_block}

VALIDATOR INSTRUCTION:
{regen_prompt}

GROUND TRUTH CONTEXT (use as your only source of facts):
{chr(10).join(context_parts)}

Write a fully corrected response now."""

    try:
        response = anthropic_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=700,
            system=REGEN_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text.strip()

    except anthropic.RateLimitError:
        log.warning("Anthropic rate limit hit — waiting 30s before retry")
        time.sleep(30)
        response = anthropic_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=700,
            system=REGEN_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text.strip()

    except Exception as e:
        log.error(f"Regeneration failed: {e}")
        return draft  # fall back to original draft on error


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def validate_and_regenerate(
    query: str,
    draft: str,
    graph_ctx: dict,
    struct_ctx: dict,
    rag_chunks: list[dict],
    verbose: bool = True,
) -> dict:
    """
    Full validation + optional regeneration pipeline.

    Args:
        query       : original user query
        draft       : Claude's initial answer from graph_agent.py
        graph_ctx   : Neo4j context dict (profile, top_by_domain, etc.)
        struct_ctx  : Snowflake mart dict (housing, safety details)
        rag_chunks  : list of RAG chunk dicts from hybrid search

    Returns:
    {
        "final_output"  : str,   # best output (original if PASS, regen if FAIL)
        "verdict"       : dict,  # GPT-4o verdict on the ORIGINAL draft
        "final_verdict" : dict,  # GPT-4o verdict after regen (if triggered)
        "regenerated"   : bool,  # whether regeneration was triggered
        "attempts"      : int,   # total generation attempts (1 = no regen needed)
        "passed"        : bool,  # whether final output passed validation
    }
    """
    current_output = draft
    last_verdict   = None
    regenerated    = False
    attempts       = 1

    for attempt in range(MAX_RETRIES + 1):

        # ── Validate ──────────────────────────────────────────────────────────
        if verbose:
            log.info(f"Validator attempt {attempt + 1}/{MAX_RETRIES + 1} …")

        verdict     = validate_output(query, current_output, graph_ctx, struct_ctx, rag_chunks)
        last_verdict = verdict
        score       = verdict.get("score", 0)
        result      = verdict.get("verdict", "FAIL")
        issues      = verdict.get("issues", {})

        if verbose:
            log.info(f"  Verdict: {result}  |  Score: {score}/100")
            for category, items in issues.items():
                if items:
                    log.info(f"  {category}:")
                    for item in items:
                        log.info(f"    - {item}")

        # ── PASS ──────────────────────────────────────────────────────────────
        if result == "PASS":
            if verbose:
                log.info(f"  Passed on attempt {attempt + 1}")
            return {
                "final_output":  current_output,
                "verdict":       verdict if attempt == 0 else last_verdict,
                "final_verdict": verdict,
                "regenerated":   regenerated,
                "attempts":      attempts,
                "passed":        True,
            }

        # ── FAIL — regenerate unless we've hit the retry limit ────────────────
        if attempt < MAX_RETRIES:
            if verbose:
                log.info(f"  Failed — regenerating (attempt {attempt + 2}) …")
            time.sleep(RETRY_DELAY)
            current_output = regenerate_output(
                query, current_output,
                graph_ctx, struct_ctx, rag_chunks,
                verdict,
            )
            regenerated = True
            attempts   += 1
        else:
            if verbose:
                log.warning(f"  Max retries reached — returning best available output")

    # Exhausted retries — return last output with FAIL status
    return {
        "final_output":  current_output,
        "verdict":       last_verdict,
        "final_verdict": last_verdict,
        "regenerated":   regenerated,
        "attempts":      attempts,
        "passed":        False,
    }


# ══════════════════════════════════════════════════════════════════════════════
# STANDALONE TEST
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Graph Validator — standalone test")
    print("=" * 60)

    # Minimal mock data to test the pipeline without a live Neo4j/Snowflake call
    test_query = "Is Allston safe and affordable?"

    test_graph_ctx = {
        "neighborhood": "ALLSTON",
        "domain_scores": [
            {"domain": "Safety",  "score": 50.3, "grade": "GOOD"},
            {"domain": "Housing", "score": 58.3, "grade": "AVERAGE"},
            {"domain": "Schools", "score": 91.0, "grade": "EXCELLENT"},
        ],
        "borders":    ["CAMBRIDGE", "BOSTON"],
        "mbta_lines": ["Green Line"],
        "similar_to": [{"neighbor": "SOUTH END", "delta": 3.1}],
    }

    test_struct_ctx = {
        "housing": {
            "neighborhood_name": "ALLSTON",
            "housing_score": 58.3,
            "housing_grade": "AVERAGE",
            "avg_price_per_sqft": 540.0,
            "avg_assessed_value": 831215.0,
            "avg_estimated_rent": 6650.0,
        },
        "safety": {
            "neighborhood_name": "ALLSTON",
            "safety_score": 50.3,
            "safety_grade": "GOOD",
            "total_incidents": 5631,
            "violent_crime_count": 43,
        },
    }

    test_rag = [
        {
            "domain": "HOUSING",
            "source_file": "Boston_Housing_Report.txt",
            "hybrid_score": 0.82,
            "chunk_text": "Allston remains one of Boston's densest rental markets, driven by proximity to BU and Harvard Extension students.",
        }
    ]

    # Deliberately inject a score error to trigger FAIL + regen
    test_draft_bad = """Allston is a very safe neighborhood with an excellent safety score
of 85 out of 100. Housing is also very affordable with a PREMIUM grade.
Rent averages around $2,000/month which is very reasonable for Boston.
There are no violent crimes reported in Allston."""

    print("\nTesting with a bad draft (should FAIL then regenerate)…\n")

    result = validate_and_regenerate(
        query      = test_query,
        draft      = test_draft_bad,
        graph_ctx  = test_graph_ctx,
        struct_ctx = test_struct_ctx,
        rag_chunks = test_rag,
        verbose    = True,
    )

    print("\n" + "=" * 60)
    print(f"Pipeline result:")
    print(f"  Passed      : {result['passed']}")
    print(f"  Regenerated : {result['regenerated']}")
    print(f"  Attempts    : {result['attempts']}")
    print(f"  Final score : {result['final_verdict'].get('score')}/100")
    print("\nFinal output:")
    print("-" * 40)
    print(result["final_output"])