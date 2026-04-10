"""
test_graph_validator_merged.py
==============================
Tests for the Graph Validator logic now living inside universal_validator.py.
Mirrors the standalone test in the old Graph_validator_agent.py but runs
entirely through UniversalValidator / validate_graph_output().

    python test_graph_validator_merged.py

Requires: OPENAI_API_KEY and ANTHROPIC_API_KEY in .env
No live Snowflake connection needed — graph path uses GPT-4o + Anthropic directly.
"""

import os
import sys
import json
from pathlib import Path

# ── Env ───────────────────────────────────────────────────────────────────────
def _find_env():
    here = Path(__file__).resolve().parent
    for _ in range(5):
        candidate = here / ".env"
        if candidate.exists():
            return candidate
        here = here.parent
    return Path(".env")

from dotenv import load_dotenv
load_dotenv(dotenv_path=_find_env())

from universal_validator import (
    UniversalValidator,
    AgentType,
    validate_graph_output,
)

# ── Shared mock data (Allston) ────────────────────────────────────────────────
GRAPH_CTX = {
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

STRUCT_CTX = {
    "housing": {
        "neighborhood_name": "ALLSTON",
        "housing_score":     58.3,
        "housing_grade":     "AVERAGE",
        "avg_price_per_sqft": 540.0,
        "avg_assessed_value": 831215.0,
        "avg_estimated_rent": 6650.0,
    },
    "safety": {
        "neighborhood_name":  "ALLSTON",
        "safety_score":       50.3,
        "safety_grade":       "GOOD",
        "total_incidents":    5631,
        "violent_crime_count": 43,
    },
}

RAG_CHUNKS = [
    {
        "domain": "HOUSING",
        "source_file": "Boston_Housing_Report.txt",
        "hybrid_score": 0.82,
        "chunk_text": (
            "Allston remains one of Boston's densest rental markets, driven by "
            "proximity to BU and Harvard Extension students."
        ),
    }
]

QUERY = "Is Allston safe and affordable?"

SEP = "=" * 60


def banner(title: str):
    print(f"\n{SEP}\n  {title}\n{SEP}")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 1 — BAD DRAFT (should FAIL → trigger improvement)
# Tests: _check_graph_answer_quality, _gpt4o_validate_graph, _improve()
# ══════════════════════════════════════════════════════════════════════════════
def test_bad_draft_triggers_fail_and_improvement():
    banner("TEST 1: Bad draft — should FAIL and be improved")

    # Deliberately wrong: safety score 85 (graph says 50.3), grade PREMIUM
    # (graph says AVERAGE), rent $2,000 (graph says $6,650), no violent crimes
    bad_draft = (
        "Allston is an extremely safe neighborhood with a safety score of 85/100. "
        "Housing is PREMIUM grade — one of the most affordable in Boston with rent "
        "averaging $2,000/month. There are no violent crimes reported in Allston. "
        "The Schools score is 45, which is below average."
    )

    result = validate_graph_output(
        query=QUERY,
        answer=bad_draft,
        graph_ctx=GRAPH_CTX,
        struct_ctx=STRUCT_CTX,
        rag_chunks=RAG_CHUNKS,
    )

    print(f"  passed           : {result['feedback']['needs_improvement'] == False}")
    print(f"  needs_improvement: {result['feedback']['needs_improvement']}  (expected: True)")
    print(f"  improved         : {result['improved']}  (expected: True)")
    print(f"  total_issues     : {result['feedback']['total_issues']}")
    print(f"\n  Checks:")
    for name, check in result["feedback"]["checks"].items():
        print(f"    {check['status']} {name}")
        for issue in check["issues"][:3]:
            print(f"      → {issue[:100]}")

    print(f"\n  Final output preview (first 300 chars):")
    print(f"  {result['answer'][:300]}")

    assert result["feedback"]["needs_improvement"], \
        "❌ FAIL: bad draft should have triggered needs_improvement=True"
    assert result["improved"], \
        "❌ FAIL: improved flag should be True after regeneration"
    print("\n  ✅ TEST 1 PASSED")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 2 — GOOD DRAFT (should PASS on first attempt)
# Tests: answer_quality check passes, GPT-4o returns PASS, no improvement triggered
# ══════════════════════════════════════════════════════════════════════════════
def test_good_draft_passes():
    banner("TEST 2: Good draft — should PASS without improvement")

    good_draft = (
        "Allston sits at the intersection of affordability pressure and solid "
        "safety infrastructure. Its Safety score of 50.3/100 earns a GOOD grade — "
        "above the Greater Boston median — with 5,631 total incidents logged and "
        "43 violent crimes recorded, a relatively low rate for a dense urban area. "
        "On housing, Allston scores 58.3/100 (AVERAGE grade), with average assessed "
        "values around $831,000 and estimated monthly rents near $6,650. This places "
        "it in mid-range territory — not the cheapest in Boston, but accessible "
        "relative to Beacon Hill or the South End. "
        "Allston's student-heavy population, driven by proximity to BU and Harvard "
        "Extension, keeps rental demand high year-round. Green Line access and dense "
        "walkable blocks make it highly livable despite the premium cost. For renters "
        "prioritising safety and transit over rock-bottom rent, Allston delivers. "
        "Sources: [graph] [structured mart] [RAG chunks]"
    )

    result = validate_graph_output(
        query=QUERY,
        answer=good_draft,
        graph_ctx=GRAPH_CTX,
        struct_ctx=STRUCT_CTX,
        rag_chunks=RAG_CHUNKS,
    )

    print(f"  needs_improvement: {result['feedback']['needs_improvement']}  (expected: False)")
    print(f"  improved         : {result['improved']}  (expected: False)")
    print(f"  total_issues     : {result['feedback']['total_issues']}  (expected: 0)")
    print(f"\n  Checks:")
    for name, check in result["feedback"]["checks"].items():
        print(f"    {check['status']} {name}")
        for issue in check["issues"][:2]:
            print(f"      → {issue[:100]}")

    # GPT-4o might still flag minor issues — only assert improved=False here
    assert not result["improved"], \
        "❌ FAIL: good draft should not have triggered regeneration"
    print("\n  ✅ TEST 2 PASSED")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 3 — SCOPING RULE (GPT-4o must NOT penalise omitted out-of-scope domains)
# Tests: CRITICAL SCOPING RULE in system prompt
# ══════════════════════════════════════════════════════════════════════════════
def test_scoping_rule_no_penalty_for_out_of_scope_domains():
    banner("TEST 3: CRITICAL SCOPING RULE — query is Safety+Housing only")

    # Query asks ONLY about safety and housing — Schools, Transit etc. should
    # not be flagged as missing_insights even though Schools score = 91 in graph
    scoped_draft = (
        "Allston offers moderate safety with a score of 50.3/100 (GOOD grade). "
        "There were 5,631 total incidents and 43 violent crimes recorded. "
        "Housing comes in at 58.3/100 (AVERAGE grade). "
        "Assessed values average $831,215 and estimated rent is ~$6,650/month. "
        "Overall Allston is reasonably safe but not cheap — a trade-off typical "
        "of Boston's inner neighbourhoods. "
        "Sources: [graph] [structured mart] [RAG chunks]"
    )

    result = validate_graph_output(
        query="Is Allston safe and affordable?",   # Schools NOT in scope
        answer=scoped_draft,
        graph_ctx=GRAPH_CTX,
        struct_ctx=STRUCT_CTX,
        rag_chunks=RAG_CHUNKS,
    )

    # Extract any missing_insights flags from raw GPT-4o issues
    raw_issues = {}
    for check in result["feedback"]["checks"].values():
        if "missing_insights" in str(check.get("issues", [])).lower():
            raw_issues["found_missing_insights"] = check["issues"]

    missing_school_flagged = any(
        "school" in str(i).lower() or "transit" in str(i).lower()
        for check in result["feedback"]["checks"].values()
        for i in check.get("issues", [])
    )

    print(f"  needs_improvement         : {result['feedback']['needs_improvement']}")
    print(f"  Schools/Transit flagged   : {missing_school_flagged}  (expected: False)")
    print(f"\n  All issues:")
    for name, check in result["feedback"]["checks"].items():
        for issue in check.get("issues", []):
            print(f"    [{name}] {issue[:100]}")

    assert not missing_school_flagged, (
        "❌ FAIL: Schools/Transit were flagged as missing even though "
        "query only asked about Safety and Housing"
    )
    print("\n  ✅ TEST 3 PASSED — SCOPING RULE correctly suppressed out-of-scope flags")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 4 — validate() API path (via UniversalValidator directly)
# Tests: AgentType.GRAPH_QUERY enum, ValidationResult shape, conn=None safety
# ══════════════════════════════════════════════════════════════════════════════
def test_direct_validator_api():
    banner("TEST 4: Direct UniversalValidator(conn=None).validate(GRAPH_QUERY)")

    validator = UniversalValidator(conn=None)
    ctx = {
        "query":        QUERY,
        "answer":       "Allston has a safety score of 50.3/100 (GOOD). Housing 58.3/100 (AVERAGE). "
                        "Rents ~$6,650/month. Sources: [graph] [structured mart] [RAG chunks]",
        "graph_ctx":    GRAPH_CTX,
        "struct_ctx":   STRUCT_CTX,
        "rag_chunks":   RAG_CHUNKS,
        "neighborhood": "Allston",
        "domains":      ["Safety", "Housing"],
    }

    vr = validator.validate(AgentType.GRAPH_QUERY, ctx)
    vr.print_summary()

    # Check return shape
    assert hasattr(vr, "passed"),     "❌ ValidationResult missing 'passed'"
    assert hasattr(vr, "improved"),   "❌ ValidationResult missing 'improved'"
    assert hasattr(vr, "checks"),     "❌ ValidationResult missing 'checks'"
    assert hasattr(vr, "all_issues"), "❌ ValidationResult missing 'all_issues'"
    assert vr.agent_type == "graph_query", f"❌ Wrong agent_type: {vr.agent_type}"

    print(f"\n  passed     : {vr.passed}")
    print(f"  improved   : {vr.improved}")
    print(f"  checks ran : {list(vr.checks.keys())}")
    print("\n  ✅ TEST 4 PASSED — ValidationResult shape and agent_type are correct")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(SEP)
    print("  NeighbourWise AI — Graph Validator (merged into universal_validator)")
    print("  Running 4 tests...")
    print(SEP)

    if not os.environ.get("OPENAI_API_KEY"):
        print("⚠️  OPENAI_API_KEY not set — GPT-4o tests will fall back to Claude (Cortex)")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("⚠️  ANTHROPIC_API_KEY not set — improvement step will use Cortex only")

    results = {}
    tests = [
        ("bad_draft",       test_bad_draft_triggers_fail_and_improvement),
        ("good_draft",      test_good_draft_passes),
        ("scoping_rule",    test_scoping_rule_no_penalty_for_out_of_scope_domains),
        ("direct_api",      test_direct_validator_api),
    ]

    for name, fn in tests:
        try:
            fn()
            results[name] = "✅ PASS"
        except AssertionError as e:
            print(f"\n{e}")
            results[name] = "❌ FAIL"
        except Exception as e:
            print(f"\n  💥 Unexpected error in {name}: {e}")
            import traceback; traceback.print_exc()
            results[name] = f"💥 ERROR: {e}"

    banner("SUMMARY")
    for name, status in results.items():
        print(f"  {status}  {name}")

    failed = [k for k, v in results.items() if not v.startswith("✅")]
    print(f"\n  {len(tests) - len(failed)}/{len(tests)} tests passed")
    sys.exit(1 if failed else 0)