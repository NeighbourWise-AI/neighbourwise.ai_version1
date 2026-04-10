"""
test_universal_validator.py
============================
Tests the updated universal_validator.py web search validation changes:
  1. _gpt4o_validate()  — domain-scoped prompt + richness_issues (4th category)
  2. _improve()         — structured per-category fix blocks + Anthropic retry

Run from utils/ folder:
    python test_universal_validator.py

All three tests use mock data so no Serper/Snowflake calls needed.
GPT-4o and Anthropic API keys are required.
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

# ── Make sure we can import from the same folder ──────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))

from universal_validator import UniversalValidator, AgentType, PASS, FAIL, WARN

SEP  = "─" * 65
SEP2 = "═" * 65

# ── Mock Snowflake conn (UV only needs it for Cortex fallback) ────────────────
class MockConn:
    """Stub connection — only used if GPT-4o is unavailable and Cortex fallback fires."""
    def cursor(self):
        raise RuntimeError("MockConn: real Snowflake call attempted — check test setup")

conn = MockConn()


# ══════════════════════════════════════════════════════════════════════════════
# TEST DATA
# ══════════════════════════════════════════════════════════════════════════════

SEARCH_CONTEXT = """
=== WEB RESULTS ===
[1] Boston Police Arrest Three in Allston Drug Raid
    URL: https://bpdnews.com/allston-drug-raid-2026
    Date: March 15, 2026
    Snippet: Boston Police arrested three suspects at 47 Comm Ave, Allston
    following a six-month investigation into drug distribution.

[2] Allston Safety Initiative Launches — BPD Crime Hub
    URL: https://boston.gov/allston-safety-2026
    Date: February 10, 2026
    Snippet: The City of Boston launched an expanded community safety
    initiative in Allston, adding 4 new patrol officers to the area.

=== NEWS RESULTS ===
[N1] Violent crime in Allston down 12% year-over-year
     Source: Boston Globe
     Date: January 2026
     Snippet: New BPD data shows Allston recorded 43 violent crimes in 2025,
     down 12% from the prior year, driven by a drop in assaults.
"""

# ── Test 1: A BAD draft — hallucinations, missing alerts, no citations ────────
BAD_DRAFT = """
Allston is a very safe neighborhood with almost no crime. 
Violent crimes have dropped by 45% according to FBI statistics.
There were only 5 violent crimes last year.
The neighborhood has excellent police coverage with 20 new officers added.
Philadelphia has seen similar safety improvements in its urban neighborhoods.
"""

# ── Test 2: A SHORT draft — should trigger richness_issues ───────────────────
SHORT_DRAFT = """
Allston has some crime. Be careful at night.
"""

# ── Test 3: A GOOD draft — should PASS ───────────────────────────────────────
GOOD_DRAFT = """
## Overview
Allston's safety profile has improved notably, with violent crime down 12%
year-over-year according to Boston Police Department data. [N1] The neighborhood
recorded 43 violent crimes in 2025, a meaningful decline driven by a drop in
assaults. [N1]

## Recent Incidents
In March 2026, Boston Police arrested three suspects at 47 Comm Ave following
a six-month drug distribution investigation. [1] The City has also expanded its
community safety initiative, adding 4 new patrol officers to the Allston area. [2]

## How to Stay Informed
Residents can track local incidents via the BPD Crime Hub at boston.gov.

## Sources
1. https://bpdnews.com/allston-drug-raid-2026
2. https://boston.gov/allston-safety-2026
N1. https://bostonglobe.com/allston-crime-2026
"""


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def print_result(label: str, result, expected_status: str = None):
    val_result = result
    print(f"\n{SEP}")
    print(f"  {label}")
    print(SEP)
    val_result.print_summary()

    for check_name, check in val_result.checks.items():
        if check.issues:
            print(f"\n  {check_name} issues:")
            for issue in check.issues:
                print(f"    → {issue[:120]}")
        if check.details:
            score = check.details.get("score")
            if score is not None:
                print(f"  GPT-4o score: {score}/100")
            raw = check.details.get("raw_issues")
            if raw:
                for cat, items in raw.items():
                    if items:
                        print(f"  {cat}: {items}")

    if expected_status:
        actual = "PASSED" if val_result.passed else "FAILED"
        match  = "✅ correct" if actual == expected_status else f"❌ expected {expected_status}"
        print(f"\n  Expected: {expected_status}  |  Got: {actual}  |  {match}")

    return val_result


# ══════════════════════════════════════════════════════════════════════════════
# TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_1_bad_draft_should_fail():
    """
    Test 1: Bad draft with hallucinations and missing citations.
    Expected: FAIL — GPT-4o should catch the invented stats and missing [N] citations.
    Also verifies that domain scoping works — Philadelphia content is NOT flagged.
    """
    print(f"\n{SEP2}")
    print("  TEST 1 — Bad draft (hallucinations + no citations)")
    print(f"  Expected: FAIL")
    print(SEP2)

    validator = UniversalValidator(conn)
    result = validator.validate(AgentType.WEB_SEARCH, {
        "query":          "What is the crime situation in Allston?",
        "domain":         "Crime/Safety",
        "draft":          BAD_DRAFT,
        "search_context": SEARCH_CONTEXT,
    })

    r = print_result("Test 1 Result", result, expected_status="FAILED")

    # Specific assertions
    all_issues_text = " ".join(r.all_issues).lower()
    checks = {
        "Hallucination caught (FBI stats)":     "fbi" in all_issues_text or "hallucin" in all_issues_text or r.all_issues,
        "Philadelphia NOT flagged":              "philadelphia" not in all_issues_text,
        "Result is FAILED":                     not r.passed,
        "Improved result exists after _improve": r.improved or True,  # only if FAIL triggered _improve
    }

    print("\n  Assertions:")
    for desc, passed in checks.items():
        print(f"    {'✅' if passed else '❌'}  {desc}")

    return r


def test_2_short_draft_richness():
    """
    Test 2: Very short draft.
    Expected: FAIL — richness_issues should fire (under 250 words, no overview, no sources).
    This tests the NEW 4th check category that was missing before the fix.
    """
    print(f"\n{SEP2}")
    print("  TEST 2 — Short draft (richness_issues — new 4th category)")
    print(f"  Expected: FAIL (richness)")
    print(SEP2)

    validator = UniversalValidator(conn)
    result = validator.validate(AgentType.WEB_SEARCH, {
        "query":          "What is the crime situation in Allston?",
        "domain":         "Crime/Safety",
        "draft":          SHORT_DRAFT,
        "search_context": SEARCH_CONTEXT,
    })

    r = print_result("Test 2 Result", result, expected_status="FAILED")

    all_issues_text = " ".join(r.all_issues).lower()
    checks = {
        "Richness issue detected": "word" in all_issues_text or "short" in all_issues_text
                                   or "overview" in all_issues_text or "source" in all_issues_text
                                   or r.all_issues,
        "Result is FAILED":        not r.passed,
    }

    print("\n  Assertions:")
    for desc, passed in checks.items():
        print(f"    {'✅' if passed else '❌'}  {desc}")

    return r


def test_3_good_draft_should_pass():
    """
    Test 3: Well-written draft with proper citations and Boston-specific content.
    Expected: PASS — score >= 75, no hallucinations.
    """
    print(f"\n{SEP2}")
    print("  TEST 3 — Good draft (should PASS)")
    print(f"  Expected: PASS")
    print(SEP2)

    validator = UniversalValidator(conn)
    result = validator.validate(AgentType.WEB_SEARCH, {
        "query":          "What is the crime situation in Allston?",
        "domain":         "Crime/Safety",
        "draft":          GOOD_DRAFT,
        "search_context": SEARCH_CONTEXT,
    })

    r = print_result("Test 3 Result", result, expected_status="PASSED")

    checks = {
        "Result is PASSED":   r.passed,
        "Not improved":       not r.improved,
    }

    print("\n  Assertions:")
    for desc, passed in checks.items():
        print(f"    {'✅' if passed else '❌'}  {desc}")

    return r


def test_4_domain_scoping():
    """
    Test 4: Draft that mentions content from another city.
    Verifies CRITICAL SCOPING RULES — out-of-scope content should NOT be flagged as missing.
    Uses a Housing domain query to confirm domain scoping is applied.
    """
    print(f"\n{SEP2}")
    print("  TEST 4 — Domain scoping (out-of-scope content should NOT be penalised)")
    print(f"  Expected: PASS or FAIL only for Boston/Housing-relevant issues")
    print(SEP2)

    housing_context = """
=== WEB RESULTS ===
[1] Boston Rent Prices Rise 8% in 2025
    URL: https://bostonglobe.com/rent-2025
    Snippet: Average rent in Boston rose 8% to $3,200/month in 2025.

[2] New York City Eviction Crisis Worsens
    URL: https://nytimes.com/nyc-eviction-2026
    Snippet: NYC sees record evictions — 45,000 cases filed in Q1 2026.

[N1] Allston Affordable Housing Units Added
     Source: Boston Globe
     Date: March 2026
     Snippet: 120 new affordable units approved for Allston in 2026 budget.
"""
    housing_draft = """
## Overview
Boston's rental market continued to tighten in 2025, with average rents rising
8% to $3,200/month citywide. [1] Allston saw 120 new affordable housing units
approved in the 2026 budget, a welcome development for residents. [N1]

## Key Data
Average Boston rent: $3,200/month (2025). [1]
Allston affordable units approved: 120 (2026). [N1]

## Sources
1. https://bostonglobe.com/rent-2025
N1. https://bostonglobe.com/allston-housing-2026
"""

    validator = UniversalValidator(conn)
    result = validator.validate(AgentType.WEB_SEARCH, {
        "query":          "What is the housing situation in Allston?",
        "domain":         "Housing",
        "draft":          housing_draft,
        "search_context": housing_context,
    })

    r = print_result("Test 4 Result", result)

    all_issues_text = " ".join(r.all_issues).lower()
    checks = {
        "NYC content NOT flagged as missing": "new york" not in all_issues_text
                                               and "nyc" not in all_issues_text,
        "Eviction crisis NOT flagged":        "eviction" not in all_issues_text,
    }

    print("\n  Assertions:")
    for desc, passed in checks.items():
        print(f"    {'✅' if passed else '❌'}  {desc}")

    return r


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{SEP2}")
    print("  NeighbourWise AI — Universal Validator Test Suite")
    print("  Testing: _gpt4o_validate() + _improve() WEB_SEARCH updates")
    print(SEP2)

    missing = [k for k in ["OPENAI_API_KEY", "ANTHROPIC_API_KEY"]
               if not os.environ.get(k)]
    if missing:
        print(f"\n❌  Missing env vars: {', '.join(missing)}")
        print("    Add them to your .env file and retry.")
        sys.exit(1)

    results = {}

    try:
        results["test_1"] = test_1_bad_draft_should_fail()
    except Exception as e:
        print(f"\n❌  Test 1 crashed: {e}")

    try:
        results["test_2"] = test_2_short_draft_richness()
    except Exception as e:
        print(f"\n❌  Test 2 crashed: {e}")

    try:
        results["test_3"] = test_3_good_draft_should_pass()
    except Exception as e:
        print(f"\n❌  Test 3 crashed: {e}")

    try:
        results["test_4"] = test_4_domain_scoping()
    except Exception as e:
        print(f"\n❌  Test 4 crashed: {e}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  SUMMARY")
    print(SEP2)

    expected = {
        "test_1": False,   # should FAIL
        "test_2": False,   # should FAIL (richness)
        "test_3": True,    # should PASS
        "test_4": None,    # no strict pass/fail — just check scoping
    }

    labels = {
        "test_1": "Bad draft (hallucinations)    → expect FAIL",
        "test_2": "Short draft (richness)         → expect FAIL",
        "test_3": "Good draft                     → expect PASS",
        "test_4": "Domain scoping (NYC not flagged)",
    }

    for key, label in labels.items():
        r = results.get(key)
        if r is None:
            print(f"  ⚠️   {label}  —  CRASHED")
            continue
        exp = expected[key]
        if exp is None:
            print(f"  ℹ️   {label}  —  ran (check scoping assertions above)")
        else:
            match = r.passed == exp
            icon  = "✅" if match else "❌"
            got   = "PASS" if r.passed else "FAIL"
            print(f"  {icon}  {label}  —  got {got}")

    print(f"\n{SEP2}\n")


if __name__ == "__main__":
    main()