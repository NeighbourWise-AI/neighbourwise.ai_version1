"""
validator_agent.py
------------------
NeighbourWise AI — GPT-4o Validator Agent

Flow:
  1. Takes original query + Claude draft + raw Serper context
  2. GPT-4o validates ONLY on content relevant to the query location/domain
  3. Returns structured verdict: PASS or FAIL + specific issues
  4. On FAIL → Claude regenerates with targeted fix instructions
  5. Returns final validated result

Usage (standalone):
    python validator_agent.py

Usage (imported):
    from validator_agent import validate_and_regenerate
    result = validate_and_regenerate(query, domain, draft_output, search_context)
"""

import os
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
import anthropic


def call_claude_with_retry(client, max_retries=3, wait_seconds=15, **kwargs):
    """
    Wraps anthropic client.messages.create with automatic retry.
    Retries on 529 Overloaded with increasing wait: 15s, 30s, 45s.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return client.messages.create(**kwargs)
        except anthropic.OverloadedError:
            if attempt < max_retries:
                wait = wait_seconds * attempt
                print(f"   Anthropic API overloaded — retrying in {wait}s "
                      f"(attempt {attempt}/{max_retries})...")
                time.sleep(wait)
            else:
                raise

# ── Load .env from root ──
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
print(f" Loaded .env from: {env_path}")

openai_client    = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
anthropic_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
SERPER_API_KEY   = os.environ.get("SERPER_API_KEY")

VALIDATOR_MODEL = "gpt-4o"
MAX_RETRIES     = 2
PASS_THRESHOLD  = 75


# ──────────────────────────────────────────────
# STEP 1 — GPT-4o Validator
# ──────────────────────────────────────────────

def validate_output(
    query: str,
    domain: str,
    draft_output: str,
    search_context: str,
) -> dict:
    """
    GPT-4o validates the Claude draft against raw Serper context.
    Only flags issues that are RELEVANT to the query's location and domain.

    Returns:
    {
        "verdict": "PASS" | "FAIL",
        "score": 0-100,
        "issues": {
            "hallucinations":   [...],
            "missing_alerts":   [...],
            "citation_gaps":    [...],
            "richness_issues":  [...],
        },
        "regeneration_prompt": "..."
    }
    """

    system_prompt = f"""You are a quality-control validator for NeighbourWise AI, \
a neighborhood livability platform focused on Greater Boston, MA.

You are validating an AI-generated response about the "{domain}" domain \
for this specific query: "{query}"

CRITICAL SCOPING RULES — read carefully before flagging anything:
- Only flag MISSING ALERTS if the omitted content is DIRECTLY relevant to \
  the queried location (e.g. South Boston, Boston, or Greater Boston area) \
  AND the queried domain ({domain}).
- Do NOT flag content from other cities (Philadelphia, New York, etc.) as missing \
  unless the query explicitly asks for regional comparisons.
- Do NOT flag content from unrelated domains (CDC health alerts, immigration, \
  weather in other states) as missing.
- Do NOT flag general system descriptions (AlertBoston, BPD Crime Hub) as missing \
  if they are already mentioned in the draft.
- A citation gap is ONLY a problem if the sentence makes a SPECIFIC factual claim \
  (date, address, statistic, named incident). General summary sentences do not \
  need citations.

WHAT TO CHECK:
1. HALLUCINATIONS — specific facts (addresses, dates, names, statistics) in the \
   draft that do NOT appear anywhere in the search context. Flag each one.
2. MISSING ALERTS — incidents or safety alerts in the search context that are \
   directly about the queried location AND domain, but completely absent from draft.
3. CITATION GAPS — sentences with specific factual claims (dates, addresses, \
   incident names, statistics) that have no [N] citation.
4. RICHNESS — is the response under 250 words? Missing an overview paragraph? \
   Missing a sources section?

SCORING:
- Start at 100
- Each hallucination: -20 points
- Each genuinely relevant missing alert: -10 points
- Each citation gap on a specific fact: -5 points
- Richness issue: -5 points
- PASS if score >= {PASS_THRESHOLD} AND hallucinations list is empty

Respond ONLY with valid JSON. No prose. Schema:
{{
  "verdict": "PASS" or "FAIL",
  "score": integer 0-100,
  "issues": {{
    "hallucinations":  ["<specific claim> — not found in sources"],
    "missing_alerts":  ["<Boston/local alert from source omitted from draft>"],
    "citation_gaps":   ["<specific factual sentence missing citation>"],
    "richness_issues": ["<specific gap>"]
  }},
  "regeneration_prompt": "<2-3 sentences of targeted fix instructions for Claude>"
}}"""

    user_message = f"""QUERY: {query}
DOMAIN: {domain}

RAW SEARCH CONTEXT (ground truth):
{search_context}

AI DRAFT TO VALIDATE:
{draft_output}

Validate and return JSON verdict."""

    response = openai_client.chat.completions.create(
        model=VALIDATOR_MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_message},
        ]
    )

    raw     = response.choices[0].message.content.strip()
    verdict = json.loads(raw)
    return verdict


# ──────────────────────────────────────────────
# STEP 2 — Claude Regeneration
# ──────────────────────────────────────────────

def regenerate_output(
    query: str,
    domain: str,
    draft_output: str,
    search_context: str,
    verdict: dict,
) -> str:
    """Passes validator verdict back to Claude with targeted fix instructions."""

    domain_ctx = (
        f'This query is about the "{domain}" domain of neighborhood livability.'
        if domain != "All" else "This query covers neighborhood livability."
    )

    issues          = verdict.get("issues", {})
    hallucinations  = issues.get("hallucinations", [])
    missing_alerts  = issues.get("missing_alerts", [])
    citation_gaps   = issues.get("citation_gaps", [])
    richness_issues = issues.get("richness_issues", [])
    regen_prompt    = verdict.get("regeneration_prompt", "")

    fix_blocks = []
    if hallucinations:
        fix_blocks.append(
            "REMOVE these hallucinated claims (not in sources):\n" +
            "\n".join(f"  - {h}" for h in hallucinations)
        )
    if missing_alerts:
        fix_blocks.append(
            "ADD these missing local incidents from the sources:\n" +
            "\n".join(f"  - {m}" for m in missing_alerts)
        )
    if citation_gaps:
        fix_blocks.append(
            "ADD [N] citations to these specific factual claims:\n" +
            "\n".join(f"  - {c}" for c in citation_gaps)
        )
    if richness_issues:
        fix_blocks.append(
            "FIX these richness gaps:\n" +
            "\n".join(f"  - {r}" for r in richness_issues)
        )

    fix_section = "\n\n".join(fix_blocks) if fix_blocks else "General quality improvement."

    system_prompt = f"""You are a neighborhood intelligence analyst for NeighbourWise AI. \
{domain_ctx}

A GPT-4o validator reviewed your previous draft and found specific issues. \
Produce an improved version that fixes every flagged problem below.

RESPONSE STRUCTURE:
1. OVERVIEW PARAGRAPH (3-5 sentences) — situational context, severity, trend. Cite [N].
2. KEY INCIDENTS & ALERTS (one ## section per item) — exact date, time, address, \
   what happened, status. Cite [N].
3. BACKGROUND & CONTEXT — stats, official resources. Cite [N].
4. SOURCES — numbered URL list.

RULES:
- Only include facts that appear in the search context. No invention.
- Only cite local Boston/Greater Boston content relevant to the query.
- Do not include incidents from other cities unless directly requested.
- Every specific factual claim (date, address, statistic) needs a [N] citation.
- No markdown bold (**). Use ## headings only.
- Target 400-600 words."""

    user_message = f"""QUERY: {query}

PREVIOUS DRAFT (fix the issues below — do not copy blindly):
{draft_output}

VALIDATOR ISSUES TO FIX:
{fix_section}

VALIDATOR INSTRUCTION:
{regen_prompt}

RAW SEARCH CONTEXT (your only allowed source of facts):
{search_context}

Write the corrected response now."""

    response = call_claude_with_retry(
        anthropic_client,
        model="claude-sonnet-4-6",
        max_tokens=2000,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}]
    )
    return response.content[0].text.strip()


# ──────────────────────────────────────────────
# MAIN ENTRY POINT
# ──────────────────────────────────────────────

def validate_and_regenerate(
    query: str,
    domain: str,
    draft_output: str,
    search_context: str,
    verbose: bool = True,
) -> dict:
    """
    Full validation + optional regeneration pipeline.

    Returns:
    {
        "final_output":  str,
        "verdict":       dict,   # verdict on original draft
        "final_verdict": dict,   # verdict after last attempt
        "regenerated":   bool,
        "attempts":      int,
        "passed":        bool,
    }
    """
    current_output = draft_output
    first_verdict  = None
    last_verdict   = None
    regenerated    = False
    attempts       = 1

    for attempt in range(MAX_RETRIES + 1):
        if verbose:
            print(f"\n── Validator (attempt {attempt + 1}) ──")

        verdict = validate_output(query, domain, current_output, search_context)
        last_verdict = verdict
        if first_verdict is None:
            first_verdict = verdict

        score  = verdict.get("score", 0)
        result = verdict.get("verdict", "FAIL")
        issues = verdict.get("issues", {})

        if verbose:
            print(f"   Verdict : {result}  |  Score: {score}/100")
            for category, items in issues.items():
                if items:
                    print(f"   {category}:")
                    for item in items:
                        print(f"     - {item}")

        # ── PASS ──
        if result == "PASS":
            if verbose:
                print(f"   Passed on attempt {attempt + 1}.")
            return {
                "final_output":  current_output,
                "verdict":       first_verdict,
                "final_verdict": verdict,
                "regenerated":   regenerated,
                "attempts":      attempts,
                "passed":        True,
            }

        # ── FAIL — regenerate if retries remain ──
        if attempt < MAX_RETRIES:
            if verbose:
                print(f"\n── Regenerating (attempt {attempt + 2}) ──")
            current_output = regenerate_output(
                query, domain, current_output, search_context, verdict
            )
            regenerated = True
            attempts   += 1
        else:
            if verbose:
                print(f"\n   Max retries reached. Returning best available output.")

    return {
        "final_output":  current_output,
        "verdict":       first_verdict,
        "final_verdict": last_verdict,
        "regenerated":   regenerated,
        "attempts":      attempts,
        "passed":        False,
    }


# ──────────────────────────────────────────────
# STANDALONE TEST
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    missing = []
    for key in ["ANTHROPIC_API_KEY", "OPENAI_API_KEY", "SERPER_API_KEY"]:
        if not os.environ.get(key):
            missing.append(key)
    if missing:
        print(f"\nERROR: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

    from app import serper_search, format_web_results, format_news_results

    # ── Change these two lines to test any domain ──
    TEST_QUERY  = "What are the latest MBTA service changes affecting South Boston?"
    TEST_DOMAIN = "MBTA"
    # ────────────────────────────────────────────────

    print(f"\nTest query : {TEST_QUERY}")
    print(f"Domain     : {TEST_DOMAIN}\n")

    # Build a domain-aware news search suffix
    domain_news_keywords = {
        "Crime/Safety":  "crime safety alert incident police",
        "Housing":       "housing rent apartments development",
        "Restaurants":   "restaurant opening closing dining food",
        "Healthcare":    "hospital clinic healthcare medical",
        "Schools":       "school education MCAS district",
        "Grocery":       "grocery store supermarket food market",
        "MBTA":          "MBTA transit bus train service",
        "Weather":       "weather storm flood snow Boston",
    }
    news_suffix = domain_news_keywords.get(TEST_DOMAIN, "Boston 2026")

    print("── Fetching Serper results ──")
    web_data   = serper_search(TEST_QUERY, search_type="search", num_results=10)
    news_data  = serper_search(
        TEST_QUERY + f" {news_suffix} 2025 2026",
        search_type="news", num_results=8
    )
    search_ctx = format_web_results(web_data) + "\n\n" + format_news_results(news_data)

    print("── Deep fetching top URLs ──")
    from app import deep_fetch_top_urls
    fetched = deep_fetch_top_urls(web_data.get("organic", []), TEST_DOMAIN, max_fetch=3)
    if fetched:
        search_ctx += "\n\n" + fetched

    print("── Generating Claude draft ──")
    draft_resp = call_claude_with_retry(
        anthropic_client,
        model="claude-sonnet-4-6",
        max_tokens=2000,
        system=f"""You are a neighborhood intelligence analyst for NeighbourWise AI \
reporting on livability in Boston, MA. Focus on the {TEST_DOMAIN} domain.
Write a rich 400-600 word response structured as:
1. Overview paragraph (3-5 sentences) summarizing key findings. Cite [N].
2. ## section per distinct finding (restaurant, incident, development, etc.) \
   with specific names, dates, addresses where available. Cite [N].
3. Background context and how to stay informed. Cite [N].
4. Numbered Sources list with URLs.
Only include Boston-area content relevant to {TEST_DOMAIN}. \
No markdown bold (**text**). Use ## headings only.""",
        messages=[{"role": "user", "content":
            f"Query: {TEST_QUERY}\n\n--- SEARCH RESULTS ---\n{search_ctx}\n---\n\nWrite your response."}]
    )
    draft = draft_resp.content[0].text.strip()

    print("\n── INITIAL DRAFT ──")
    print(draft)

    print("\n\n══════════════════════════════════")
    print("   RUNNING VALIDATOR AGENT")
    print("══════════════════════════════════")
    result = validate_and_regenerate(
        query=TEST_QUERY,
        domain=TEST_DOMAIN,
        draft_output=draft,
        search_context=search_ctx,
        verbose=True,
    )

    print("\n\n── FINAL OUTPUT ──")
    print(result["final_output"])
    print(f"\n── Summary ──")
    print(f"  Regenerated : {result['regenerated']}")
    print(f"  Attempts    : {result['attempts']}")
    print(f"  Passed      : {result['passed']}")
    print(f"  Final score : {result['final_verdict'].get('score')}/100")
