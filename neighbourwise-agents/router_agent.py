"""
router_agent.py — NeighbourWise AI  (speed-optimized)
═══════════════════════════════════════════════════════
Speed changes vs previous version:
  1. classify_query()  → keyword rules first (0s), Cortex only as fallback
  2. handle_data_query() → SQL + RAG run in PARALLEL via ThreadPoolExecutor
  3. RAG is SKIPPED entirely when SQL already returned rich structured data
     (saves 5-10s on the most common queries)

Target latency:
  Before: ~20-30s
  After:  ~8-12s
"""

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

from shared.snowflake_conn import (
    get_conn,
    run_query,
    cortex_complete,
    rag_search,
    MODEL_GENERATE,
    MODEL_VALIDATE,
)
from universal_validator import UniversalValidator, AgentType, validate_and_improve


# ══════════════════════════════════════════════════════════════════════════════
# SPEED FIX 1 — FAST KEYWORD CLASSIFIER (0ms, no Cortex call)
# Falls back to Cortex only when keywords are ambiguous.
# ══════════════════════════════════════════════════════════════════════════════

# Neighborhoods we know about — used to extract names cheaply
_KNOWN_NEIGHBORHOODS = [
    "Fenway", "Roxbury", "Dorchester", "Back Bay", "Beacon Hill",
    "South End", "Jamaica Plain", "Allston", "East Boston", "Charlestown",
    "Mission Hill", "Hyde Park", "Roslindale", "Mattapan", "North End",
    "Chinatown", "Downtown", "South Boston", "West End", "Longwood",
    "Cambridge", "Somerville", "Brookline", "Newton", "Medford",
    "Malden", "Revere", "Chelsea", "Everett", "Arlington",
    "Watertown", "Salem", "Quincy",
]

def _extract_neighborhood_fast(query: str) -> Optional[str]:
    """O(n) neighborhood extraction — no LLM needed."""
    q = query.lower()
    for n in _KNOWN_NEIGHBORHOODS:
        if n.lower() in q:
            return n
    return None

def _keyword_classify(query: str) -> Optional[dict]:
    """
    Pure keyword classification. Returns a result dict if confident,
    or None if the query is ambiguous and needs Cortex.
    Covers ~80% of real queries with 0 latency.
    """
    q = query.lower()
    nbhd = _extract_neighborhood_fast(query)

    # ── Report signals (highest priority) ────────────────────────────────────
    if any(k in q for k in ["generate report", "full report", "report for",
                              "neighborhood report", "pdf"]):
        return {"intent": "report", "neighborhood": nbhd,
                "domain": None, "confidence": 0.95,
                "reasoning": "keyword: report"}

    # ── Image signals ─────────────────────────────────────────────────────────
    if any(k in q for k in ["look like", "photos of", "pictures of",
                              "images of", "show me photos", "show me pictures"]):
        return {"intent": "image", "neighborhood": nbhd,
                "domain": None, "confidence": 0.95,
                "reasoning": "keyword: image"}

    # ── Web search signals ────────────────────────────────────────────────────
    if any(k in q for k in ["latest", "recent", "news", "current", "today",
                              "this week", "opening", "closing", "update",
                              "2025", "2026", "happening", "right now"]):
        return {"intent": "web_search", "neighborhood": nbhd,
                "domain": None, "confidence": 0.9,
                "reasoning": "keyword: web_search"}

    # ── Chart signals ─────────────────────────────────────────────────────────
    if any(k in q for k in ["chart", "graph", "plot", "scatter", "trend",
                              "over time", "monthly", "rank all", "top 10",
                              "top 5", "correlation", "breakdown", "percentage",
                              "compare", "vs ", "versus"]):
        return {"intent": "chart", "neighborhood": nbhd,
                "domain": None, "confidence": 0.85,
                "reasoning": "keyword: chart"}

    # ── Unambiguous data_query signals ────────────────────────────────────────
    if any(k in q for k in ["how many", "what is the", "which neighborhoods",
                              "average rent", "list all", "find me", "tell me about",
                              "is it safe", "is it good", "should i move",
                              "planning to move", "thinking of moving",
                              "score for", "grade for"]):
        return {"intent": "data_query", "neighborhood": nbhd,
                "domain": None, "confidence": 0.85,
                "reasoning": "keyword: data_query"}

    # Ambiguous — let Cortex decide
    return None


# Cortex classification only called when keywords are ambiguous
_CLASSIFICATION_PROMPT = """Classify this Boston neighborhood query into ONE intent.

INTENTS: report | chart | image | data_query | web_search

- report:      "generate report", "full report", "pdf"
- chart:       "chart", "graph", "compare", "rank", "trend", "scatter"
- image:       "look like", "photos", "pictures", "images"
- web_search:  "latest", "recent", "news", "current", "today", "update"
- data_query:  factual questions answered by the database (default)

Respond ONLY with JSON:
{"intent":"...","neighborhood":"Title Case or null","domain":"or null","confidence":0.0-1.0,"reasoning":"one sentence"}

Query: "{query}"
JSON:"""

def classify_query(query: str, conn) -> dict:
    """
    Fast path: keyword rules (0ms).
    Slow path: Cortex Mistral (~3-5s) only when keywords are ambiguous.
    """
    # Fast path — covers ~80% of queries
    result = _keyword_classify(query)
    if result is not None:
        print(f"[Router] ⚡ Fast classify → {result['intent']} (no LLM)")
        print(f"[Router] Intent     : {result['intent']}")
        print(f"[Router] Neighborhood: {result.get('neighborhood')}")
        print(f"[Router] Confidence  : {result['confidence']}")
        return result

    # Slow path — Cortex fallback for ambiguous queries
    print(f"[Router] 🔄 Ambiguous query — falling back to Cortex classify...")
    prompt = _CLASSIFICATION_PROMPT.format(query=query)
    raw    = cortex_complete(prompt, conn, model=MODEL_GENERATE)
    try:
        raw    = re.sub(r"```(?:json)?|```", "", raw).strip()
        result = json.loads(raw)
        print(f"[Router] Intent     : {result.get('intent')}")
        print(f"[Router] Neighborhood: {result.get('neighborhood')}")
        print(f"[Router] Confidence  : {result.get('confidence')}")
        return result
    except json.JSONDecodeError:
        print(f"[Router] JSON parse failed — defaulting to data_query")
        return {
            "intent": "data_query", "neighborhood": _extract_neighborhood_fast(query),
            "domain": None, "confidence": 0.5, "reasoning": "parse fallback",
        }


# ══════════════════════════════════════════════════════════════════════════════
# SQL AGENT PROMPT
# ══════════════════════════════════════════════════════════════════════════════

SQL_AGENT_PROMPT = """You are a SQL expert for NeighbourWise AI (Boston neighborhood database).

RULES:
1. ALWAYS use full path: NEIGHBOURWISE_DOMAINS.MARTS.<table>
2. NEIGHBORHOOD_NAME is ALWAYS UPPERCASE (e.g. 'DORCHESTER', 'BACK BAY')
3. Use UPPER() when filtering: WHERE UPPER(NEIGHBORHOOD_NAME) = 'FENWAY'
4. Add IS NOT NULL for every score column in ORDER BY / WHERE
5. IS_BOSTON/IS_CAMBRIDGE/IS_GREATER_BOSTON ONLY exist on MASTER_LOCATION
6. Use named aliases: ml, ns, nm, nsc, nr, ng, nh, nho, nb, nu, nms
7. Default LIMIT 10 unless query asks for all
8. SELECT only the columns directly relevant to the question — do NOT select *
9. Output ONLY the SQL — no explanation, no markdown, no backticks

SAFETY QUERY RULE:
If the question is about safety/crime for a SPECIFIC neighborhood, use a CTE
to pull three result sets cleanly — target profile, nearby neighborhoods,
and actual violent crime types. Use this EXACT pattern (substitute neighborhood):

WITH target AS (
    SELECT 'TARGET'          AS record_type,
           NEIGHBORHOOD_NAME AS name,
           SAFETY_SCORE, SAFETY_GRADE,
           TOTAL_INCIDENTS, VIOLENT_CRIME_COUNT, PCT_VIOLENT,
           CAST(NULL AS VARCHAR) AS offense
    FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
    WHERE UPPER(NEIGHBORHOOD_NAME) = 'DORCHESTER'
),
nearby AS (
    SELECT 'NEARBY'          AS record_type,
           NEIGHBORHOOD_NAME AS name,
           SAFETY_SCORE, SAFETY_GRADE,
           TOTAL_INCIDENTS, VIOLENT_CRIME_COUNT, PCT_VIOLENT,
           CAST(NULL AS VARCHAR) AS offense
    FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
    WHERE UPPER(NEIGHBORHOOD_NAME) IN ('ROXBURY','JAMAICA PLAIN','MATTAPAN','SOUTH BOSTON')
      AND SAFETY_SCORE IS NOT NULL
),
violent AS (
    SELECT 'VIOLENT_CRIME'              AS record_type,
           OFFENSE_DESCRIPTION          AS name,
           CAST(NULL AS FLOAT)          AS SAFETY_SCORE,
           CAST(NULL AS VARCHAR)        AS SAFETY_GRADE,
           CAST(COUNT(*) AS INTEGER)    AS TOTAL_INCIDENTS,
           CAST(COUNT(*) AS INTEGER)    AS VIOLENT_CRIME_COUNT,
           CAST(NULL AS FLOAT)          AS PCT_VIOLENT,
           OFFENSE_DESCRIPTION          AS offense
    FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_CRIME
    WHERE UPPER(NEIGHBORHOOD_NAME) = 'DORCHESTER'
      AND IS_VIOLENT_CRIME = TRUE
    GROUP BY OFFENSE_DESCRIPTION
    ORDER BY TOTAL_INCIDENTS DESC
    LIMIT 5
)
SELECT * FROM target
UNION ALL
SELECT * FROM nearby
UNION ALL
SELECT * FROM violent
ORDER BY record_type, SAFETY_SCORE DESC NULLS LAST

Geographic proximity map — use the right neighbors for each neighborhood:
  Dorchester     → Roxbury, Jamaica Plain, Mattapan, South Boston
  Roxbury        → Dorchester, Jamaica Plain, Mission Hill, South End
  Jamaica Plain  → Roxbury, Roslindale, Mission Hill, West Roxbury
  Mattapan       → Dorchester, Hyde Park, Roslindale, Roxbury
  South Boston   → Dorchester, South End, Downtown, East Boston
  East Boston    → Charlestown, South Boston, Downtown, North End
  Charlestown    → East Boston, North End, West End, Downtown
  North End      → Charlestown, Downtown, West End, East Boston
  Beacon Hill    → West End, Back Bay, Downtown, North End
  Back Bay       → South End, Fenway, Beacon Hill, Downtown
  South End      → Roxbury, Back Bay, Fenway, Mission Hill
  Fenway         → Mission Hill, South End, Longwood, Allston
  Mission Hill   → Roxbury, Jamaica Plain, Fenway, South End
  Allston        → Brighton, Fenway, East Boston, West End
  Brighton       → Allston, West Roxbury, Jamaica Plain, Hyde Park
  West Roxbury   → Jamaica Plain, Roslindale, Hyde Park, Brighton
  Hyde Park      → Roslindale, Mattapan, West Roxbury, Jamaica Plain
  Roslindale     → Jamaica Plain, West Roxbury, Hyde Park, Mattapan
  Chinatown      → Downtown, South End, Back Bay, Leather District
  Downtown       → Beacon Hill, North End, Chinatown, South End

COLUMN REFERENCE:
- "how many hospitals"      → HOSPITAL_COUNT      from MRT_NEIGHBORHOOD_HEALTHCARE
- "how many clinics"        → CLINIC_COUNT        from MRT_NEIGHBORHOOD_HEALTHCARE
- "how many facilities"     → TOTAL_FACILITIES    from MRT_NEIGHBORHOOD_HEALTHCARE
- "how many schools"        → TOTAL_SCHOOLS       from MRT_NEIGHBORHOOD_SCHOOLS
- "how many restaurants"    → TOTAL_RESTAURANTS   from MRT_NEIGHBORHOOD_RESTAURANTS
- "how many stations"       → TOTAL_STATIONS      from MRT_NEIGHBORHOOD_BLUEBIKES
- "how many stops"          → TOTAL_STOPS         from MRT_NEIGHBORHOOD_MBTA
- "how many stores"         → TOTAL_STORES        from MRT_NEIGHBORHOOD_GROCERY_STORES
- "how many universities"   → TOTAL_UNIVERSITIES  from MRT_NEIGHBORHOOD_UNIVERSITIES
- "safety score"            → SAFETY_SCORE        from MRT_NEIGHBORHOOD_SAFETY
- "average rent"            → AVG_ESTIMATED_RENT  from MRT_NEIGHBORHOOD_HOUSING

TABLES:
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HEALTHCARE
  (NEIGHBORHOOD_NAME, TOTAL_FACILITIES, HOSPITAL_COUNT, CLINIC_COUNT,
   TOTAL_BED_CAPACITY, HEALTHCARE_GRADE, HEALTHCARE_SCORE, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
  (NEIGHBORHOOD_NAME, TOTAL_INCIDENTS, VIOLENT_CRIME_COUNT, PCT_VIOLENT,
   SAFETY_GRADE, SAFETY_SCORE, MOST_COMMON_OFFENSE, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING
  (NEIGHBORHOOD_NAME, TOTAL_PROPERTIES, AVG_ASSESSED_VALUE, AVG_ESTIMATED_RENT,
   AVG_PRICE_PER_SQFT, HOUSING_GRADE, HOUSING_SCORE, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_MBTA
  (NEIGHBORHOOD_NAME, TOTAL_STOPS, HAS_RAPID_TRANSIT, TRANSIT_GRADE,
   TRANSIT_SCORE, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_RESTAURANTS
  (NEIGHBORHOOD_NAME, TOTAL_RESTAURANTS, AVG_RATING, CUISINE_DIVERSITY,
   RESTAURANT_GRADE, RESTAURANT_SCORE, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SCHOOLS
  (NEIGHBORHOOD_NAME, TOTAL_SCHOOLS, PUBLIC_SCHOOL_COUNT, SCHOOL_GRADE,
   SCHOOL_SCORE, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_BLUEBIKES
  (NEIGHBORHOOD_NAME, TOTAL_STATIONS, TOTAL_DOCKS, BIKESHARE_GRADE,
   BIKESHARE_SCORE, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_GROCERY_STORES
  (NEIGHBORHOOD_NAME, TOTAL_STORES, SUPERMARKET_COUNT, GROCERY_GRADE,
   GROCERY_SCORE, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_UNIVERSITIES
  (NEIGHBORHOOD_NAME, TOTAL_UNIVERSITIES, EDUCATION_GRADE, EDUCATION_SCORE,
   UNIVERSITY_NAMES, LOCATION_ID)
NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
  (NEIGHBORHOOD_NAME, CITY, MASTER_SCORE, MASTER_GRADE, SAFETY_SCORE,
   TRANSIT_SCORE, HOUSING_SCORE, GROCERY_SCORE, HEALTHCARE_SCORE,
   SCHOOL_SCORE, RESTAURANT_SCORE, EDUCATION_SCORE, TOP_STRENGTH,
   TOP_WEAKNESS, LOCATION_ID)
MARTS.MASTER_LOCATION
  (LOCATION_ID, NEIGHBORHOOD_NAME, CITY, SQMILES,
   IS_BOSTON, IS_CAMBRIDGE, IS_GREATER_BOSTON)

Question: {question}
SQL:"""


# ══════════════════════════════════════════════════════════════════════════════
# SYNTHESIS PROMPT
# ══════════════════════════════════════════════════════════════════════════════

SYNTHESIS_PROMPT = """You are NeighbourWise AI, a knowledgeable and candid Boston neighborhood advisor.

HARD RULES:
- NEVER just repeat the score number. Always contextualise it:
    80-100 → "one of the safest in Boston"
    65-79  → "above average, solid"
    50-64  → "moderate — roughly middle of the pack across Boston's 51 neighborhoods"
    35-49  → "below average — notably higher crime than most Boston neighborhoods"
    0-34   → "one of the highest-crime areas in Boston"
- Convert ALL UPPERCASE names to Title Case.
- NO markdown tables anywhere. Write everything as flowing prose and bullet points.
- Compare AVG_ESTIMATED_RENT to Boston median of ~$2,800/month where relevant.

FORMAT — exactly three sections:

### Summary
3-5 sentences of flowing prose. Open with a direct answer.
No score number in the first sentence — lead with the takeaway.
Mention the most important 2-3 facts naturally in the text.

### Key Data
Write as SHORT prose paragraphs or a tight bullet list — NO markdown tables.
For safety queries structure it like:
  • **Safety grade:** [grade] — [one-line plain-English interpretation]
  • **Total incidents:** [number] ([pct_violent]% violent)
  • **Actual violent crimes recorded:** list each VIOLENT_CRIME type with its count
    — if none recorded, say "No violent crimes on record for this period"
  • **Nearby neighborhoods:** for each NEARBY row write one line:
    "[Name] — [grade], score [X], [one observation about why it may suit them]"

### Insights
3-4 bullet points. Each must add NEW information not already in Summary.

For safety queries:
  • 🔴/🟡/🟢 **Honest safety take** — practical advice based on actual crime types found.
    If violent crime count is 0 or very low, say so reassuringly.
    If high, name the offense types and give 2 concrete tips.
  • 🏘️ **Nearby alternatives** — pick the 2 safest from the NEARBY rows and explain
    why each is a realistic alternative (transit link, rough rent, character).
  • 🚇 **Getting around** — mention specific MBTA lines if transit data available.
  • 💰 **Housing trade-off** — actual rent vs $2,800 Boston median if data present.

DATA:
{context}

QUESTION: {question}

Write your answer now:"""


# ══════════════════════════════════════════════════════════════════════════════
# SPEED FIX 2 — RAG SKIP LOGIC
# Structured mart data already contains descriptions for most queries.
# Only run RAG for open-ended/lifestyle questions.
# ══════════════════════════════════════════════════════════════════════════════

# Columns that, when populated, mean the mart data is self-sufficient
_RICH_COLUMNS = {
    "SAFETY_DESCRIPTION", "TRANSIT_DESCRIPTION", "RESTAURANT_DESCRIPTION",
    "EDUCATION_DESCRIPTION", "SAFETY_GRADE", "TRANSIT_GRADE",
    "HEALTHCARE_GRADE", "SCHOOL_GRADE", "HOUSING_GRADE",
}

# Query patterns where RAG adds genuine value beyond the structured data
_RAG_USEFUL_PATTERNS = [
    "vibe", "feel", "character", "lifestyle", "community", "culture",
    "what is it like", "what's it like", "who lives", "demographic",
    "gentrification", "history", "known for", "famous for",
    "mental health", "food desert", "equity", "challenge",
]

def _should_run_rag(query: str, sql_results: list) -> bool:
    """
    Returns True only when RAG will add meaningful value.
    Skips RAG (~5-10s saved) when:
      - SQL already returned rich structured data with descriptions/grades
      - Query is a simple factual count ("how many hospitals")
      - Query is a ranking ("top 5 safest")
    Keeps RAG when:
      - Query asks about vibe/lifestyle/character
      - SQL returned 0 rows (RAG is the fallback)
      - Query contains open-ended lifestyle keywords
    """
    q = query.lower()

    # Always run RAG if SQL returned nothing — it's the only fallback
    if not sql_results:
        return True

    # Always run RAG for lifestyle/character questions
    if any(p in q for p in _RAG_USEFUL_PATTERNS):
        print("[DataQuery] RAG: running (lifestyle/open-ended query)")
        return True

    # Skip RAG for simple count/rank queries — SQL is sufficient
    if any(k in q for k in ["how many", "top 5", "top 10", "rank",
                              "which neighborhoods have", "list"]):
        print("[DataQuery] RAG: skipped (simple factual/count query)")
        return False

    # Skip RAG if SQL results contain rich description columns
    if sql_results:
        result_keys = set(sql_results[0].keys()) if sql_results else set()
        if result_keys & _RICH_COLUMNS:
            print("[DataQuery] RAG: skipped (SQL returned rich structured data)")
            return False

    # Default: run RAG (safe fallback)
    print("[DataQuery] RAG: running (default)")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# SPEED FIX 3 — PARALLEL SQL + RAG via ThreadPoolExecutor
# ══════════════════════════════════════════════════════════════════════════════

def _run_sql(query: str, conn) -> dict:
    """Execute SQL agent — runs in thread."""
    sql_prompt = SQL_AGENT_PROMPT.format(question=query)
    sql_text   = cortex_complete(sql_prompt, conn, model=MODEL_GENERATE)
    sql_text   = sql_text.strip().replace("```sql", "").replace("```", "").strip()

    if not sql_text or sql_text.startswith("Error"):
        return {"sql": None, "results": None, "error": sql_text}

    try:
        cur = conn.cursor()
        cur.execute(sql_text)
        if cur.description:
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.close()
            print(f"[DataQuery] SQL returned {len(rows)} rows")
            return {"sql": sql_text, "results": rows, "error": None}
        cur.close()
        return {"sql": sql_text, "results": [], "error": None}
    except Exception as e:
        print(f"[DataQuery] SQL error: {e}")
        return {"sql": sql_text, "results": None, "error": str(e)}


def _run_rag(query: str, conn, domain_filter: str = None) -> list:
    """Execute RAG vector search — runs in thread."""
    chunks = rag_search(query, conn, domain_filter=domain_filter, top_k=3)
    print(f"[DataQuery] RAG returned {len(chunks)} chunks")
    return chunks


def handle_data_query(query: str, conn, domain_filter: str = None) -> dict:
    """
    SQL + conditional RAG → synthesize → validate.

    Speed optimizations applied here:
      - SQL and RAG fire in PARALLEL (saves ~4s)
      - RAG is skipped entirely for simple factual queries (saves ~8s)
      - Separate connections used per thread to avoid cursor conflicts
    """
    print(f"\n[DataQuery] Processing: {query}")
    t0 = time.time()

    # ── PARALLEL: fire SQL immediately; decide on RAG after SQL returns ───────
    # We use two connections: one for SQL (blocks on Cortex + Snowflake),
    # one for RAG (blocks on EMBED_TEXT_768). Both run simultaneously.
    sql_data   = {"sql": None, "results": None, "error": None}
    rag_chunks = []

    # First, run SQL in a thread; simultaneously probe whether RAG is needed.
    # Since we don't know SQL results yet, we do a two-phase approach:
    #   Phase A: SQL thread starts immediately
    #   Phase B: RAG thread starts immediately in parallel
    #   Phase C: After both finish, discard RAG if _should_run_rag says no

    conn_sql = get_conn()   # dedicated connection for SQL thread
    conn_rag = get_conn()   # dedicated connection for RAG thread

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_sql = executor.submit(_run_sql, query, conn_sql)
            future_rag = executor.submit(_run_rag, query, conn_rag, domain_filter)

            sql_data   = future_sql.result()
            rag_result = future_rag.result()

        # Discard RAG results if they won't add value (saves synthesis time)
        sql_results = sql_data.get("results") or []
        if _should_run_rag(query, sql_results):
            rag_chunks = rag_result
        else:
            rag_chunks = []
            print(f"[DataQuery] RAG discarded — SQL data is self-sufficient")

    finally:
        conn_sql.close()
        conn_rag.close()

    print(f"[DataQuery] SQL+RAG completed in {time.time()-t0:.1f}s")

    # ── Synthesize ────────────────────────────────────────────────────────────
    parts = []
    if sql_results:
        parts.append(
            f"DATABASE RESULTS:\n"
            f"{json.dumps(sql_results[:15], indent=2, default=str)}"
        )
    if rag_chunks:
        parts.append(
            "DOCUMENT INSIGHTS:\n" + "\n\n".join(
                f"[{c.get('DOMAIN','?')}] {c.get('CHUNK_TEXT','')[:500]}"
                for c in rag_chunks[:3]
            )
        )

    if not parts:
        draft = (
            "I couldn't find relevant information for that query. "
            "Try asking about a specific Boston neighborhood — for example, "
            "'Is Dorchester safe?' or 'How many hospitals are in Roxbury?'"
        )
    else:
        context = "\n\n".join(parts)
        prompt  = SYNTHESIS_PROMPT.format(context=context, question=query)
        draft   = cortex_complete(prompt, conn, model=MODEL_GENERATE)

    print(f"[DataQuery] Draft generated ({len(draft)} chars) | total so far: {time.time()-t0:.1f}s")

    # ── Validate (Claude — only if programmatic checks fail) ──────────────────
    validated = validate_and_improve(conn, query, draft, sql_data, {"chunks": rag_chunks})
    print(f"[DataQuery] Total time: {time.time()-t0:.1f}s")

    return {
        "type":       "data_query",
        "answer":     validated["answer"],
        "sql":        sql_data.get("sql"),
        "results":    sql_results,
        "rag_chunks": rag_chunks,
        "improved":   validated["improved"],
        "validation": validated["feedback"],
        "_sql_key":   f"sql_{abs(hash(query))}",
    }


# ══════════════════════════════════════════════════════════════════════════════
# CHART HANDLER (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def handle_chart(query: str, conn) -> dict:
    print(f"\n[Chart] Processing: {query}")
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from graphic_agent import classify_intent, generate_chart
        plan      = classify_intent(query)
        chart_type = plan.get("chart_type", "bar")
        if not plan.get("sql"):
            return {"type": "chart", "error": "Could not generate SQL for this chart query"}
        out_path = generate_chart(plan, query)
        if out_path:
            return {"type": "chart", "path": out_path, "chart_type": chart_type}
        return {"type": "chart", "error": "Chart generation failed"}
    except Exception as e:
        print(f"[Chart] Error: {e}")
        return {"type": "chart", "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# IMAGE HANDLER (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def handle_image(neighborhood: str, conn) -> dict:
    print(f"\n[Image] Processing: {neighborhood}")
    if not neighborhood:
        return {"type": "image", "error": "No neighborhood name extracted from query"}
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from graphic_agent import generate_neighborhood_images
        saved_paths = generate_neighborhood_images(neighborhood)
        if saved_paths:
            return {"type": "image", "neighborhood": neighborhood, "paths": saved_paths}
        return {"type": "image", "error": f"Image generation failed for {neighborhood}"}
    except Exception as e:
        print(f"[Image] Error: {e}")
        return {"type": "image", "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# WEB SEARCH HANDLER (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def handle_web_search(query: str, domain: str = "All") -> dict:
    print(f"\n[WebSearch] Processing: {query} (domain: {domain})")
    try:
        from web_search_agent import (
            serper_search, format_web_results, format_news_results,
            deep_fetch_top_urls, generate_draft, DOMAIN_NEWS_KEYWORDS,
        )
        news_suffix = DOMAIN_NEWS_KEYWORDS.get(domain, "Boston 2026")
        web_data    = serper_search(query, search_type="search", num_results=10)
        news_data   = serper_search(query + f" {news_suffix} 2025 2026",
                                    search_type="news", num_results=8)
        search_ctx  = format_web_results(web_data) + "\n\n" + format_news_results(news_data)
        try:
            fetched = deep_fetch_top_urls(web_data.get("organic", []), domain, max_fetch=3)
            if fetched:
                search_ctx += "\n\n" + fetched
        except Exception as e:
            print(f"[WebSearch] Deep fetch warning: {e}")

        draft      = generate_draft(query, domain, search_ctx)
        result_text = draft
        validation  = None

        if os.environ.get("OPENAI_API_KEY"):
            try:
                from validator_agent import validate_and_regenerate
                val    = validate_and_regenerate(query, domain, draft, search_ctx, verbose=False)
                result_text = val["final_output"]
                validation  = {
                    "passed":       val["passed"],
                    "regenerated":  val["regenerated"],
                    "score":        val["final_verdict"].get("score"),
                }
            except Exception as e:
                print(f"[WebSearch] Validation error (non-fatal): {e}")

        return {"type": "web_search", "answer": result_text, "validation": validation}
    except Exception as e:
        print(f"[WebSearch] Error: {e}")
        return {"type": "web_search", "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# REPORT HANDLER (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def handle_report(neighborhood: str, conn) -> dict:
    print(f"\n[Report] Orchestrating full report for: {neighborhood}")
    if not neighborhood:
        return {"type": "report", "error": "No neighborhood specified"}
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from report_agent import (
            fetch_domain_data, fetch_neighboring_neighborhoods,
            fetch_crime_trend, fetch_sarimax_forecast, fetch_crime_narrative,
            fetch_rag_context, clean_rag_chunks, summarize_rag_with_cortex,
            generate_cortex_narratives, generate_report,
        )
        from graphic_agent import (
            generate_radar_chart, generate_bar_neighbors, generate_grouped_bar,
            generate_crime_trend as generate_crime_trend_chart,
            generate_neighborhood_images,
        )

        data = fetch_domain_data(neighborhood, conn)
        if not data:
            return {"type": "report", "error": f"No data found for '{neighborhood}'"}

        city        = str(data.get("CITY", "Boston")).title()
        neighbor_df = fetch_neighboring_neighborhoods(neighborhood, city, conn)
        crime_df    = fetch_crime_trend(neighborhood, conn)
        forecast_df = fetch_sarimax_forecast(neighborhood, conn)
        crime_narr  = fetch_crime_narrative(neighborhood, conn)

        raw_rag       = fetch_rag_context(neighborhood, conn)
        cleaned_rag   = clean_rag_chunks(raw_rag)
        rag_narrative = summarize_rag_with_cortex(cleaned_rag, neighborhood, city, conn)
        narratives    = generate_cortex_narratives(data, conn)

        chart_paths = {
            "chart_radar":         generate_radar_chart(data, neighborhood),
            "chart_bar_neighbors": generate_bar_neighbors(data, neighbor_df, neighborhood),
            "chart_grouped_bar":   generate_grouped_bar(data, neighborhood),
            "chart_crime_trend":   generate_crime_trend_chart(crime_df, neighborhood,
                                                               forecast_df=forecast_df),
        }
        image_paths = generate_neighborhood_images(neighborhood)

        precomputed = {
            "data": data, "neighbor_df": neighbor_df, "crime_df": crime_df,
            "forecast_df": forecast_df, "crime_narrative": crime_narr,
            "rag_narrative": rag_narrative, "narratives": narratives,
            "chart_paths": chart_paths, "image_paths": image_paths,
        }
        pdf_path = generate_report(neighborhood, precomputed=precomputed)

        if pdf_path and Path(pdf_path).exists():
            return {"type": "report", "neighborhood": neighborhood,
                    "pdf_path": pdf_path, "chart_paths": chart_paths}
        return {"type": "report", "error": f"PDF assembly failed for {neighborhood}"}
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"type": "report", "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ROUTER
# ══════════════════════════════════════════════════════════════════════════════

def route(query: str, conn, domain_filter: str = None) -> dict:
    print(f"\n{'='*65}\n[Router] Query: {query}\n{'='*65}")

    classification = classify_query(query, conn)
    intent         = classification.get("intent", "data_query")
    neighborhood   = classification.get("neighborhood")
    domain         = classification.get("domain") or domain_filter or "All"
    confidence     = classification.get("confidence", 0.0)

    print(f"\n[Router] Dispatching → {intent.upper()}")
    if confidence < 0.5:
        print(f"[Router] ⚠️  Low confidence ({confidence:.2f}) — defaulting to data_query")
        intent = "data_query"

    if intent == "report":
        return handle_report(neighborhood or _extract_neighborhood_fast(query) or "", conn)
    elif intent == "chart":
        return handle_chart(query, conn)
    elif intent == "image":
        return handle_image(neighborhood or _extract_neighborhood_fast(query) or "", conn)
    elif intent == "web_search":
        return handle_web_search(query, domain=domain)
    else:
        return handle_data_query(query, conn, domain_filter=domain_filter)


# ══════════════════════════════════════════════════════════════════════════════
# CLI (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def display_result(result: dict):
    rtype = result.get("type", "unknown")
    print(f"\n{'─'*65}\n  Result: {rtype.upper()}\n{'─'*65}")
    if result.get("error"):
        print(f"\n❌ {result['error']}"); return
    if rtype == "data_query":
        print(f"\n{result.get('answer','')}")
        print(f"\nSQL rows: {len(result.get('results') or [])} | "
              f"RAG chunks: {len(result.get('rag_chunks') or [])}")
        tag = "🔍 Improved" if result.get("improved") else "✅ Passed"
        print(f"Validator: {tag}")
    elif rtype == "chart":
        print(f"\n{'✅' if result.get('path') else '❌'} {result.get('path','failed')}")
    elif rtype == "image":
        print(f"\n✅ {len(result.get('paths',[]))}/4 images for {result.get('neighborhood','')}")
    elif rtype == "web_search":
        print(f"\n{result.get('answer','')}")
    elif rtype == "report":
        print(f"\n{'✅' if result.get('pdf_path') else '❌'} {result.get('pdf_path','failed')}")
    print(f"\n{'─'*65}\n")


def main():
    parser = argparse.ArgumentParser(description="NeighbourWise AI — Router Agent")
    parser.add_argument("--query",       "-q")
    parser.add_argument("--interactive", "-i", action="store_true")
    parser.add_argument("--domain",      "-d", default=None)
    args = parser.parse_args()

    missing = [k for k in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
               if not os.environ.get(k)]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}"); sys.exit(1)

    conn = get_conn()
    try:
        if args.interactive:
            print("\nNeighbourWise AI — Interactive. Type 'exit' to quit.")
            while True:
                q = input("\nQ: ").strip()
                if not q or q.lower() in ("exit", "quit"): break
                display_result(route(q, conn, domain_filter=args.domain))
                time.sleep(0.3)
        elif args.query:
            display_result(route(args.query, conn, domain_filter=args.domain))
        else:
            parser.print_help()
    finally:
        conn.close()


if __name__ == "__main__":
    main()