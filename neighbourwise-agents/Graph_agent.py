"""
Graph_agent.py  (v4 — LangGraph parallel retrieval)
=====================================================
NeighbourWise AI — Graph Agent

What changed from v3:
  The three sequential I/O-bound retrieval steps:
    Neo4j  (~2-4s)  ─┐
    SF mart (~4-6s)  ─┤  now run IN PARALLEL via LangGraph Send API
    RAG    (~4-6s)  ─┘
  Synthesis + validation remain sequential (hard data dependencies).

  Expected latency improvement: ~8-12s off each graph_query call.
  (Was 41-61s sequential. Retrieval phase now takes max(Neo4j, mart, RAG)
   instead of sum, saving the two shorter tasks' wall-clock time.)

LangGraph graph structure:
  [plan] → Send("neo4j_node") ──┐
         → Send("mart_node")  ──┼→ [merge] → [synthesize] → [validate] → END
         → Send("rag_node")   ──┘

Usage (unchanged from v3):
    python Graph_agent.py -q "Is Allston safe and affordable?"
    python Graph_agent.py -i
    from Graph_agent import ask_graph_agent

Install:
    pip install langgraph langchain-core
"""

import os
import sys
import json
import time
import textwrap
import argparse
import logging
import operator
from pathlib import Path
from typing import Optional, Annotated

from dotenv import load_dotenv
from neo4j import GraphDatabase
import snowflake.connector
import anthropic

# ── LangGraph imports ─────────────────────────────────────────────────────────
from langgraph.graph import StateGraph, END
from langgraph.types import Send
from typing_extensions import TypedDict

from Graph_validator_agent import validate_and_regenerate


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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("graph_agent")


def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"\n  Missing required env variable: {key}\n"
            f"  Set it in your .env file at: {_env_path}\n"
        )
    return val


# ── Credentials ───────────────────────────────────────────────────────────────

NEO4J_URI      = _require("NEO4J_URI")
NEO4J_USER     = _require("NEO4J_USERNAME")
NEO4J_PASSWORD = _require("NEO4J_PASSWORD")

SF_ACCOUNT   = _require("SNOWFLAKE_ACCOUNT")
SF_USER      = _require("SNOWFLAKE_USER")
SF_PASSWORD  = _require("SNOWFLAKE_PASSWORD")
SF_WAREHOUSE = _require("SNOWFLAKE_WAREHOUSE")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "NEIGHBOURWISE_DOMAINS")
SF_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "")

ANTHROPIC_API_KEY = _require("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = "claude-sonnet-4-6"

RAG_DB        = "NEIGHBOURWISE_DOMAINS"
RAG_SCHEMA    = "RAW_UNSTRUCTURED"
RAG_TABLE     = "RAW_DOMAIN_CHUNKS"
RAG_TOP_K      = 5
MIN_RAG_SCORE  = 0.60   # chunks below this hybrid score are discarded as irrelevant
VECTOR_WEIGHT  = 0.65
KW_WEIGHT      = 0.35

NEO4J_DOMAINS = [
    "Safety", "Housing", "Grocery", "Healthcare",
    "MBTA", "Restaurants", "Schools", "Universities", "Bluebikes",
]

NEO4J_TO_RAG_DOMAIN = {
    "Safety": "CRIME", "Housing": "HOUSING", "Grocery": "GROCERY",
    "Healthcare": "HEALTHCARE", "MBTA": "TRANSIT", "Restaurants": "RESTAURANTS",
    "Schools": "SCHOOLS", "Universities": "UNIVERSITIES", "Bluebikes": "BLUEBIKES",
}

DOMAIN_KEYWORDS = {
    "Safety":       ["crime", "safe", "safety", "violence", "theft", "assault",
                     "police", "incident", "robbery", "shooting"],
    "Housing":      ["housing", "rent", "price", "afford", "sqft", "property",
                     "buy", "home", "apartment", "condo", "assessed", "value"],
    "Grocery":      ["grocery", "supermarket", "food store", "market",
                     "whole foods", "trader joe", "star market", "essential store"],
    "Healthcare":   ["hospital", "clinic", "doctor", "health", "medical",
                     "urgent care", "pharmacy", "healthcare", "facility"],
    "MBTA":         ["mbta", "transit", "bus", "subway", "train", "commute",
                     "green line", "red line", "orange line", "blue line",
                     "silver line", "stop", "station", "rapid transit"],
    "Restaurants":  ["restaurant", "dining", "eat", "food", "cafe",
                     "bar", "cuisine", "takeout", "delivery", "yelp"],
    "Schools":      ["school", "elementary", "middle school", "high school",
                     "public school", "charter", "k-12", "district"],
    "Universities": ["university", "college", "higher education", "campus",
                     "mit", "harvard", "northeastern", "bu ", "boston university",
                     "student", "degree", "research"],
    "Bluebikes":    ["bluebikes", "bike share", "bicycle", "bikeshare",
                     "cycling", "bike station", "bike dock"],
}

GREATER_BOSTON = [
    "BOSTON", "CAMBRIDGE", "SOMERVILLE", "QUINCY", "BROOKLINE",
    "ARLINGTON", "WATERTOWN", "CHELSEA", "EVERETT", "REVERE",
    "MEDFORD", "MALDEN", "NEWTON", "BELMONT", "LEXINGTON",
    "SALEM", "BEVERLY", "PEABODY", "MILTON",
    "ALLSTON", "BACK BAY", "BAY VILLAGE", "BEACON HILL",
    "BRIGHTON", "CHARLESTOWN", "CHINATOWN", "DORCHESTER",
    "DOWNTOWN", "EAST BOSTON", "FENWAY", "HYDE PARK",
    "JAMAICA PLAIN", "MATTAPAN", "MISSION HILL", "NORTH END",
    "ROSLINDALE", "ROXBURY", "SOUTH BOSTON", "SOUTH END",
    "WEST ROXBURY", "WEST END",
    "AREA 2/MIT", "CAMBRIDGEPORT", "EAST CAMBRIDGE",
    "MID CAMBRIDGE", "NORTH CAMBRIDGE", "WEST CAMBRIDGE",
    "WELLINGTON-HARRINGTON",
]


# ══════════════════════════════════════════════════════════════════════════════
# LANGGRAPH STATE
# ══════════════════════════════════════════════════════════════════════════════

class GraphAgentState(TypedDict):
    """
    Shared state object that flows through every node.

    Retrieval results use Annotated[list, operator.add] so that
    when multiple parallel nodes write to the same field, LangGraph
    MERGES their results instead of overwriting.

    Fields written by a single node use plain types (no annotation needed).
    """
    # ── Set by plan_node ──────────────────────────────────────────────────────
    query:        str
    neighborhood: Optional[str]
    domains:      list[str]

    # ── Accumulated by parallel retrieval nodes (merge semantics) ─────────────
    # Each parallel node appends its result as a single-element list.
    # LangGraph concatenates them automatically via operator.add.
    graph_ctx_parts:  Annotated[list[dict], operator.add]   # neo4j_node writes here
    struct_ctx_parts: Annotated[list[dict], operator.add]   # mart_node writes here
    rag_chunk_parts:  Annotated[list[list], operator.add]   # rag_node writes here

    # ── Set by merge_node (assembled from the three lists above) ──────────────
    graph_ctx:  dict
    struct_ctx: dict
    rag_chunks: list[dict]

    # ── Set by synthesize_node ────────────────────────────────────────────────
    draft: str

    # ── Set by validate_node ──────────────────────────────────────────────────
    answer:      str
    val_verdict: dict
    val_checks:  dict
    regenerated: bool
    val_passed:  Optional[bool]
    val_attempts: int


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS  (same logic as v3, unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def detect_domains(query: str) -> list[str]:
    q = query.lower()
    found = [d for d, kws in DOMAIN_KEYWORDS.items() if any(k in q for k in kws)]
    return found if found else NEO4J_DOMAINS[:]

def rag_domains_for(neo4j_domains: list[str]) -> list[str]:
    return [NEO4J_TO_RAG_DOMAIN[d] for d in neo4j_domains if d in NEO4J_TO_RAG_DOMAIN]

def extract_neighborhood(query: str, hint: Optional[str] = None) -> Optional[str]:
    if hint:
        return hint.strip().upper()
    q_upper = query.upper()
    for hood in GREATER_BOSTON:
        if hood in q_upper:
            return hood
    return None

def neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def sf_connect():
    return snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE, database=SF_DATABASE, role=SF_ROLE or None,
        network_timeout=120, login_timeout=60,
    )

def neo4j_neighborhood_profile(driver, neighborhood: str) -> dict:
    with driver.session() as session:
        scores = [dict(r) for r in session.run("""
            MATCH (n:Neighborhood {name: $name})-[r:HAS_SCORE]->(d:Domain)
            RETURN d.name AS domain, r.composite_score AS score, r.grade AS grade,
                   r.avg_price_per_sqft AS price_sqft, r.avg_living_area_sqft AS living_area,
                   r.total_incidents AS total_incidents, r.violent_crime_count AS violent_crime,
                   r.total_stops AS transit_stops, r.has_rapid_transit AS has_rapid_transit,
                   r.total_restaurants AS restaurants, r.avg_rating AS restaurant_rating,
                   r.total_schools AS schools, r.total_universities AS universities,
                   r.total_stations AS bike_stations
            ORDER BY r.composite_score DESC
        """, name=neighborhood)]
        borders = [r["neighbor"] for r in session.run("""
            MATCH (n:Neighborhood {name: $name})-[:BORDERS]->(b:Neighborhood)
            RETURN b.name AS neighbor
        """, name=neighborhood)]
        mbta = [r["line"] for r in session.run("""
            MATCH (n:Neighborhood {name: $name})-[:SERVED_BY]->(m:MBTALine)
            RETURN m.name AS line
        """, name=neighborhood)]
        similar = [dict(r) for r in session.run("""
            MATCH (n:Neighborhood {name: $name})-[s:SIMILAR_TO]-(b:Neighborhood)
            RETURN b.name AS neighbor, s.avg_score_delta AS delta,
                   s.shared_domains AS shared_domains, s.based_on AS based_on
            ORDER BY s.avg_score_delta ASC LIMIT 5
        """, name=neighborhood)]
    return {"neighborhood": neighborhood, "domain_scores": scores,
            "borders": borders, "mbta_lines": mbta, "similar_to": similar}

def neo4j_top_by_domain(driver, domain: str, limit: int = 5) -> list[dict]:
    with driver.session() as session:
        return [dict(r) for r in session.run("""
            MATCH (n:Neighborhood)-[r:HAS_SCORE]->(d:Domain {name: $domain})
            RETURN n.name AS neighborhood, r.composite_score AS score, r.grade AS grade
            ORDER BY r.composite_score DESC LIMIT $limit
        """, domain=domain, limit=limit)]

def neo4j_bottom_by_domain(driver, domain: str, limit: int = 5) -> list[dict]:
    """
    Bottom N neighborhoods by composite score for a domain.
    Used to give Claude real affordable/safe alternative data
    so it never needs to invent comparison scores.
    """
    with driver.session() as session:
        return [dict(r) for r in session.run("""
            MATCH (n:Neighborhood)-[r:HAS_SCORE]->(d:Domain {name: $domain})
            WHERE r.composite_score IS NOT NULL
            RETURN n.name AS neighborhood, r.composite_score AS score, r.grade AS grade
            ORDER BY r.composite_score DESC
        """, domain=domain)]


def neo4j_neighborhood_rank(driver, neighborhood: str, domain: str) -> dict:
    """
    Returns the queried neighborhood's exact rank among all neighborhoods
    for a given domain, plus real scores of the 3 neighborhoods above and below.

    Uses Python-side ranking (Cypher lacks row_number in older Neo4j versions).
    Returns a flat dict with explicit sentences Claude must use verbatim.
    """
    with driver.session() as session:
        rows = [dict(r) for r in session.run("""
            MATCH (n:Neighborhood)-[r:HAS_SCORE]->(d:Domain {name: $domain})
            WHERE r.composite_score IS NOT NULL
            RETURN n.name AS neighborhood, r.composite_score AS score, r.grade AS grade
            ORDER BY r.composite_score DESC
        """, domain=domain)]

    total = len(rows)
    idx   = next((i for i, r in enumerate(rows) if r["neighborhood"] == neighborhood), None)

    if idx is None:
        return {
            "rank":     None,
            "total":    total,
            "summary":  f"Rank not found for {neighborhood} in {domain}",
            "above":    [],
            "below":    [],
        }

    rank  = idx + 1
    above = rows[max(0, idx - 3):idx]        # up to 3 real neighborhoods scoring higher
    below = rows[idx + 1:min(total, idx + 4)] # up to 3 real neighborhoods scoring lower

    return {
        "rank":    rank,
        "total":   total,
        # Pre-formatted sentence Claude must cite directly — no invention possible
        "summary": (
            f"{neighborhood} ranks {rank} out of {total} neighborhoods "
            f"in Greater Boston for {domain} (score {rows[idx]['score']:.1f}, "
            f"grade {rows[idx]['grade']})"
        ),
        "above":   above,   # real neighborhoods with higher scores
        "below":   below,   # real neighborhoods with lower scores
    }


def neo4j_transit_connected(driver, neighborhood: str) -> list[dict]:
    with driver.session() as session:
        return [dict(r) for r in session.run("""
            MATCH (n:Neighborhood {name: $name})-[:SERVED_BY]->(m:MBTALine)
                  <-[:SERVED_BY]-(b:Neighborhood)
            WHERE b.name <> $name
            RETURN b.name AS neighbor, m.name AS shared_line
            ORDER BY m.name, b.name
        """, name=neighborhood)]

def sf_housing_detail(cur, neighborhood: str) -> Optional[dict]:
    cur.execute("""
        SELECT NEIGHBORHOOD_NAME, CITY, HOUSING_SCORE, HOUSING_GRADE,
               AVG_PRICE_PER_SQFT, AVG_LIVING_AREA_SQFT, TOTAL_PROPERTIES,
               AVG_ASSESSED_VALUE, AVG_ESTIMATED_RENT, PASS1_SCORE
        FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING
        WHERE UPPER(NEIGHBORHOOD_NAME) = UPPER(%s) LIMIT 1
    """, (neighborhood,))
    cols = [d[0].lower() for d in cur.description]
    row  = cur.fetchone()
    return dict(zip(cols, row)) if row else None

def sf_safety_detail(cur, neighborhood: str) -> Optional[dict]:
    cur.execute("""
        SELECT NEIGHBORHOOD_NAME, CITY, SAFETY_SCORE, SAFETY_GRADE,
               TOTAL_INCIDENTS, VIOLENT_CRIME_COUNT, PROPERTY_CRIME_COUNT,
               INCIDENTS_PER_SQMILE, YOY_CHANGE_PCT
        FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
        WHERE UPPER(NEIGHBORHOOD_NAME) = UPPER(%s) LIMIT 1
    """, (neighborhood,))
    cols = [d[0].lower() for d in cur.description]
    row  = cur.fetchone()
    return dict(zip(cols, row)) if row else None

def sf_rag_search(cur, query: str, neo4j_domains: list[str],
                  top_k: int = RAG_TOP_K) -> list[dict]:
    rag_tags = rag_domains_for(neo4j_domains) or list(NEO4J_TO_RAG_DOMAIN.values())
    domain_filter = ", ".join(f"'{t}'" for t in rag_tags)
    cur.execute(f"""
        WITH vector_scores AS (
            SELECT chunk_id, source_file, domain, chunk_text,
                VECTOR_COSINE_SIMILARITY(chunk_embedding,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'query: ' || %s)
                ) AS vec_score
            FROM {RAG_DB}.{RAG_SCHEMA}.{RAG_TABLE}
            WHERE UPPER(domain) IN ({domain_filter})
        ),
        keyword_scores AS (
            SELECT chunk_id,
                CASE WHEN LOWER(chunk_text) LIKE %s THEN 1.0 ELSE 0.0 END AS kw_score
            FROM {RAG_DB}.{RAG_SCHEMA}.{RAG_TABLE}
        ),
        combined AS (
            SELECT v.chunk_id, v.source_file, v.domain, v.chunk_text,
                ({VECTOR_WEIGHT} * v.vec_score + {KW_WEIGHT} * k.kw_score) AS hybrid_score
            FROM vector_scores v JOIN keyword_scores k ON v.chunk_id = k.chunk_id
        )
        SELECT chunk_id, source_file, domain, chunk_text, hybrid_score
        FROM combined ORDER BY hybrid_score DESC LIMIT %s
    """, (query, f"%{query.lower()[:30]}%", top_k))
    cols = [d[0].lower() for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def _normalise_rag_chunks(raw: list[dict]) -> list[dict]:
    return [
        {
            "DOMAIN": c.get("domain", "?").upper(), "CHUNK_TEXT": c.get("chunk_text", ""),
            "SOURCE_FILE": c.get("source_file", ""), "similarity": float(c.get("hybrid_score", 0)),
            "domain": c.get("domain", "?"), "chunk_text": c.get("chunk_text", ""),
            "source_file": c.get("source_file", ""), "hybrid_score": float(c.get("hybrid_score", 0)),
        }
        for c in raw
    ]

def _fmt_check(issues: list, fatal: bool = False, warn: bool = False) -> dict:
    if not issues:
        return {"status": "✅ PASS", "issues": []}
    if fatal:
        return {"status": "❌ FAIL", "issues": issues}
    if warn:
        return {"status": "⚠️  WARN", "issues": issues}
    return {"status": "❌ FAIL", "issues": issues}

SYSTEM_PROMPT = """You are the NeighbourWise AI graph agent for Greater Boston neighborhood
livability analysis.

══════════════════════════════════════════════════════
ANTI-HALLUCINATION RULES — READ BEFORE WRITING ANYTHING
══════════════════════════════════════════════════════
1. PEER COMPARISONS: The user message contains "=== VERIFIED RANKING DATA ===".
   This is the ONLY source you may use for citing other neighborhoods' scores.
   Every neighborhood name + score + grade you mention MUST appear verbatim there.

2. NO TRAINING DATA: Do not use your training knowledge for Boston neighborhood
   scores. Those figures may be outdated or estimated. The VERIFIED RANKING DATA
   is the sole ground truth for comparisons.

3. IF A SCORE IS NOT IN VERIFIED RANKING DATA: Do not cite it. Write instead:
   "Peer comparison data not available" or omit the comparison entirely.

4. SCOPE: Answer only the domains the query explicitly asks about.
   "Is Allston safe and affordable?" = Safety + Housing only.
   Do NOT add MBTA, Restaurants, Grocery, etc. unless the query requests them.
══════════════════════════════════════════════════════

Response format:
  - Lead with a direct answer to the user's question
  - State each queried domain's score, grade, and rank from VERIFIED RANKING DATA
  - For peer comparisons: name + score + grade, sourced only from VERIFIED RANKING DATA
  - Note INSUFFICIENT DATA if a domain has score 0
  - Keep response between 300-500 words
  - End with: "Sources: [graph] [structured mart] [RAG chunks]"
Never fabricate scores or relationships not present in the provided context."""


# ══════════════════════════════════════════════════════════════════════════════
# LANGGRAPH NODES
# Each function receives the full state dict and returns a partial update.
# LangGraph merges the returned dict into state automatically.
# ══════════════════════════════════════════════════════════════════════════════

def plan_node(state: GraphAgentState) -> dict:
    """
    Step 1: detect domains and neighborhood from the query.
    Runs once, instantly (pure Python, no I/O).
    Returns the base fields that all downstream nodes need.
    """
    query        = state["query"]
    neighborhood = state.get("neighborhood")  # may be a hint passed by caller

    domains      = detect_domains(query)
    neighborhood = extract_neighborhood(query, neighborhood)

    log.info(f"[plan] domains={domains}  neighborhood={neighborhood}")

    return {
        "domains":          domains,
        "neighborhood":     neighborhood,
        # Initialise accumulator lists so parallel nodes can append safely
        "graph_ctx_parts":  [],
        "struct_ctx_parts": [],
        "rag_chunk_parts":  [],
    }


def neo4j_node(state: GraphAgentState) -> dict:
    """
    Parallel retrieval — Neo4j graph traversal.
    Runs concurrently with mart_node and rag_node via Send API.
    Appends its result to graph_ctx_parts (merged by operator.add).
    """
    t0           = time.time()
    neighborhood = state.get("neighborhood")
    domains      = state.get("domains", [])
    ctx          = {}

    try:
        driver = neo4j_driver()
        if neighborhood:
            ctx["profile"]         = neo4j_neighborhood_profile(driver, neighborhood)
            ctx["transit_network"] = neo4j_transit_connected(driver, neighborhood)
        # Top 5 per domain — real peer scores Claude can cite
        ctx["top_by_domain"] = {
            d: neo4j_top_by_domain(driver, d, limit=5)
            for d in domains           # all detected domains, not just [:3]
        }

        # Exact rank + immediate peers for each detected domain
        # This is the key fix: Claude gets "Allston ranks 31st/51 on Safety"
        # so it never needs to invent comparison scores
        if neighborhood:
            ctx["neighborhood_ranks"] = {
                d: neo4j_neighborhood_rank(driver, neighborhood, d)
                for d in domains
            }

        driver.close()
        log.info(f"[neo4j_node] complete ({time.time()-t0:.1f}s)")
    except Exception as e:
        log.warning(f"[neo4j_node] failed: {e}")
        ctx = {"error": str(e)}

    # Wrap in list — operator.add will concatenate with other nodes' lists
    return {"graph_ctx_parts": [ctx]}


def mart_node(state: GraphAgentState) -> dict:
    """
    Parallel retrieval — Snowflake structured mart queries.
    Runs concurrently with neo4j_node and rag_node.
    """
    t0           = time.time()
    neighborhood = state.get("neighborhood")
    domains      = state.get("domains", [])
    ctx          = {}

    try:
        conn = sf_connect()
        cur  = conn.cursor()
        if neighborhood:
            if "Housing" in domains:
                h = sf_housing_detail(cur, neighborhood)
                if h:
                    ctx["housing"] = h
            if "Safety" in domains:
                s = sf_safety_detail(cur, neighborhood)
                if s:
                    ctx["safety"] = s
        cur.close()
        conn.close()
        log.info(f"[mart_node] complete ({time.time()-t0:.1f}s) — {list(ctx.keys())}")
    except Exception as e:
        log.warning(f"[mart_node] failed: {e}")

    return {"struct_ctx_parts": [ctx]}


def rag_node(state: GraphAgentState) -> dict:
    """
    Parallel retrieval — Snowflake RAG hybrid search.
    Runs concurrently with neo4j_node and mart_node.
    """
    t0      = time.time()
    query   = state["query"]
    domains = state.get("domains", [])
    chunks  = []

    try:
        conn = sf_connect()
        cur  = conn.cursor()
        raw_chunks = _normalise_rag_chunks(sf_rag_search(cur, query, domains))
        cur.close()
        conn.close()
        # Discard chunks below the minimum relevance threshold
        chunks   = [c for c in raw_chunks if c.get("hybrid_score", 0) >= MIN_RAG_SCORE]
        skipped  = len(raw_chunks) - len(chunks)
        log.info(
            f"[rag_node] complete ({time.time()-t0:.1f}s) — "
            f"{len(chunks)} relevant chunks kept, {skipped} below {MIN_RAG_SCORE} threshold discarded"
        )
    except Exception as e:
        log.warning(f"[rag_node] failed: {e}")

    # Wrap chunk list in an outer list so operator.add merges correctly
    return {"rag_chunk_parts": [chunks]}


def merge_node(state: GraphAgentState) -> dict:
    """
    Step 3 (fan-in): assembles the three parallel results into clean dicts.
    LangGraph routes here automatically after ALL three parallel nodes finish.
    """
    # graph_ctx_parts is a list of dicts from neo4j_node — take the first (only one)
    graph_ctx  = state["graph_ctx_parts"][0]  if state.get("graph_ctx_parts")  else {}

    # struct_ctx_parts same — one dict from mart_node
    struct_ctx = state["struct_ctx_parts"][0] if state.get("struct_ctx_parts") else {}

    # rag_chunk_parts is a list of lists — flatten one level
    rag_chunks = state["rag_chunk_parts"][0]  if state.get("rag_chunk_parts")  else []

    log.info(
        f"[merge_node] graph_ctx keys={list(graph_ctx.keys())} | "
        f"struct_ctx keys={list(struct_ctx.keys())} | "
        f"rag_chunks={len(rag_chunks)}"
    )
    return {
        "graph_ctx":  graph_ctx,
        "struct_ctx": struct_ctx,
        "rag_chunks": rag_chunks,
    }


def synthesize_node(state: GraphAgentState) -> dict:
    """
    Step 4: Claude synthesis — waits for merge_node to complete.
    Hard dependency on all three retrieval results.
    """
    t0         = time.time()
    query      = state["query"]
    graph_ctx  = state.get("graph_ctx", {})
    struct_ctx = state.get("struct_ctx", {})
    rag_chunks = state.get("rag_chunks", [])

    parts = []

    # ── Pre-formatted ranking sentences (injected first so Claude reads them
    #    as explicit facts before seeing the raw JSON context) ─────────────────
    rank_sentences = []
    for domain, rank_data in (graph_ctx.get("neighborhood_ranks") or {}).items():
        summary = rank_data.get("summary")
        if summary:
            rank_sentences.append(f"  RANK: {summary}")
        above = rank_data.get("above", [])
        if above:
            peers = ", ".join(
                f"{r['neighborhood'].title()} (score {r['score']:.1f}, {r['grade']})"
                for r in above
            )
            rank_sentences.append(f"  NEIGHBORHOODS SCORING HIGHER on {domain}: {peers}")
        below = rank_data.get("below", [])
        if below:
            peers = ", ".join(
                f"{r['neighborhood'].title()} (score {r['score']:.1f}, {r['grade']})"
                for r in below
            )
            rank_sentences.append(f"  NEIGHBORHOODS SCORING LOWER on {domain}: {peers}")

    if rank_sentences:
        parts.append(
            "=== VERIFIED RANKING DATA (use these exact facts — do not compute or invent) ===\n"
            + "\n".join(rank_sentences)
        )

    if graph_ctx:
        # Strip top_by_domain and neighborhood_ranks — already pre-formatted
        # as plain sentences in VERIFIED RANKING DATA above. Keeping them as
        # raw JSON lists causes Claude to invent scores for nearby neighborhoods.
        graph_stripped = {
            k: v for k, v in graph_ctx.items()
            if k not in ("top_by_domain", "neighborhood_ranks")
        }
        parts.append("\n=== GRAPH PROFILE (Neo4j) ===")
        parts.append(json.dumps(graph_stripped, indent=2, default=str))
    if struct_ctx:
        parts.append("\n=== STRUCTURED CONTEXT (Snowflake Marts) ===")
        parts.append(json.dumps(struct_ctx, indent=2, default=str))
    if rag_chunks:
        parts.append("\n=== UNSTRUCTURED CONTEXT (RAG Chunks) ===")
        for i, c in enumerate(rag_chunks, 1):
            parts.append(
                f"[{i}] Domain: {c.get('domain','?')} | "
                f"Source: {c.get('source_file','?')} | "
                f"Score: {c.get('hybrid_score', 0):.3f}\n"
                f"{c.get('chunk_text','')[:600]}"
            )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resp   = client.messages.create(
        model=CLAUDE_MODEL, max_tokens=700, system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": f"User query: {query}\n\n" + "\n".join(parts)}],
    )
    draft = resp.content[0].text
    log.info(f"[synthesize_node] complete ({time.time()-t0:.1f}s)  {len(draft)} chars")
    return {"draft": draft}


def validate_node(state: GraphAgentState) -> dict:
    """
    Step 5: GPT-4o validation + Claude regeneration if needed.
    Hard dependency on draft from synthesize_node.
    """
    t0         = time.time()
    query      = state["query"]
    draft      = state["draft"]
    graph_ctx  = state.get("graph_ctx", {})
    struct_ctx = state.get("struct_ctx", {})
    rag_chunks = state.get("rag_chunks", [])

    val_verdict  = {}
    val_checks   = {}
    regenerated  = False
    val_passed   = None
    val_attempts = 1
    answer       = draft

    try:
        validation = validate_and_regenerate(
            query=query, draft=draft,
            graph_ctx=graph_ctx, struct_ctx=struct_ctx, rag_chunks=rag_chunks,
            verbose=True,
        )
        answer       = validation["final_output"]
        val_verdict  = validation["final_verdict"]
        regenerated  = validation["regenerated"]
        val_passed   = validation["passed"]
        val_attempts = validation["attempts"]

        raw_issues = val_verdict.get("issues", {})
        val_checks = {
            "score_errors":      _fmt_check(raw_issues.get("score_errors", [])),
            "grade_errors":      _fmt_check(raw_issues.get("grade_errors", [])),
            "fabricated_data":   _fmt_check(raw_issues.get("fabricated_data", []), fatal=True),
            "missing_insights":  _fmt_check(raw_issues.get("missing_insights", []), warn=True),
            "comparison_errors": _fmt_check(raw_issues.get("comparison_errors", [])),
            "richness_issues":   _fmt_check(raw_issues.get("richness_issues", []), warn=True),
        }
        log.info(
            f"[validate_node] complete ({time.time()-t0:.1f}s) — "
            f"passed={val_passed} score={val_verdict.get('score')}/100 "
            f"regenerated={regenerated} attempts={val_attempts}"
        )
    except Exception as e:
        log.warning(f"[validate_node] failed (non-fatal): {e}")

    return {
        "answer": answer, "val_verdict": val_verdict, "val_checks": val_checks,
        "regenerated": regenerated, "val_passed": val_passed, "val_attempts": val_attempts,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ROUTING FUNCTION — triggers parallel fan-out from plan_node
# ══════════════════════════════════════════════════════════════════════════════

def dispatch_retrieval(state: GraphAgentState) -> list[Send]:
    """
    Called after plan_node. Returns three Send objects — one per retrieval node.
    LangGraph executes all three concurrently then waits for all to finish
    before routing to merge_node.

    The Send API passes the CURRENT state to each target node, so each
    parallel node has access to query, domains, and neighborhood.
    """
    return [
        Send("neo4j_node", state),
        Send("mart_node",  state),
        Send("rag_node",   state),
    ]


# ══════════════════════════════════════════════════════════════════════════════
# BUILD THE LANGGRAPH
# ══════════════════════════════════════════════════════════════════════════════

def _build_graph() -> StateGraph:
    """
    Graph topology:
      plan_node
         │ (conditional edge → dispatch_retrieval → 3x Send)
         ├── neo4j_node ─┐
         ├── mart_node  ─┤ (all three run in parallel)
         └── rag_node   ─┘
                          │ (all join at merge_node automatically)
                       merge_node
                          │
                    synthesize_node
                          │
                     validate_node
                          │
                         END
    """
    builder = StateGraph(GraphAgentState)

    # Register nodes
    builder.add_node("plan_node",       plan_node)
    builder.add_node("neo4j_node",      neo4j_node)
    builder.add_node("mart_node",       mart_node)
    builder.add_node("rag_node",        rag_node)
    builder.add_node("merge_node",      merge_node)
    builder.add_node("synthesize_node", synthesize_node)
    builder.add_node("validate_node",   validate_node)

    # Entry point
    builder.set_entry_point("plan_node")

    # plan_node → fan-out to three parallel nodes via Send API
    builder.add_conditional_edges(
        "plan_node",
        dispatch_retrieval,
        # Declare all possible target nodes from this conditional edge
        ["neo4j_node", "mart_node", "rag_node"],
    )

    # Each parallel node → merge_node (LangGraph waits for ALL to finish)
    builder.add_edge("neo4j_node", "merge_node")
    builder.add_edge("mart_node",  "merge_node")
    builder.add_edge("rag_node",   "merge_node")

    # Sequential chain after merge
    builder.add_edge("merge_node",      "synthesize_node")
    builder.add_edge("synthesize_node", "validate_node")
    builder.add_edge("validate_node",   END)

    return builder.compile()


# Build once at module load — reused for every ask_graph_agent() call
_GRAPH = _build_graph()


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API — same signature as v3, fully backward compatible
# ══════════════════════════════════════════════════════════════════════════════

def ask_graph_agent(query: str, neighborhood: str = None) -> dict:
    """
    Full graph agent pipeline using LangGraph parallel retrieval.

    Drop-in replacement for v3 ask_graph_agent().
    Same return dict shape — router_agent.py needs no changes.

    Retrieval improvement vs v3:
      v3: Neo4j (~3s) + mart (~5s) + RAG (~5s) = ~13s sequential
      v4: max(Neo4j, mart, RAG) = ~5s parallel   → saves ~8s per call
    """
    t0 = time.time()
    log.info(f"[ask_graph_agent] query={query!r}  neighborhood={neighborhood!r}")

    # Run the LangGraph
    initial_state: GraphAgentState = {
        "query":            query,
        "neighborhood":     neighborhood,
        "domains":          [],
        "graph_ctx_parts":  [],
        "struct_ctx_parts": [],
        "rag_chunk_parts":  [],
        "graph_ctx":        {},
        "struct_ctx":       {},
        "rag_chunks":       [],
        "draft":            "",
        "answer":           "",
        "val_verdict":      {},
        "val_checks":       {},
        "regenerated":      False,
        "val_passed":       None,
        "val_attempts":     1,
    }

    final_state = _GRAPH.invoke(initial_state)

    # Unpack final state into the standard return dict
    graph_ctx  = final_state.get("graph_ctx",  {})
    struct_ctx = final_state.get("struct_ctx", {})
    rag_chunks = final_state.get("rag_chunks", [])
    val_verdict = final_state.get("val_verdict", {})
    val_checks  = final_state.get("val_checks",  {})
    regenerated = final_state.get("regenerated", False)
    val_passed  = final_state.get("val_passed",  None)
    val_attempts= final_state.get("val_attempts", 1)
    answer      = final_state.get("answer", "")

    all_issues = [i for issues in (val_verdict.get("issues") or {}).values()
                  for i in (issues or [])]
    any_fatal  = bool((val_verdict.get("issues") or {}).get("fabricated_data"))

    log.info(f"[ask_graph_agent] total={time.time()-t0:.1f}s")

    return {
        "type":       "graph_query",
        "answer":     answer,
        "sql":        None,
        "results":    [],
        "rag_chunks": rag_chunks,
        "improved":   regenerated,
        "validation": {
            "checks":            val_checks,
            "needs_improvement": any_fatal or (not val_passed),
            "total_issues":      len(all_issues),
            "all_issues":        all_issues,
            "passed":            val_passed,
            "score":             val_verdict.get("score"),
            "regenerated":       regenerated,
            "attempts":          val_attempts,
        },
        "neighborhood": final_state.get("neighborhood"),
        "domains":      final_state.get("domains", []),
        "graph_data": {
            "profile":         graph_ctx.get("profile"),
            "top_by_domain":   graph_ctx.get("top_by_domain", {}),
            "transit_network": graph_ctx.get("transit_network", []),
        },
        "sources": {
            "graph_nodes":     bool(graph_ctx and "error" not in graph_ctx),
            "structured_mart": bool(struct_ctx),
            "rag_chunks":      len(rag_chunks),
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# TERMINAL DISPLAY  (unchanged from v3)
# ══════════════════════════════════════════════════════════════════════════════

def display_result(result: dict):
    SEP  = "─" * 65
    SEP2 = "═" * 65

    if result.get("error"):
        print(f"\n❌  {result['error']}\n")
        return

    print(f"\n{SEP2}")
    print(f"  GRAPH AGENT  —  {result.get('neighborhood') or 'No neighborhood detected'}")
    print(f"{SEP2}\n")

    for line in result.get("answer", "").splitlines():
        print(textwrap.fill(line, width=78) if len(line) > 78 else line)
    print()

    s = result.get("sources", {})
    print(f"{SEP}")
    print(
        f"  Sources  →  "
        f"Graph: {'✓' if s.get('graph_nodes') else '✗'}  |  "
        f"Mart: {'✓' if s.get('structured_mart') else '✗'}  |  "
        f"RAG chunks: {s.get('rag_chunks', 0)}"
    )
    print(f"  Domains  →  {', '.join(result.get('domains', []))}")

    val = result.get("validation", {})
    if val:
        passed_str = "✅ Passed" if val.get("passed") else "🔍 Regenerated"
        regen_str  = f"  (attempt {val.get('attempts', 1)})" if val.get("regenerated") else ""
        print(f"{SEP}")
        print(f"  Validator  →  {passed_str}{regen_str}  |  Score: {val.get('score')}/100")
        for name, c in (val.get("checks") or {}).items():
            if c.get("issues"):
                print(f"     {c.get('status','')}  {name}")
                for issue in c["issues"]:
                    print(f"          → {issue[:110]}")

    rag = result.get("rag_chunks", [])
    if rag:
        print(f"{SEP}")
        print("  RAG sources:")
        for c in rag[:3]:
            domain = c.get("DOMAIN", c.get("domain", "?"))
            src    = Path(c.get("SOURCE_FILE", c.get("source_file", "?"))).name
            sim    = c.get("similarity", c.get("hybrid_score", 0))
            print(f"     [{domain}]  {src}  (score {sim:.3f})")

    print(f"{SEP2}\n")


# ══════════════════════════════════════════════════════════════════════════════
# CLI  (unchanged from v3)
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="NeighbourWise Graph Agent (LangGraph) — terminal interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            Examples:
              python Graph_agent.py -q "Is Allston safe and affordable?"
              python Graph_agent.py -q "Best transit access" -n CAMBRIDGE
              python Graph_agent.py -i
        """)
    )
    p.add_argument("-q", "--query",       help="Single query string")
    p.add_argument("-n", "--neighborhood", help="Neighborhood hint (e.g. ALLSTON)")
    p.add_argument("-i", "--interactive",  action="store_true")
    p.add_argument("--json",              action="store_true",
                   help="Print raw JSON instead of formatted output")
    return p, p.parse_args()


def run_query(query: str, neighborhood: str = None, as_json: bool = False):
    print(f"\n[Graph Agent] Query: {query!r}")
    if neighborhood:
        print(f"[Graph Agent] Neighborhood hint: {neighborhood}")
    print()
    try:
        result = ask_graph_agent(query, neighborhood=neighborhood)
    except Exception as e:
        print(f"\n❌  Pipeline error: {e}\n")
        return
    if as_json:
        print(json.dumps(result, indent=2, default=str))
    else:
        display_result(result)


def main():
    print(f"  [env] Loaded .env from: {_env_path}")
    parser, args = parse_args()

    if not sys.stdin.isatty() and not args.query and not args.interactive:
        query = sys.stdin.read().strip()
        if query:
            run_query(query, neighborhood=args.neighborhood, as_json=args.json)
        return

    if args.query:
        run_query(args.query, neighborhood=args.neighborhood, as_json=args.json)
        return

    if args.interactive:
        print("\n" + "═" * 65)
        print("  NeighbourWise Graph Agent (LangGraph) — Interactive")
        print("  Type a question, or 'exit' to quit.")
        print("═" * 65)
        while True:
            try:
                query = input("\nQ: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye.")
                break
            if not query:
                continue
            if query.lower() in ("exit", "quit", "q"):
                print("Goodbye.")
                break
            run_query(query, neighborhood=args.neighborhood, as_json=args.json)
        return

    parser.print_help()


if __name__ == "__main__":
    main()