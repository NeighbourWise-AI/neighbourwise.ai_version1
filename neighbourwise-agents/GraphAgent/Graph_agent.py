"""
graph_agent.py  (v2 — aligned to real Snowflake + Neo4j schema)
===============================================================
NeighbourWise AI — Graph Agent (Part 2)
Query router that combines Neo4j graph traversal, Snowflake structured mart
queries, and Snowflake RAG hybrid search, then synthesises with Claude.

Fixes applied vs original:
  - NEO4J_USERNAME env key (AuraDB convention)
  - _find_env_file() walker + _require() helper (matches loader)
  - Domain keyword map aligned to real Neo4j Domain node names:
      Safety, Housing, Grocery, Healthcare, MBTA, Restaurants,
      Schools, Universities, Bluebikes  (Weather / Crime / Transit removed)
  - RAG domain name mapping (Neo4j "Safety" → RAG "CRIME" etc.)
  - s.avg_score_delta (was s.score_delta — returned None)
  - sf_housing_detail uses real column + table names
  - Claude model updated to claude-sonnet-4-6

Endpoints:
    POST /graph-agent/query
         body: { "query": "...", "neighborhood": "ALLSTON" (optional) }
    GET  /graph-agent/health

Usage:
    python graph_agent.py              # port 5001
    python graph_agent.py --port 5002
    python graph_agent.py --debug
"""

import os
import json
import logging
import argparse
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS
from neo4j import GraphDatabase
import snowflake.connector
import anthropic

# ── Env ───────────────────────────────────────────────────────────────────────
# Same _find_env_file walker as neo4j_schema_loader.py

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
log = logging.getLogger("graph_agent")


def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"\n\n  Missing required env variable: {key}\n"
            f"  Make sure it is set in your .env file at: {_env_path}\n"
        )
    return val


# ── Credentials ───────────────────────────────────────────────────────────────

NEO4J_URI      = _require("NEO4J_URI")
NEO4J_USER     = _require("NEO4J_USERNAME")    # AuraDB exports as NEO4J_USERNAME
NEO4J_PASSWORD = _require("NEO4J_PASSWORD")

SF_ACCOUNT   = _require("SNOWFLAKE_ACCOUNT")
SF_USER      = _require("SNOWFLAKE_USER")
SF_PASSWORD  = _require("SNOWFLAKE_PASSWORD")
SF_WAREHOUSE = _require("SNOWFLAKE_WAREHOUSE")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "NEIGHBOURWISE_DOMAINS")
SF_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "")

ANTHROPIC_API_KEY = _require("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = "claude-sonnet-4-6"        # current model

# ── RAG settings ──────────────────────────────────────────────────────────────

RAG_DB        = "NEIGHBOURWISE_DOMAINS"
RAG_SCHEMA    = "RAW_UNSTRUCTURED"
RAG_TABLE     = "RAW_DOMAIN_CHUNKS"
RAG_TOP_K     = 5
VECTOR_WEIGHT = 0.65
KW_WEIGHT     = 0.35

# ── Domain name mappings ──────────────────────────────────────────────────────
# Neo4j Domain node names  (what's stored in the graph)
NEO4J_DOMAINS = [
    "Safety", "Housing", "Grocery", "Healthcare",
    "MBTA", "Restaurants", "Schools", "Universities", "Bluebikes",
]

# RAG chunk DOMAIN column values  (what's stored in RAW_DOMAIN_CHUNKS)
# Maps Neo4j domain name → RAG domain tag used in the chunks table
NEO4J_TO_RAG_DOMAIN = {
    "Safety":       "CRIME",
    "Housing":      "HOUSING",
    "Grocery":      "GROCERY",
    "Healthcare":   "HEALTHCARE",
    "MBTA":         "TRANSIT",
    "Restaurants":  "RESTAURANTS",
    "Schools":      "SCHOOLS",
    "Universities": "UNIVERSITIES",
    "Bluebikes":    "BLUEBIKES",
}

# ── Query planner ─────────────────────────────────────────────────────────────
# Keys MUST match Neo4j Domain node names exactly

DOMAIN_KEYWORDS = {
    "Safety": [
        "crime", "safe", "safety", "violence", "theft", "assault",
        "police", "incident", "robbery", "shooting",
    ],
    "Housing": [
        "housing", "rent", "price", "afford", "sqft", "property",
        "buy", "home", "apartment", "condo", "assessed", "value",
    ],
    "Grocery": [
        "grocery", "supermarket", "food store", "market",
        "whole foods", "trader joe", "star market", "essential store",
    ],
    "Healthcare": [
        "hospital", "clinic", "doctor", "health", "medical",
        "urgent care", "pharmacy", "healthcare", "facility",
    ],
    "MBTA": [
        "mbta", "transit", "bus", "subway", "train", "commute",
        "green line", "red line", "orange line", "blue line",
        "silver line", "stop", "station", "rapid transit",
    ],
    "Restaurants": [
        "restaurant", "dining", "eat", "food", "cafe",
        "bar", "cuisine", "takeout", "delivery", "yelp",
    ],
    "Schools": [
        "school", "elementary", "middle school", "high school",
        "public school", "charter", "k-12", "district",
    ],
    "Universities": [
        "university", "college", "higher education", "campus",
        "mit", "harvard", "northeastern", "bu ", "boston university",
        "student", "degree", "research",
    ],
    "Bluebikes": [
        "bluebikes", "bike share", "bicycle", "bikeshare",
        "cycling", "bike station", "bike dock",
    ],
}


def detect_domains(query: str) -> list[str]:
    """Return Neo4j domain names relevant to the query. Defaults to all."""
    q = query.lower()
    found = [d for d, kws in DOMAIN_KEYWORDS.items() if any(k in q for k in kws)]
    return found if found else NEO4J_DOMAINS[:]


def rag_domains_for(neo4j_domains: list[str]) -> list[str]:
    """Convert Neo4j domain names to RAG chunk domain tags."""
    return [NEO4J_TO_RAG_DOMAIN[d] for d in neo4j_domains if d in NEO4J_TO_RAG_DOMAIN]


def extract_neighborhood(query: str, hint: Optional[str] = None) -> Optional[str]:
    """
    Return neighborhood name in UPPER CASE to match Neo4j node names.
    Uses hint if provided, otherwise scans query text.
    """
    if hint:
        return hint.strip().upper()

    # All 51 neighborhoods stored in Neo4j (uppercase to match node names)
    greater_boston = [
        # Cities
        "BOSTON", "CAMBRIDGE", "SOMERVILLE", "QUINCY", "BROOKLINE",
        "ARLINGTON", "WATERTOWN", "CHELSEA", "EVERETT", "REVERE",
        "MEDFORD", "MALDEN", "NEWTON", "BELMONT", "LEXINGTON",
        "SALEM", "BEVERLY", "PEABODY", "MILTON",
        # Boston neighborhoods
        "ALLSTON", "BACK BAY", "BAY VILLAGE", "BEACON HILL",
        "BRIGHTON", "CHARLESTOWN", "CHINATOWN", "DORCHESTER",
        "DOWNTOWN", "EAST BOSTON", "FENWAY", "HYDE PARK",
        "JAMAICA PLAIN", "MATTAPAN", "MISSION HILL", "NORTH END",
        "ROSLINDALE", "ROXBURY", "SOUTH BOSTON", "SOUTH END",
        "WEST ROXBURY", "WEST END",
        # Cambridge neighborhoods
        "AREA 2/MIT", "CAMBRIDGEPORT", "EAST CAMBRIDGE",
        "MID CAMBRIDGE", "NORTH CAMBRIDGE", "WEST CAMBRIDGE",
        "WELLINGTON-HARRINGTON",
    ]
    q_upper = query.upper()
    for hood in greater_boston:
        if hood in q_upper:
            return hood
    return None


# ── Connection factories ──────────────────────────────────────────────────────

def neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def sf_connect():
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        role=SF_ROLE or None,
        network_timeout=120,
        login_timeout=60,
    )


# ── Neo4j queries ─────────────────────────────────────────────────────────────

def neo4j_neighborhood_profile(driver, neighborhood: str) -> dict:
    """Full profile: domain scores, borders, MBTA lines, similar neighborhoods."""
    with driver.session() as session:

        scores = [dict(r) for r in session.run("""
            MATCH (n:Neighborhood {name: $name})-[r:HAS_SCORE]->(d:Domain)
            RETURN d.name                AS domain,
                   r.composite_score     AS score,
                   r.grade               AS grade,
                   r.avg_price_per_sqft  AS price_sqft,
                   r.avg_living_area_sqft AS living_area,
                   r.total_incidents     AS total_incidents,
                   r.violent_crime_count AS violent_crime,
                   r.total_stops         AS transit_stops,
                   r.has_rapid_transit   AS has_rapid_transit,
                   r.total_restaurants   AS restaurants,
                   r.avg_rating          AS restaurant_rating,
                   r.total_schools       AS schools,
                   r.total_universities  AS universities,
                   r.total_stations      AS bike_stations
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

        # Fixed: avg_score_delta (was score_delta — returned None)
        similar = [dict(r) for r in session.run("""
            MATCH (n:Neighborhood {name: $name})-[s:SIMILAR_TO]-(b:Neighborhood)
            RETURN b.name          AS neighbor,
                   s.avg_score_delta AS delta,
                   s.shared_domains  AS shared_domains,
                   s.based_on        AS based_on
            ORDER BY s.avg_score_delta ASC
            LIMIT 5
        """, name=neighborhood)]

    return {
        "neighborhood": neighborhood,
        "domain_scores": scores,
        "borders":       borders,
        "mbta_lines":    mbta,
        "similar_to":    similar,
    }


def neo4j_top_by_domain(driver, domain: str, limit: int = 5) -> list[dict]:
    """Top N neighborhoods for a given domain by composite score."""
    with driver.session() as session:
        return [dict(r) for r in session.run("""
            MATCH (n:Neighborhood)-[r:HAS_SCORE]->(d:Domain {name: $domain})
            RETURN n.name AS neighborhood,
                   r.composite_score AS score,
                   r.grade           AS grade
            ORDER BY r.composite_score DESC
            LIMIT $limit
        """, domain=domain, limit=limit)]


def neo4j_transit_connected(driver, neighborhood: str) -> list[dict]:
    """Neighborhoods sharing an MBTA line with the given neighborhood."""
    with driver.session() as session:
        return [dict(r) for r in session.run("""
            MATCH (n:Neighborhood {name: $name})-[:SERVED_BY]->(m:MBTALine)
                  <-[:SERVED_BY]-(b:Neighborhood)
            WHERE b.name <> $name
            RETURN b.name AS neighbor, m.name AS shared_line
            ORDER BY m.name, b.name
        """, name=neighborhood)]


def neo4j_compare(driver, hoods: list[str]) -> list[dict]:
    """Side-by-side domain scores for a list of neighborhoods."""
    with driver.session() as session:
        return [dict(r) for r in session.run("""
            MATCH (n:Neighborhood)-[r:HAS_SCORE]->(d:Domain)
            WHERE n.name IN $hoods
            RETURN n.name AS neighborhood,
                   d.name AS domain,
                   r.composite_score AS score,
                   r.grade           AS grade
            ORDER BY n.name, d.name
        """, hoods=hoods)]


# ── Snowflake mart queries ────────────────────────────────────────────────────

def sf_housing_detail(cur, neighborhood: str) -> Optional[dict]:
    """
    Pull housing detail from the mart using real column names.
    Neighborhood names in Neo4j are UPPERCASE — match with UPPER().
    """
    cur.execute("""
        SELECT
            NEIGHBORHOOD_NAME,
            CITY,
            HOUSING_SCORE,
            HOUSING_GRADE,
            AVG_PRICE_PER_SQFT,
            AVG_LIVING_AREA_SQFT,
            TOTAL_PROPERTIES,
            AVG_ASSESSED_VALUE,
            AVG_ESTIMATED_RENT,
            PASS1_SCORE
        FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING
        WHERE UPPER(NEIGHBORHOOD_NAME) = UPPER(%s)
        LIMIT 1
    """, (neighborhood,))
    cols = [d[0].lower() for d in cur.description]
    row  = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def sf_safety_detail(cur, neighborhood: str) -> Optional[dict]:
    """Pull safety/crime detail for a neighborhood."""
    cur.execute("""
        SELECT
            NEIGHBORHOOD_NAME,
            CITY,
            SAFETY_SCORE,
            SAFETY_GRADE,
            TOTAL_INCIDENTS,
            VIOLENT_CRIME_COUNT,
            PROPERTY_CRIME_COUNT,
            INCIDENTS_PER_SQMILE,
            YOY_CHANGE_PCT
        FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
        WHERE UPPER(NEIGHBORHOOD_NAME) = UPPER(%s)
        LIMIT 1
    """, (neighborhood,))
    cols = [d[0].lower() for d in cur.description]
    row  = cur.fetchone()
    return dict(zip(cols, row)) if row else None


# ── Snowflake RAG hybrid search ───────────────────────────────────────────────

def sf_rag_search(cur, query: str, neo4j_domains: list[str],
                  top_k: int = RAG_TOP_K) -> list[dict]:
    """
    Hybrid search on RAW_DOMAIN_CHUNKS (65% vector + 35% keyword).
    neo4j_domains are converted to RAG domain tags before filtering.
    """
    rag_tags = rag_domains_for(neo4j_domains)
    if not rag_tags:
        rag_tags = list(NEO4J_TO_RAG_DOMAIN.values())  # fallback: all

    domain_filter = ", ".join(f"'{t}'" for t in rag_tags)

    cur.execute(f"""
        WITH vector_scores AS (
            SELECT
                chunk_id,
                source_file,
                domain,
                chunk_text,
                VECTOR_COSINE_SIMILARITY(
                    chunk_embedding,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'query: ' || %s)
                ) AS vec_score
            FROM {RAG_DB}.{RAG_SCHEMA}.{RAG_TABLE}
            WHERE UPPER(domain) IN ({domain_filter})
        ),
        keyword_scores AS (
            SELECT
                chunk_id,
                CASE WHEN LOWER(chunk_text) LIKE %s THEN 1.0 ELSE 0.0 END AS kw_score
            FROM {RAG_DB}.{RAG_SCHEMA}.{RAG_TABLE}
        ),
        combined AS (
            SELECT
                v.chunk_id,
                v.source_file,
                v.domain,
                v.chunk_text,
                ({VECTOR_WEIGHT} * v.vec_score + {KW_WEIGHT} * k.kw_score) AS hybrid_score
            FROM vector_scores v
            JOIN keyword_scores k ON v.chunk_id = k.chunk_id
        )
        SELECT chunk_id, source_file, domain, chunk_text, hybrid_score
        FROM combined
        ORDER BY hybrid_score DESC
        LIMIT %s
    """, (query, f"%{query.lower()[:30]}%", top_k))

    cols = [d[0].lower() for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Claude synthesis ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are the NeighbourWise AI graph agent for Greater Boston neighborhood
livability analysis. You receive data from three sources:

  1. Graph context   — Neo4j: domain scores, grades, borders, MBTA lines, similar neighborhoods
  2. Structured data — Snowflake mart: precise numeric metrics per neighborhood
  3. RAG context     — Unstructured chunks from domain documents (crime reports, housing studies, etc.)

Response rules:
  - Lead with a direct answer to the user's question
  - Quote specific scores and grades from the data (e.g. "Safety score 70/100, GOOD grade")
  - Compare to neighboring or similar neighborhoods where the data supports it
  - Mention which MBTA lines serve the neighborhood when transit is relevant
  - Note INSUFFICIENT DATA honestly if a domain has score 0
  - Keep response between 300–500 words
  - End with: "Sources: [graph] [structured mart] [RAG chunks]" listing which contributed

Never fabricate scores or relationships not present in the provided context."""


def synthesize(query: str, graph_ctx: dict, struct_ctx: dict,
               rag_chunks: list[dict]) -> str:
    parts = []

    if graph_ctx:
        parts.append("=== GRAPH CONTEXT (Neo4j) ===")
        parts.append(json.dumps(graph_ctx, indent=2, default=str))

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

    user_message = f"User query: {query}\n\n" + "\n".join(parts)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resp   = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=700,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    return resp.content[0].text


# ── Flask app ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)


@app.route("/graph-agent/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "agent": "NeighbourWise Graph Agent v2"})


@app.route("/graph-agent/query", methods=["POST"])
def query_endpoint():
    body         = request.get_json(force=True)
    user_query   = body.get("query", "").strip()
    hood_hint    = body.get("neighborhood", None)

    if not user_query:
        return jsonify({"error": "query field is required"}), 400

    log.info(f"Query: {user_query!r}  |  neighborhood hint: {hood_hint!r}")

    # 1. Query planning
    domains      = detect_domains(user_query)
    neighborhood = extract_neighborhood(user_query, hood_hint)
    log.info(f"Domains: {domains}  |  Neighborhood: {neighborhood}")

    # 2. Neo4j graph retrieval
    graph_ctx = {}
    try:
        driver = neo4j_driver()
        if neighborhood:
            graph_ctx["profile"]         = neo4j_neighborhood_profile(driver, neighborhood)
            graph_ctx["transit_network"] = neo4j_transit_connected(driver, neighborhood)
        graph_ctx["top_by_domain"] = {
            d: neo4j_top_by_domain(driver, d, limit=5)
            for d in domains[:3]
        }
        driver.close()
        log.info("Neo4j retrieval complete")
    except Exception as e:
        log.warning(f"Neo4j retrieval failed: {e}")
        graph_ctx = {"error": str(e)}

    # 3. Snowflake structured retrieval
    struct_ctx = {}
    try:
        conn = sf_connect()
        cur  = conn.cursor()
        if neighborhood:
            if "Housing" in domains:
                housing = sf_housing_detail(cur, neighborhood)
                if housing:
                    struct_ctx["housing"] = housing
            if "Safety" in domains:
                safety = sf_safety_detail(cur, neighborhood)
                if safety:
                    struct_ctx["safety"] = safety
        cur.close()
        conn.close()
        log.info(f"Snowflake structured retrieval complete: {list(struct_ctx.keys())}")
    except Exception as e:
        log.warning(f"Snowflake structured retrieval failed: {e}")

    # 4. RAG hybrid search
    rag_chunks = []
    try:
        conn = sf_connect()
        cur  = conn.cursor()
        rag_chunks = sf_rag_search(cur, user_query, domains)
        cur.close()
        conn.close()
        log.info(f"RAG retrieved {len(rag_chunks)} chunks")
    except Exception as e:
        log.warning(f"RAG retrieval failed: {e}")

    # 5. Claude synthesis
    try:
        answer = synthesize(user_query, graph_ctx, struct_ctx, rag_chunks)
        log.info("Synthesis complete")
    except Exception as e:
        log.error(f"Synthesis failed: {e}")
        return jsonify({"error": f"Synthesis failed: {e}"}), 500

    return jsonify({
        "query":        user_query,
        "neighborhood": neighborhood,
        "domains":      domains,
        "answer":       answer,
        "sources": {
            "graph_nodes":     bool(graph_ctx and "error" not in graph_ctx),
            "structured_mart": bool(struct_ctx),
            "rag_chunks":      len(rag_chunks),
        },
    })


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="NeighbourWise Graph Agent")
    p.add_argument("--port",  type=int, default=5001)
    p.add_argument("--debug", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    log.info(f"Starting NeighbourWise Graph Agent on port {args.port} …")
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)