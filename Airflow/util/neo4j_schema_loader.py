"""
neo4j_schema_loader.py  (v2 — matches real Snowflake schema)
=============================================================
NeighbourWise AI — Graph Agent: Schema Loader
Syncs ALL 9 domain mart tables from Snowflake into Neo4j, then enriches
each Neighborhood node with unstructured context from RAW_DOMAIN_CHUNKS.

Neo4j graph produced:
    Nodes
    ─────
    (:Neighborhood  {name, city, state, rag_<domain>_context, rag_<domain>_source})
    (:Domain        {name})
    (:MBTALine      {name})

    Relationships
    ─────────────
    (:Neighborhood)-[:HAS_SCORE  {domain, composite_score, grade, …metrics}]->(:Domain)
    (:Neighborhood)-[:BORDERS   ]->(:Neighborhood)
    (:Neighborhood)-[:SERVED_BY ]->(:MBTALine)
    (:Neighborhood)-[:SIMILAR_TO {avg_score_delta, shared_domains}]->(:Neighborhood)

Usage:
    python neo4j_schema_loader.py               # full sync (all domains + RAG)
    python neo4j_schema_loader.py --dry-run     # preview fetched data, no writes
    python neo4j_schema_loader.py --skip-rag    # skip RAG enrichment
    python neo4j_schema_loader.py --domain Housing  # single domain only

Requirements:
    pip install neo4j snowflake-connector-python python-dotenv
"""

import os
import sys
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase
import snowflake.connector

# ── Env ───────────────────────────────────────────────────────────────────────
# Walk up from the script's location until we find a .env file.
# Works whether the script lives in utils/, the project root, or anywhere else.

def _find_env_file() -> Path:
    current = Path(__file__).resolve().parent
    for _ in range(5):                          # search up to 5 levels up
        candidate = current / ".env"
        if candidate.exists():
            return candidate
        current = current.parent
    return Path(".env")                         # fallback: current working dir

_env_path = _find_env_file()
load_dotenv(dotenv_path=_env_path)
print(f"  [env] Loaded .env from: {_env_path}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("graph_loader")

# ── Neo4j ─────────────────────────────────────────────────────────────────────

def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"\n\n  Missing required env variable: {key}\n"
            f"  Make sure it is set in your .env file at: {_env_path}\n"
        )
    return val

NEO4J_URI      = _require("NEO4J_URI")
NEO4J_USER     = _require("NEO4J_USERNAME")   # AuraDB exports this as NEO4J_USERNAME
NEO4J_PASSWORD = _require("NEO4J_PASSWORD")

# ── Snowflake ─────────────────────────────────────────────────────────────────

SF_ACCOUNT   = _require("SNOWFLAKE_ACCOUNT")
SF_USER_SF   = _require("SNOWFLAKE_USER")
SF_PASSWORD  = _require("SNOWFLAKE_PASSWORD")
SF_WAREHOUSE = _require("SNOWFLAKE_WAREHOUSE")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "NEIGHBOURWISE_DOMAINS")
SF_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "")

# ── Domain mart config ────────────────────────────────────────────────────────
# Maps domain label → mart table + column names.
# If your mart uses slightly different column names, edit here only.
# extra_cols are optional — missing ones are skipped gracefully.

DOMAIN_MARTS = {
    "Safety": {
        "table":      "MRT_NEIGHBORHOOD_SAFETY",
        "score_col":  "SAFETY_SCORE",
        "grade_col":  "SAFETY_GRADE",
        "extra_cols": ["TOTAL_INCIDENTS", "VIOLENT_CRIME_COUNT", "PROPERTY_CRIME_COUNT",
                       "INCIDENTS_PER_SQMILE", "YOY_CHANGE_PCT", "PCT_VIOLENT"],
    },
    "Housing": {
        "table":      "MRT_NEIGHBORHOOD_HOUSING",
        "score_col":  "HOUSING_SCORE",
        "grade_col":  "HOUSING_GRADE",
        "extra_cols": ["AVG_PRICE_PER_SQFT", "AVG_LIVING_AREA_SQFT",
                       "TOTAL_PROPERTIES", "AVG_ASSESSED_VALUE",
                       "AVG_ESTIMATED_RENT", "PASS1_SCORE"],
    },
    "Grocery": {
        "table":      "MRT_NEIGHBORHOOD_GROCERY_STORES",
        "score_col":  "GROCERY_SCORE",
        "grade_col":  "GROCERY_GRADE",
        "extra_cols": ["TOTAL_STORES", "STORES_PER_SQMILE",
                       "SUPERMARKET_COUNT", "ESSENTIAL_STORE_COUNT", "PCT_ESSENTIAL"],
    },
    "Healthcare": {
        "table":      "MRT_NEIGHBORHOOD_HEALTHCARE",
        "score_col":  "HEALTHCARE_SCORE",
        "grade_col":  "HEALTHCARE_GRADE",
        "extra_cols": ["TOTAL_FACILITIES", "HOSPITAL_COUNT", "CLINIC_COUNT",
                       "FACILITIES_PER_SQMILE", "DENSITY_SCORE", "DIVERSITY_SCORE"],
    },
    "MBTA": {
        "table":      "MRT_NEIGHBORHOOD_MBTA",
        "score_col":  "TRANSIT_SCORE",
        "grade_col":  "TRANSIT_GRADE",
        "extra_cols": ["TOTAL_STOPS", "RAPID_TRANSIT_STOPS", "BUS_STOPS",
                       "COMMUTER_RAIL_STOPS", "TOTAL_ROUTES",
                       "HAS_RAPID_TRANSIT", "PCT_ACCESSIBLE_STOPS"],
    },
    "Restaurants": {
        "table":      "MRT_NEIGHBORHOOD_RESTAURANTS",
        "score_col":  "RESTAURANT_SCORE",
        "grade_col":  "RESTAURANT_GRADE",
        "extra_cols": ["TOTAL_RESTAURANTS", "AVG_RATING", "CUISINE_DIVERSITY",
                       "RESTAURANTS_PER_SQMILE", "PCT_HIGH_QUALITY"],
    },
    "Schools": {
        "table":      "MRT_NEIGHBORHOOD_SCHOOLS",
        "score_col":  "SCHOOL_SCORE",
        "grade_col":  "SCHOOL_GRADE",
        "extra_cols": ["TOTAL_SCHOOLS", "PUBLIC_SCHOOL_COUNT", "PRIVATE_SCHOOL_COUNT",
                       "SCHOOLS_PER_SQMILE", "LEVEL_COVERAGE_SCORE"],
    },
    "Universities": {
        "table":      "MRT_NEIGHBORHOOD_UNIVERSITIES",
        "score_col":  "EDUCATION_SCORE",
        "grade_col":  "EDUCATION_GRADE",
        "extra_cols": ["TOTAL_UNIVERSITIES", "PUBLIC_COUNT", "PRIVATE_COUNT",
                       "HAS_UNIVERSITIES", "HAS_HIGHER_EDUCATION",
                       "UNIVERSITIES_PER_SQMILE"],
    },
    "Bluebikes": {
        "table":      "MRT_NEIGHBORHOOD_BLUEBIKES",
        "score_col":  "BIKESHARE_SCORE",
        "grade_col":  "BIKESHARE_GRADE",
        "extra_cols": ["TOTAL_STATIONS", "TOTAL_DOCKS",
                       "STATIONS_PER_SQMILE", "AVG_DOCKS_PER_STATION"],
    },
}

# Adjust if your marts use different neighborhood / city column names
HOOD_COL = "NEIGHBORHOOD_NAME"
CITY_COL = "CITY"

# RAG source
RAG_TABLE = "NEIGHBOURWISE_DOMAINS.RAW_UNSTRUCTURED.RAW_DOMAIN_CHUNKS"

# ── Static topology ───────────────────────────────────────────────────────────

ADJACENCY = [
    ("Boston", "Cambridge"),   ("Boston", "Somerville"),
    ("Boston", "Brookline"),   ("Boston", "Quincy"),
    ("Boston", "Chelsea"),     ("Boston", "Everett"),
    ("Cambridge", "Somerville"), ("Cambridge", "Watertown"),
    ("Cambridge", "Arlington"), ("Somerville", "Medford"),
    ("Somerville", "Arlington"), ("Arlington", "Lexington"),
    ("Arlington", "Belmont"),  ("Watertown", "Belmont"),
    ("Watertown", "Newton"),   ("Brookline", "Newton"),
    ("Quincy", "Milton"),      ("Chelsea", "Revere"),
    ("Everett", "Malden"),     ("Everett", "Medford"),
    ("Salem", "Beverly"),      ("Salem", "Peabody"),
]

MBTA_COVERAGE = {
    "Red Line":    ["Boston", "Cambridge", "Somerville", "Quincy", "Milton"],
    "Green Line":  ["Boston", "Cambridge", "Somerville", "Medford", "Newton", "Brookline"],
    "Orange Line": ["Boston", "Somerville", "Medford", "Malden", "Everett"],
    "Blue Line":   ["Boston", "Chelsea", "Revere", "Salem", "Beverly"],
    "Silver Line": ["Boston", "Chelsea"],
}

SIMILARITY_THRESHOLD = 10.0


# ══════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE
# ══════════════════════════════════════════════════════════════════════════════

def sf_connect():
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER_SF,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        role=SF_ROLE or None,
        network_timeout=120,
        login_timeout=60,
    )


def discover_columns(cur, table_name: str) -> set:
    """Return set of uppercase column names that actually exist in the table."""
    cur.execute(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'MARTS'
          AND UPPER(TABLE_NAME) = UPPER('{table_name}')
    """)
    return {r[0].upper() for r in cur.fetchall()}


def fetch_mart(cur, domain: str, config: dict) -> list[dict]:
    """Fetch one domain mart, skipping extra_cols that don't exist."""
    table    = f"NEIGHBOURWISE_DOMAINS.MARTS.{config['table']}"
    existing = discover_columns(cur, config["table"])

    base  = [HOOD_COL, CITY_COL, config["score_col"], config["grade_col"]]
    extra = [c for c in config["extra_cols"] if c.upper() in existing]
    missing = set(config["extra_cols"]) - existing
    if missing:
        log.warning(f"  [{domain}] Columns not found, skipped: {missing}")

    cols_sql = ", ".join(base + extra)
    try:
        cur.execute(f"SELECT {cols_sql} FROM {table}")
    except Exception as e:
        log.error(f"  [{domain}] Query failed: {e}")
        return []

    col_names = [d[0].lower() for d in cur.description]
    rows = [dict(zip(col_names, row)) for row in cur.fetchall()]
    for r in rows:
        r["_domain"] = domain
    log.info(f"  [{domain}] {len(rows)} rows from {config['table']}")
    return rows


def fetch_all_marts(cur) -> dict:
    return {d: fetch_mart(cur, d, cfg) for d, cfg in DOMAIN_MARTS.items()}


def fetch_rag_context(cur) -> dict:
    """
    Pull the top 1 RAG chunk per DOMAIN from RAW_DOMAIN_CHUNKS.
    These get stored as properties on every Neighborhood node so the
    graph agent has unstructured context without an extra Snowflake call.

    Returns: { "CRIME": {"source": "...", "text": "..."}, ... }
    """
    log.info("Fetching top RAG chunk per domain from RAW_DOMAIN_CHUNKS …")
    cur.execute(f"""
        SELECT DOMAIN, SOURCE_FILE, CHUNK_TEXT
        FROM (
            SELECT
                DOMAIN,
                SOURCE_FILE,
                CHUNK_TEXT,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(DOMAIN)
                    ORDER BY CHUNK_ID ASC
                ) AS rn
            FROM {RAG_TABLE}
            WHERE CHUNK_TEXT IS NOT NULL
              AND LENGTH(CHUNK_TEXT) > 100
        )
        WHERE rn = 1
    """)
    result = {}
    for domain, source, text in cur.fetchall():
        if domain:
            result[domain.upper()] = {
                "source": source or "",
                "text":   (text or "")[:800],
            }
    log.info(f"  RAG context fetched for domains: {list(result.keys())}")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# NEO4J
# ══════════════════════════════════════════════════════════════════════════════

def create_constraints(session):
    for label, prop in [("Neighborhood", "name"), ("Domain", "name"), ("MBTALine", "name")]:
        session.run(
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (x:{label}) REQUIRE x.{prop} IS UNIQUE"
        )
    log.info("Constraints ready")


def upsert_domain_nodes(session):
    names = list(DOMAIN_MARTS.keys())
    session.run("UNWIND $d AS n MERGE (:Domain {name: n})", d=names)
    log.info(f"Upserted {len(names)} Domain nodes")


def upsert_mbta_nodes(session):
    lines = list(MBTA_COVERAGE.keys())
    session.run("UNWIND $l AS n MERGE (:MBTALine {name: n})", l=lines)
    log.info(f"Upserted {len(lines)} MBTALine nodes")


def upsert_neighborhoods(session, all_data: dict) -> list:
    seen = {}
    for rows in all_data.values():
        for r in rows:
            name = r.get(HOOD_COL.lower()) or r.get("neighborhood_name")
            city = r.get(CITY_COL.lower()) or r.get("city", "")
            if name and name not in seen:
                seen[name] = {"name": name, "city": city, "state": "MA"}

    session.run("""
        UNWIND $hoods AS h
        MERGE (n:Neighborhood {name: h.name})
        SET n.city = h.city, n.state = h.state
    """, hoods=list(seen.values()))
    log.info(f"Upserted {len(seen)} Neighborhood nodes")
    return list(seen.keys())


def upsert_has_score(session, domain: str, config: dict, rows: list):
    hood_key  = HOOD_COL.lower()
    score_key = config["score_col"].lower()
    grade_key = config["grade_col"].lower()

    params = []
    for r in rows:
        hood = r.get(hood_key) or r.get("neighborhood_name")
        if not hood:
            continue
        p = {
            "hood":   hood,
            "domain": domain,
            "score":  float(r.get(score_key) or 0),
            "grade":  str(r.get(grade_key) or ""),
        }
        for col in config["extra_cols"]:
            val = r.get(col.lower())
            if val is not None:
                try:
                    p[col.lower()] = float(val)
                except (TypeError, ValueError):
                    p[col.lower()] = str(val)
        params.append(p)

    if not params:
        log.warning(f"  [{domain}] No valid rows for HAS_SCORE — skipping")
        return

    # Build SET clause for extra metric columns
    sample_extras = [k for k in params[0] if k not in ("hood", "domain", "score", "grade")]
    extra_sets = "\n        ".join(f"rel.{k} = row.{k}," for k in sample_extras)

    session.run(f"""
        UNWIND $rows AS row
        MATCH (n:Neighborhood {{name: row.hood}})
        MATCH (d:Domain        {{name: row.domain}})
        MERGE (n)-[rel:HAS_SCORE {{domain: row.domain}}]->(d)
        SET rel.composite_score = row.score,
            rel.grade           = row.grade{"," if extra_sets else ""}
        {extra_sets.rstrip(",")}
    """, rows=params)
    log.info(f"  [{domain}] {len(params)} HAS_SCORE relationships upserted")


def enrich_rag(session, rag_context: dict):
    """
    Write RAG chunk text + source as properties on ALL Neighborhood nodes.
    Property names:  rag_<domain_lower>_context  /  rag_<domain_lower>_source
    """
    for domain_upper, ctx in rag_context.items():
        prop_text = f"rag_{domain_upper.lower()}_context"
        prop_src  = f"rag_{domain_upper.lower()}_source"
        session.run(f"""
            MATCH (n:Neighborhood)
            SET n.{prop_text} = $text,
                n.{prop_src}  = $src
        """, text=ctx["text"], src=ctx["source"])
        log.info(f"  RAG enriched all hoods with {domain_upper} context → {prop_text}")


def upsert_borders(session):
    session.run("""
        UNWIND $pairs AS p
        MATCH (a:Neighborhood {name: p[0]})
        MATCH (b:Neighborhood {name: p[1]})
        MERGE (a)-[:BORDERS]->(b)
        MERGE (b)-[:BORDERS]->(a)
    """, pairs=[[a, b] for a, b in ADJACENCY])
    log.info(f"Upserted {len(ADJACENCY) * 2} BORDERS edges")


def upsert_served_by(session):
    params = [
        {"line": line, "hood": hood}
        for line, hoods in MBTA_COVERAGE.items()
        for hood in hoods
    ]
    session.run("""
        UNWIND $rows AS r
        MATCH (n:Neighborhood {name: r.hood})
        MATCH (m:MBTALine      {name: r.line})
        MERGE (n)-[:SERVED_BY]->(m)
    """, rows=params)
    log.info(f"Upserted SERVED_BY for {len(params)} pairs")


def upsert_similar_to(session, threshold: float = SIMILARITY_THRESHOLD):
    session.run("""
        MATCH (a:Neighborhood)-[ra:HAS_SCORE]->(d:Domain)
              <-[rb:HAS_SCORE]-(b:Neighborhood)
        WHERE a.name < b.name
        WITH a, b,
             avg(abs(ra.composite_score - rb.composite_score)) AS avg_delta,
             count(d) AS shared_domains
        WHERE avg_delta <= $thr AND shared_domains >= 2
        MERGE (a)-[s:SIMILAR_TO]-(b)
        SET s.avg_score_delta = avg_delta,
            s.shared_domains  = shared_domains,
            s.based_on        = 'multi-domain average'
    """, thr=threshold)
    log.info(f"Upserted SIMILAR_TO edges (threshold={threshold})")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def inspect_columns(cur):
    """Print ALL column names for every domain mart table — use this to fix config."""
    print("\n" + "=" * 60)
    print("COLUMN INSPECTOR — real Snowflake column names")
    print("=" * 60)
    for domain, config in DOMAIN_MARTS.items():
        table = config["table"]
        cur.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'MARTS'
              AND UPPER(TABLE_NAME) = UPPER('{table}')
            ORDER BY ORDINAL_POSITION
        """)
        cols = cur.fetchall()
        print(f"\n  [{domain}] {table}")
        if cols:
            for col_name, dtype in cols:
                print(f"    {col_name:<45} {dtype}")
        else:
            print(f"    *** Table not found or no columns returned ***")
    print("=" * 60 + "\n")


def parse_args():
    p = argparse.ArgumentParser(description="Sync Snowflake marts + RAG → Neo4j")
    p.add_argument("--dry-run",  action="store_true",
                   help="Fetch from Snowflake but skip Neo4j writes")
    p.add_argument("--skip-rag", action="store_true",
                   help="Skip RAG enrichment step")
    p.add_argument("--inspect",  action="store_true",
                   help="Print all real column names from every mart table then exit")
    p.add_argument("--domain",   default="all",
                   help=f"Single domain or 'all'. Options: {list(DOMAIN_MARTS)}")
    return p.parse_args()


def main():
    args = parse_args()

    conn = sf_connect()
    cur  = conn.cursor()

    # ── Inspect mode: print real column names and exit ────────────────────────
    if args.inspect:
        inspect_columns(cur)
        cur.close()
        conn.close()
        return

    # ── Step 1: Snowflake ─────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("STEP 1  Fetching data from Snowflake")
    log.info("=" * 60)

    try:
        if args.domain == "all":
            all_data = fetch_all_marts(cur)
        else:
            if args.domain not in DOMAIN_MARTS:
                log.error(f"Unknown domain '{args.domain}'. Options: {list(DOMAIN_MARTS)}")
                sys.exit(1)
            all_data = {args.domain: fetch_mart(cur, args.domain, DOMAIN_MARTS[args.domain])}

        rag_context = {} if args.skip_rag else fetch_rag_context(cur)
    finally:
        cur.close()
        conn.close()

    # ── Dry run ───────────────────────────────────────────────────────────────
    if args.dry_run:
        log.info("\n=== DRY RUN — no Neo4j writes ===")
        for d, rows in all_data.items():
            log.info(f"  {d:<15}: {len(rows)} rows")
        log.info(f"  RAG domains    : {list(rag_context.keys())}")
        log.info(f"  Adjacency pairs: {len(ADJACENCY)}")
        return

    # ── Step 2: Neo4j ─────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("STEP 2  Writing to Neo4j")
    log.info("=" * 60)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:

        log.info("\n-- Constraints + static nodes --")
        create_constraints(session)
        upsert_domain_nodes(session)
        upsert_mbta_nodes(session)

        log.info("\n-- Neighborhood nodes --")
        hood_names = upsert_neighborhoods(session, all_data)

        log.info("\n-- HAS_SCORE relationships (one pass per domain) --")
        for domain, rows in all_data.items():
            if rows:
                upsert_has_score(session, domain, DOMAIN_MARTS[domain], rows)

        if rag_context:
            log.info("\n-- RAG enrichment on Neighborhood nodes --")
            enrich_rag(session, rag_context)

        log.info("\n-- Structural edges --")
        upsert_borders(session)
        upsert_served_by(session)
        upsert_similar_to(session)

    driver.close()
    log.info("\n" + "=" * 60)
    log.info("✓  Sync complete")
    log.info(f"   Neighborhoods : {len(hood_names)}")
    log.info(f"   Domains       : {list(all_data.keys())}")
    log.info(f"   RAG domains   : {list(rag_context.keys())}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()