"""
chunk_search.py
───────────────
Semantic search over chunks stored in:
  NEIGHBOURWISE_DOMAINS.RAW_UNSTRUCTURED.RAW_PROXIMITY_GROCERY_CHUNKS

How it works
────────────
1. Embed the user query with the same model used at ingest (snowflake-arctic-embed-m)
   via Snowflake Cortex EMBED_TEXT_768().
2. Run VECTOR_COSINE_SIMILARITY() against every stored chunk — fully
   server-side inside Snowflake, no data leaves the warehouse.
3. Return the top-k most relevant chunks, ranked by similarity score.

Usage
─────
  # Interactive REPL
  python chunk_search.py

  # Single query from the command line
  python chunk_search.py --query "supermarkets in Dorchester" --top-k 5

  # Save results to CSV
  python chunk_search.py --query "fruit markets Boston" --top-k 10 --output results.csv

  # Filter to a specific source file
  python chunk_search.py --query "convenience stores" --source grocery_details_02_12.csv
"""

import argparse
import csv
import json
import os
import sys
import textwrap
from datetime import datetime
from typing import Optional

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_DATABASE    = "NEIGHBOURWISE_DOMAINS"
DEFAULT_SCHEMA      = "RAW_UNSTRUCTURED"
DEFAULT_TABLE       = "RAW_PROXIMITY_GROCERY_CHUNKS"
DEFAULT_EMBED_MODEL = "e5-base-v2"
DEFAULT_TOP_K       = 5
MIN_SIMILARITY      = 0.0   # filter out results below this score (0.0 = no filter)

# ── Snowflake connection ───────────────────────────────────────────────────────

def sf_connect(database: str, schema: str):
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
        schema=schema,
        role=os.environ.get("SNOWFLAKE_ROLE"),
        network_timeout=120,
        login_timeout=60,
    )

# ── Embed a single query ───────────────────────────────────────────────────────

E5_MODELS       = {"e5-base-v2", "e5-large-v2"}
E5_QUERY_PREFIX = "query: "


def embed_query(cur, query: str, model: str) -> list:
    """
    Embed the user query. Prepends "query: " for e5 models — Snowflake
    Cortex passes the full string to the underlying model unchanged,
    so the prefix IS respected. Passages were stored with "passage: "
    prefix at ingest, giving correctly calibrated cosine scores.
    """
    if model in E5_MODELS:
        query = E5_QUERY_PREFIX + query
    safe_query = query.replace("'", "''")[:2000]
    cur.execute(
        f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model}', '{safe_query}');"
    )
    row = cur.fetchone()
    vec = row[0]
    if isinstance(vec, str):
        vec = json.loads(vec)
    return vec

# ── Vector similarity search ───────────────────────────────────────────────────

def _keyword_terms(query: str) -> list:
    """Extract meaningful words (>3 chars) from the query for keyword boosting."""
    import re
    stopwords = {"what","who","when","where","how","which","does","did",
                 "the","and","for","are","was","were","from","with","that",
                 "this","have","has","had","tell","about","give","list","find"}
    words = re.findall(r"[a-zA-Z]{4,}", query.lower())
    return [w for w in words if w not in stopwords]


def search_chunks(
    cur,
    database: str,
    schema: str,
    table: str,
    query_vector: list,
    top_k: int,
    min_similarity: float,
    source_filter: Optional[str],
    raw_query: str = "",
) -> list[dict]:
    """
    Hybrid search: vector cosine similarity + keyword boost.

    Pure vector search compresses scores into a narrow band (~0.63-0.80)
    when model calibration is imperfect, making ranking unreliable.
    Adding a keyword signal ensures exact-match chunks surface correctly.

    Score = (vector_similarity * 0.65) + (keyword_match_fraction * 0.35)

    keyword_match_fraction = how many query keywords appear in chunk_text
    (0.0 → none match, 1.0 → all match)
    """
    fqn           = f"{database}.{schema}.{table}"
    vec_json      = json.dumps(query_vector)
    source_clause = f"AND source_file ILIKE '%{source_filter}%'" if source_filter else ""

    # Build keyword ILIKE conditions for the boost signal
    terms         = _keyword_terms(raw_query) if raw_query else []
    n_terms       = len(terms) if terms else 1
    kw_parts      = " + ".join(
        [f"IFF(LOWER(chunk_text) ILIKE '%{t}%', 1, 0)" for t in terms]
    ) if terms else "0"

    sql = f"""
        WITH base AS (
            SELECT
                chunk_id,
                source_file,
                chunk_index,
                chunk_text,
                VECTOR_COSINE_SIMILARITY(
                    chunk_embedding,
                    PARSE_JSON('{vec_json}')::VECTOR(FLOAT, 768)
                )                                               AS vec_score,
                ({kw_parts})                                    AS kw_hits,
                ({kw_parts}) / {n_terms}.0                      AS kw_score
            FROM {fqn}
            WHERE 1=1
            {source_clause}
        )
        SELECT
            chunk_id,
            source_file,
            chunk_index,
            chunk_text,
            vec_score,
            kw_score,
            ROUND(kw_hits)                                      AS keyword_matches,
            (vec_score * 0.65 + kw_score * 0.35)               AS similarity
        FROM base
        WHERE vec_score >= {min_similarity}
        ORDER BY similarity DESC
        LIMIT {top_k};
    """
    cur.execute(sql)
    columns = [col[0].lower() for col in cur.description]
    rows    = cur.fetchall()
    return [dict(zip(columns, row)) for row in rows]

# ── Display helpers ────────────────────────────────────────────────────────────

SEPARATOR  = "─" * 72
# With correct e5 query: prefix, strong matches score ~0.88-0.95+
SCORE_BARS = {0.90: "●●●●● excellent",
              0.80: "●●●●○ good",
              0.70: "●●●○○ fair",
              0.60: "●●○○○ weak",
              0.00: "●○○○○ poor"}

def score_bar(sim: float) -> str:
    for threshold, label in sorted(SCORE_BARS.items(), reverse=True):
        if sim >= threshold:
            return label
    return "○○○○○ poor"


def print_results(results: list[dict], query: str, top_k: int) -> None:
    print(f"\n{'═' * 72}")
    print(f"  Query : {query}")
    print(f"  Found : {len(results)} result(s)  (top {top_k} requested)")
    print(f"{'═' * 72}\n")

    if not results:
        print("  No matching chunks found. Try a different query or lower --min-sim.")
        return

    for rank, r in enumerate(results, 1):
        sim     = float(r["similarity"])
        vec_s   = float(r.get("vec_score", sim))
        kw_s    = float(r.get("kw_score", 0))
        kw_hits = int(float(r.get("keyword_matches", 0)))
        bar     = score_bar(sim)
        text    = r["chunk_text"]

        # Show first 400 chars of the chunk, wrapped neatly
        preview = textwrap.fill(
            text[:400] + ("…" if len(text) > 400 else ""),
            width=68,
            initial_indent="  ",
            subsequent_indent="  ",
        )

        print(f"  #{rank}  {bar}  hybrid: {sim:.4f}  "
              f"(vec={vec_s:.4f}  kw={kw_s:.2f}  hits={kw_hits})")
        print(f"  Source     : {r['source_file']}")
        print(f"  Chunk index: {r['chunk_index']}  (id={r['chunk_id']})")
        print(f"  {SEPARATOR}")
        print(preview)
        print(f"\n{SEPARATOR}\n")


def save_csv(results: list[dict], path: str, query: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "similarity", "chunk_id", "source_file",
                         "chunk_index", "chunk_text", "query", "retrieved_at"])
        ts = datetime.utcnow().isoformat()
        for rank, r in enumerate(results, 1):
            writer.writerow([
                rank,
                f"{float(r['similarity']):.6f}",
                r["chunk_id"],
                r["source_file"],
                r["chunk_index"],
                r["chunk_text"],
                query,
                ts,
            ])
    print(f"\n  ✓ Results saved to: {path}")

# ── Interactive REPL ──────────────────────────────────────────────────────────

def run_repl(cur, args) -> None:
    print("\n" + "═" * 72)
    print("  NeighbourWise Chunk Search — interactive mode")
    print(f"  Table  : {args.database}.{args.schema}.{args.table}")
    print(f"  Model  : {args.embed_model}   Top-K : {args.top_k}")
    print("  Type 'exit' or Ctrl-C to quit")
    print("═" * 72 + "\n")

    while True:
        try:
            query = input("  🔍  Query: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n  Goodbye.")
            break

        if not query:
            continue
        if query.lower() in ("exit", "quit", "q"):
            print("  Goodbye.")
            break

        try:
            print("  Embedding query …", end="", flush=True)
            vec = embed_query(cur, query, args.embed_model)
            print(" done.")

            results = search_chunks(
                cur=cur,
                database=args.database,
                schema=args.schema,
                table=args.table,
                query_vector=vec,
                top_k=args.top_k,
                min_similarity=args.min_sim,
                source_filter=args.source,
                raw_query=query,
            )
            print_results(results, query, args.top_k)

            if args.output:
                save_csv(results, args.output, query)

        except Exception as exc:
            print(f"\n  ⚠ Error: {exc}\n")

# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Semantic chunk search over NEIGHBOURWISE_DOMAINS.RAW_UNSTRUCTURED",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Examples:
          python chunk_search.py
          python chunk_search.py --query "fruit markets in Boston" --top-k 5
          python chunk_search.py --query "convenience stores Springfield" --top-k 10 --output out.csv
          python chunk_search.py --query "supermarkets" --source grocery_details_02_12.csv
        """),
    )
    p.add_argument("--query",       default=None,
                   help="Query string. Omit to enter interactive REPL mode.")
    p.add_argument("--top-k",       type=int,   default=DEFAULT_TOP_K,
                   help=f"Number of results to return (default {DEFAULT_TOP_K})")
    p.add_argument("--min-sim",     type=float, default=MIN_SIMILARITY,
                   help="Minimum cosine similarity threshold, e.g. 0.6 (default 0.0)")
    p.add_argument("--source",      default=None,
                   help="Filter results to chunks from a specific source file (substring match)")
    p.add_argument("--output",      default=None,
                   help="Optional CSV file path to save results")
    p.add_argument("--database",    default=DEFAULT_DATABASE)
    p.add_argument("--schema",      default=DEFAULT_SCHEMA)
    p.add_argument("--table",       default=DEFAULT_TABLE)
    p.add_argument("--embed-model", default=DEFAULT_EMBED_MODEL)
    return p.parse_args()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    args.database = args.database.upper()
    args.schema   = args.schema.upper()

    conn = sf_connect(args.database, args.schema)
    cur  = conn.cursor()

    try:
        if args.query:
            # ── Single query mode ───────────────────────────────────────────
            print(f"\nEmbedding query …", end="", flush=True)
            vec = embed_query(cur, args.query, args.embed_model)
            print(" done.")

            results = search_chunks(
                cur=cur,
                database=args.database,
                schema=args.schema,
                table=args.table,
                query_vector=vec,
                top_k=args.top_k,
                min_similarity=args.min_sim,
                source_filter=args.source,
                raw_query=args.query,
            )
            print_results(results, args.query, args.top_k)

            if args.output:
                save_csv(results, args.output, args.query)

        else:
            # ── Interactive REPL mode ───────────────────────────────────────
            run_repl(cur, args)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()