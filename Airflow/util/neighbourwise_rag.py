#!/usr/bin/env python3
"""
neighbourwise_rag.py
────────────────────
Consolidated RAG pipeline for NeighbourWise AI.
Downloads documents, extracts text, chunks, embeds via Snowflake Cortex,
loads to Snowflake, and provides semantic search.

Usage:
  # Step 1: Download PDF and extract text
  python neighbourwise_rag.py download \
      --url "https://www.boston.gov/sites/default/files/file/2025/07/2024%20City%20of%20Boston%20Annual%20Surveillance%20Report.pdf" \
      --outdir ./rag_docs \
      --domain crime

  # Step 2: Chunk, embed, and load to Snowflake
  python neighbourwise_rag.py load \
      --input ./rag_docs/crime/ \
      --domain crime

  # Step 3: Search
  python neighbourwise_rag.py search \
      --query "surveillance cameras Dorchester" \
      --domain crime

  # Or interactive search
  python neighbourwise_rag.py search --domain crime

Requirements:
  pip install snowflake-connector-python pdfplumber requests python-dotenv
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_DATABASE      = "NEIGHBOURWISE_DOMAINS"
DEFAULT_SCHEMA        = "RAW_UNSTRUCTURED"
DEFAULT_EMBED_MODEL   = "e5-base-v2"
DEFAULT_CHUNK_SIZE    = 1000
DEFAULT_CHUNK_OVERLAP = 200
DEFAULT_EMBED_BATCH   = 50
DEFAULT_INSERT_BATCH  = 200
DEFAULT_MIN_CHARS     = 150
DEFAULT_TOP_K         = 5

# E5 model prefixes — passage: for ingest, query: for search
E5_MODELS         = {"e5-base-v2", "e5-large-v2"}
E5_PASSAGE_PREFIX = "passage: "
E5_QUERY_PREFIX   = "query: "


# ═════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE CONNECTION
# ═════════════════════════════════════════════════════════════════════════════

def sf_connect():
    """Connect to Snowflake using env variables from .env file."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", DEFAULT_DATABASE),
        schema=DEFAULT_SCHEMA,
        role=os.environ.get("SNOWFLAKE_ROLE"),
        insecure_mode=True,
        network_timeout=120,
        login_timeout=60,
    )


def get_table_name(domain: str, model: str = DEFAULT_EMBED_MODEL) -> str:
    """Table naming convention: RAW_{DOMAIN}_CHUNKS or RAW_{DOMAIN}_CHUNKS_{MODEL} for non-default."""
    base = f"RAW_{domain.upper()}_CHUNKS"
    if model == DEFAULT_EMBED_MODEL:
        return base
    # Sanitize model name for table suffix: e5-base-v2 → E5_BASE_V2
    suffix = model.upper().replace("-", "_").replace(".", "_")
    return f"{base}_{suffix}"


def ensure_schema(cur) -> None:
    """Create RAW_UNSTRUCTURED schema if it doesn't exist."""
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DEFAULT_DATABASE}.{DEFAULT_SCHEMA};")
    cur.execute(f"USE SCHEMA {DEFAULT_DATABASE}.{DEFAULT_SCHEMA};")
    print(f"  Schema {DEFAULT_DATABASE}.{DEFAULT_SCHEMA} ready.")


def ensure_table(cur, domain: str, model: str = DEFAULT_EMBED_MODEL) -> None:
    """Create the chunks table for a given domain + model."""
    fqn = f"{DEFAULT_DATABASE}.{DEFAULT_SCHEMA}.{get_table_name(domain, model)}"
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            chunk_id        NUMBER AUTOINCREMENT PRIMARY KEY,
            source_file     VARCHAR,
            chunk_index     NUMBER,
            chunk_text      VARCHAR,
            chunk_embedding VECTOR(FLOAT, 768),
            loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
    """)
    print(f"  Table {fqn} ready.")


# ═════════════════════════════════════════════════════════════════════════════
# DOWNLOAD — Fetch PDF, extract text
# ═════════════════════════════════════════════════════════════════════════════

def download_pdf(url: str, outdir: Path) -> Path:
    """Download a PDF from a URL to the output directory."""
    outdir.mkdir(parents=True, exist_ok=True)

    # Derive filename from URL
    filename = url.split("/")[-1]
    if not filename.endswith(".pdf"):
        filename = filename + ".pdf"
    # Clean URL-encoded characters
    filename = requests.utils.unquote(filename)
    # Sanitize
    filename = re.sub(r'[^\w\-.]', '_', filename)

    pdf_path = outdir / filename

    print(f"  Downloading: {url}")
    resp = requests.get(url, timeout=60, stream=True)
    resp.raise_for_status()

    with open(pdf_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    size_kb = pdf_path.stat().st_size / 1024
    print(f"  Saved: {pdf_path} ({size_kb:.1f} KB)")
    return pdf_path


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from a PDF using pdfplumber."""
    import pdfplumber

    pages_text = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text:
                pages_text.append(f"--- Page {i+1} ---\n{text}")

    full_text = "\n\n".join(pages_text)
    print(f"  Extracted {len(pages_text)} pages, {len(full_text)} characters")
    return full_text


def save_extracted_text(text: str, pdf_path: Path) -> Path:
    """Save extracted text alongside the PDF."""
    txt_path = pdf_path.with_suffix('.txt')
    txt_path.write_text(text, encoding='utf-8')
    print(f"  Text saved: {txt_path}")
    return txt_path


def cmd_download(args):
    """CLI handler for the 'download' command."""
    outdir = Path(args.outdir) / args.domain
    print(f"\n{'='*60}")
    print(f"  DOWNLOAD — domain: {args.domain}")
    print(f"{'='*60}")

    pdf_path = download_pdf(args.url, outdir)
    text = extract_text_from_pdf(pdf_path)
    save_extracted_text(text, pdf_path)

    print(f"\n  ✓ Done. Text ready for loading at: {outdir}")


# ═════════════════════════════════════════════════════════════════════════════
# TEXT CHUNKING
# ═════════════════════════════════════════════════════════════════════════════

def _char_chunk(text: str, chunk_size: int, overlap: int) -> List[str]:
    """Character-window chunking with whitespace-aware splitting."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        if end < len(text):
            # Try to break at whitespace
            brk = end
            while brk > start and not text[brk].isspace():
                brk -= 1
            if brk > start:
                end = brk
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end - overlap if overlap < (end - start) else end
    return chunks


def chunk_text(text: str, chunk_size: int, overlap: int, filename: str = "") -> List[str]:
    """
    Hybrid chunker: structural for markdown/page-based text, char-window otherwise.
    PDF extracted text uses page markers (--- Page N ---) as section boundaries.
    """
    PAGE_RE = re.compile(r"(?=--- Page \d+ ---)")

    # Split on page boundaries if present (PDF-extracted text)
    sections = PAGE_RE.split(text)
    if len(sections) <= 1:
        return _char_chunk(text, chunk_size, overlap)

    chunks = []
    for section in sections:
        section = section.strip()
        if not section:
            continue
        if len(section) <= chunk_size:
            chunks.append(section)
        else:
            # Sub-chunk large pages, prepend page header to continuations
            lines = section.split('\n', 1)
            header = lines[0].strip() if lines[0].startswith('---') else ""
            for j, sub in enumerate(_char_chunk(section, chunk_size, overlap)):
                if j > 0 and header and not sub.startswith('---'):
                    chunks.append(f"{header} (cont.)\n{sub}")
                else:
                    chunks.append(sub)

    return chunks


# ═════════════════════════════════════════════════════════════════════════════
# EMBEDDING via Snowflake Cortex
# ═════════════════════════════════════════════════════════════════════════════

def _add_passage_prefix(text: str, model: str) -> str:
    return (E5_PASSAGE_PREFIX + text) if model in E5_MODELS else text


def embed_batch(cur, texts: List[str], model: str) -> List[list]:
    """Embed a batch of texts in one Snowflake round-trip."""
    if not texts:
        return []

    parts = []
    for i, text in enumerate(texts):
        safe = _add_passage_prefix(text, model).replace("'", "''")
        safe = safe[:2000]  # Cortex token limit
        parts.append(
            f"SELECT {i} AS idx, "
            f"SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model}', '{safe}') AS vec"
        )

    sql = "\nUNION ALL\n".join(parts) + "\nORDER BY idx"
    cur.execute(sql)
    rows = cur.fetchall()

    embeddings = []
    for _, vec in rows:
        if isinstance(vec, str):
            vec = json.loads(vec)
        embeddings.append(vec)
    return embeddings


def embed_all_chunks(cur, chunks: List[str], model: str,
                     batch_size: int) -> List[list]:
    """Embed all chunks with progress bar and retry logic."""
    total = len(chunks)
    embeddings = []
    batches = [chunks[i:i+batch_size] for i in range(0, total, batch_size)]
    n_batches = len(batches)
    t0 = time.time()

    for b_idx, batch in enumerate(batches):
        for attempt in range(1, 4):
            try:
                batch_embs = embed_batch(cur, batch, model)
                break
            except Exception as exc:
                if attempt == 3:
                    raise
                wait = 2 ** attempt
                print(f"\n    Embed retry {attempt}: {exc}")
                time.sleep(wait)

        embeddings.extend(batch_embs)

        # Progress
        done = len(embeddings)
        pct = done / total * 100
        elapsed = time.time() - t0
        eta = (elapsed / done * (total - done)) if done > 0 else 0
        sys.stdout.write(
            f"\r  Embedding: {done}/{total} ({pct:.1f}%) ETA {eta:.0f}s   "
        )
        sys.stdout.flush()

    print()
    return embeddings


# ═════════════════════════════════════════════════════════════════════════════
# INSERT to Snowflake (via temp table to handle VECTOR type)
# ═════════════════════════════════════════════════════════════════════════════

def insert_chunks(cur, conn, domain: str, source_file: str,
                  chunks: List[str], embeddings: List[list],
                  batch_size: int, model: str = DEFAULT_EMBED_MODEL) -> None:
    """Insert chunks with embeddings using temp table workaround for VECTOR type."""
    fqn = f"{DEFAULT_DATABASE}.{DEFAULT_SCHEMA}.{get_table_name(domain, model)}"
    tmp = f"{DEFAULT_DATABASE}.{DEFAULT_SCHEMA}.TMP_CHUNK_STAGE"
    total = len(chunks)
    done = 0

    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {tmp} (
            source_file    VARCHAR,
            chunk_index    NUMBER,
            chunk_text     VARCHAR,
            embedding_json VARCHAR
        );
    """)

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        rows = [
            (source_file, start + i, chunk, json.dumps(emb))
            for i, (chunk, emb) in enumerate(
                zip(chunks[start:end], embeddings[start:end])
            )
        ]

        cur.executemany(
            f"INSERT INTO {tmp} (source_file, chunk_index, chunk_text, embedding_json) "
            f"VALUES (%s, %s, %s, %s)",
            rows,
        )

        cur.execute(f"""
            INSERT INTO {fqn}
                (source_file, chunk_index, chunk_text, chunk_embedding)
            SELECT
                source_file, chunk_index, chunk_text,
                PARSE_JSON(embedding_json)::VECTOR(FLOAT, 768)
            FROM {tmp};
        """)

        cur.execute(f"TRUNCATE TABLE {tmp};")
        conn.commit()
        done += len(rows)
        sys.stdout.write(f"\r  Inserting: {done}/{total} rows   ")
        sys.stdout.flush()

    cur.execute(f"DROP TABLE IF EXISTS {tmp};")
    print()


def cmd_load(args):
    """CLI handler for the 'load' command."""
    input_dir = Path(args.input)
    domain = args.domain
    model = args.embed_model
    table_name = get_table_name(domain, model)
    fqn = f"{DEFAULT_DATABASE}.{DEFAULT_SCHEMA}.{table_name}"

    print(f"\n{'='*60}")
    print(f"  LOAD — domain: {domain}")
    print(f"  Source: {input_dir}")
    print(f"  Model: {model}, Chunk: {args.chunk_size}/{args.chunk_overlap}")
    print(f"  Table: {table_name}")
    print(f"{'='*60}")

    # Find all .txt files in the input directory
    txt_files = sorted(input_dir.glob("*.txt"))
    if not txt_files:
        print(f"  No .txt files found in {input_dir}. Run 'download' first.")
        return

    print(f"  Found {len(txt_files)} text file(s)")

    conn = sf_connect()
    cur = conn.cursor()

    try:
        ensure_schema(cur)
        ensure_table(cur, domain, model)

        # ── Dedup: check which source files are already loaded ────────────
        try:
            cur.execute(f"SELECT DISTINCT source_file FROM {fqn};")
            already_loaded = {row[0] for row in cur.fetchall()}
        except:
            already_loaded = set()

        if already_loaded:
            print(f"  Already loaded: {len(already_loaded)} file(s)")

        total_chunks = 0
        skipped = 0
        for txt_path in txt_files:
            if txt_path.name in already_loaded:
                print(f"\n  ── {txt_path.name}  ⏭ SKIP (already loaded)")
                skipped += 1
                continue

            print(f"\n  ── {txt_path.name}")
            text = txt_path.read_text(encoding='utf-8')

            chunks = [
                c for c in chunk_text(text, args.chunk_size, args.chunk_overlap, txt_path.name)
                if len(c) >= args.min_chars
            ]
            print(f"  {len(chunks)} chunks (min {args.min_chars} chars)")

            if not chunks:
                print("  Skipping — no valid chunks")
                continue

            print(f"  Embedding ({model}, batch={args.embed_batch})...")
            embeddings = embed_all_chunks(cur, chunks, model, args.embed_batch)

            print(f"  Loading to Snowflake...")
            insert_chunks(cur, conn, domain, txt_path.name,
                          chunks, embeddings, args.insert_batch, model)

            total_chunks += len(chunks)
            print(f"  ✓ {len(chunks)} chunks loaded from {txt_path.name}")

        # Final count
        cur.execute(f"SELECT COUNT(*) FROM {fqn};")
        total_rows = cur.fetchone()[0]
        print(f"\n{'='*60}")
        print(f"  ✅ Load complete")
        print(f"  Table: {fqn}")
        print(f"  New chunks this run: {total_chunks}")
        print(f"  Skipped (already loaded): {skipped}")
        print(f"  Total rows in table: {total_rows}")
        print(f"{'='*60}")

    finally:
        cur.close()
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# SEARCH — Semantic search over loaded chunks
# ═════════════════════════════════════════════════════════════════════════════

def embed_query(cur, query: str, model: str) -> list:
    """Embed a query string. Prepends 'query: ' for e5 models."""
    q = (E5_QUERY_PREFIX + query) if model in E5_MODELS else query
    safe = q.replace("'", "''")[:2000]
    cur.execute(
        f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model}', '{safe}');"
    )
    vec = cur.fetchone()[0]
    if isinstance(vec, str):
        vec = json.loads(vec)
    return vec


def _keyword_terms(query: str) -> list:
    """Extract meaningful words for keyword boosting."""
    stopwords = {"what","who","when","where","how","which","does","did",
                 "the","and","for","are","was","were","from","with","that",
                 "this","have","has","had","tell","about","give","list","find",
                 "boston","cambridge","somerville","neighborhood","area","city"}
    words = re.findall(r"[a-zA-Z]{4,}", query.lower())
    return [w for w in words if w not in stopwords]


def search_chunks(cur, domain: str, query_vector: list, top_k: int,
                  raw_query: str = "", source_filter: Optional[str] = None,
                  model: str = DEFAULT_EMBED_MODEL) -> list:
    """
    Hybrid search: vector cosine similarity (65%) + keyword boost (35%).
    """
    fqn = f"{DEFAULT_DATABASE}.{DEFAULT_SCHEMA}.{get_table_name(domain, model)}"
    vec_json = json.dumps(query_vector)
    source_clause = f"AND source_file ILIKE '%{source_filter}%'" if source_filter else ""

    terms = _keyword_terms(raw_query) if raw_query else []
    n_terms = len(terms) if terms else 1
    kw_parts = " + ".join(
        [f"IFF(LOWER(chunk_text) ILIKE '%{t}%', 1, 0)" for t in terms]
    ) if terms else "0"

    sql = f"""
        WITH base AS (
            SELECT
                chunk_id, source_file, chunk_index, chunk_text,
                VECTOR_COSINE_SIMILARITY(
                    chunk_embedding,
                    PARSE_JSON('{vec_json}')::VECTOR(FLOAT, 768)
                ) AS vec_score,
                ({kw_parts}) AS kw_hits,
                ({kw_parts}) / {n_terms}.0 AS kw_score
            FROM {fqn}
            WHERE 1=1 {source_clause}
        )
        SELECT
            chunk_id, source_file, chunk_index, chunk_text,
            vec_score, kw_score,
            ROUND(kw_hits) AS keyword_matches,
            (vec_score * 0.65 + kw_score * 0.35) AS similarity
        FROM base
        ORDER BY similarity DESC
        LIMIT {top_k};
    """
    cur.execute(sql)
    columns = [col[0].lower() for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def print_results(results: list, query: str, top_k: int) -> None:
    """Display search results."""
    print(f"\n{'═'*60}")
    print(f"  Query : {query}")
    print(f"  Found : {len(results)} result(s)")
    print(f"{'═'*60}\n")

    if not results:
        print("  No matching chunks found.")
        return

    for rank, r in enumerate(results, 1):
        sim = float(r["similarity"])
        vec_s = float(r.get("vec_score", sim))
        kw_s = float(r.get("kw_score", 0))
        kw_hits = int(float(r.get("keyword_matches", 0)))
        text = r["chunk_text"]

        preview = textwrap.fill(
            text[:400] + ("…" if len(text) > 400 else ""),
            width=68, initial_indent="  ", subsequent_indent="  ",
        )

        print(f"  #{rank}  hybrid: {sim:.4f}  (vec={vec_s:.4f}  kw={kw_s:.2f}  hits={kw_hits})")
        print(f"  Source: {r['source_file']}  chunk: {r['chunk_index']}")
        print(f"  {'─'*56}")
        print(preview)
        print()


def cmd_search(args):
    """CLI handler for the 'search' command."""
    domain = args.domain
    model = args.embed_model

    conn = sf_connect()
    cur = conn.cursor()

    try:
        if args.query:
            # Single query mode
            print(f"\n  Embedding query ({model})...", end="", flush=True)
            vec = embed_query(cur, args.query, model)
            print(" done.")

            results = search_chunks(
                cur, domain, vec, args.top_k,
                raw_query=args.query, source_filter=args.source,
                model=model
            )
            print_results(results, args.query, args.top_k)
        else:
            # Interactive REPL
            print(f"\n{'═'*60}")
            print(f"  NeighbourWise RAG Search — domain: {domain}")
            print(f"  Model: {model}  Table: {get_table_name(domain, model)}")
            print(f"  Top-K: {args.top_k}")
            print(f"  Type 'exit' to quit")
            print(f"{'═'*60}\n")

            while True:
                try:
                    query = input("  🔍 Query: ").strip()
                except (KeyboardInterrupt, EOFError):
                    print("\n  Goodbye.")
                    break

                if not query or query.lower() in ("exit", "quit", "q"):
                    print("  Goodbye.")
                    break

                try:
                    print("  Embedding...", end="", flush=True)
                    vec = embed_query(cur, query, model)
                    print(" done.")

                    results = search_chunks(
                        cur, domain, vec, args.top_k,
                        raw_query=query, source_filter=args.source,
                        model=model
                    )
                    print_results(results, query, args.top_k)
                except Exception as exc:
                    print(f"\n  ⚠ Error: {exc}\n")

    finally:
        cur.close()
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="NeighbourWise RAG Pipeline — download, load, search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── download ──────────────────────────────────────────────────────────
    dl = subparsers.add_parser("download", help="Download PDF and extract text")
    dl.add_argument("--url", required=True, help="URL of the PDF to download")
    dl.add_argument("--outdir", default="./rag_docs", help="Output directory")
    dl.add_argument("--domain", required=True,
                    help="Domain name (crime, grocery, healthcare, etc.)")

    # ── load ──────────────────────────────────────────────────────────────
    ld = subparsers.add_parser("load", help="Chunk, embed, and load to Snowflake")
    ld.add_argument("--input", required=True,
                    help="Directory containing .txt files from download step")
    ld.add_argument("--domain", required=True,
                    help="Domain name — determines table: RAW_{DOMAIN}_CHUNKS")
    ld.add_argument("--embed-model", default=DEFAULT_EMBED_MODEL)
    ld.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    ld.add_argument("--chunk-overlap", type=int, default=DEFAULT_CHUNK_OVERLAP)
    ld.add_argument("--embed-batch", type=int, default=DEFAULT_EMBED_BATCH)
    ld.add_argument("--insert-batch", type=int, default=DEFAULT_INSERT_BATCH)
    ld.add_argument("--min-chars", type=int, default=DEFAULT_MIN_CHARS)

    # ── search ────────────────────────────────────────────────────────────
    sr = subparsers.add_parser("search", help="Semantic search over loaded chunks")
    sr.add_argument("--query", default=None,
                    help="Query string (omit for interactive REPL)")
    sr.add_argument("--domain", required=True,
                    help="Domain name to search in")
    sr.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    sr.add_argument("--source", default=None,
                    help="Filter to specific source file (substring match)")
    sr.add_argument("--embed-model", default=DEFAULT_EMBED_MODEL)

    # ── compare ───────────────────────────────────────────────────────────
    cp = subparsers.add_parser("compare", help="Compare search results across models")
    cp.add_argument("--domain", required=True, help="Domain name")
    cp.add_argument("--models", required=True,
                    help="Comma-separated model names (e.g. 'e5-base-v2,snowflake-arctic-embed-m')")
    cp.add_argument("--query", default=None,
                    help="Custom query (omit to run standard validation queries)")

    args = parser.parse_args()

    if args.command == "download":
        cmd_download(args)
    elif args.command == "load":
        cmd_load(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "compare":
        cmd_compare(args)


def cmd_compare(args):
    """Run the same queries against two models and compare results side-by-side."""
    domain = args.domain
    models = [m.strip() for m in args.models.split(",")]
    queries = [
        "breaking and entering arrest April 2024",
        "grocery stores in Allston",
        "CCTV video evidence used to catch criminals",
    ]
    if args.query:
        queries = [args.query]

    conn = sf_connect()
    cur = conn.cursor()

    try:
        print(f"\n{'═'*70}")
        print(f"  MODEL COMPARISON — domain: {domain}")
        print(f"  Models: {' vs '.join(models)}")
        print(f"{'═'*70}")

        for query in queries:
            print(f"\n  Query: \"{query}\"")
            print(f"  {'─'*60}")

            for model in models:
                table = get_table_name(domain, model)
                # Check table exists
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {DEFAULT_DATABASE}.{DEFAULT_SCHEMA}.{table};")
                    count = cur.fetchone()[0]
                except:
                    print(f"    {model:<30} Table {table} not found — run 'load' first")
                    continue

                vec = embed_query(cur, query, model)
                results = search_chunks(
                    cur, domain, vec, 3,
                    raw_query=query, model=model
                )

                if results:
                    top = results[0]
                    sim = float(top["similarity"])
                    vec_s = float(top.get("vec_score", sim))
                    kw_s = float(top.get("kw_score", 0))
                    chunk_preview = top["chunk_text"][:100].replace('\n', ' ')
                    print(f"    {model:<30} top={sim:.4f} (vec={vec_s:.4f} kw={kw_s:.2f}) "
                          f"chunk#{top['chunk_index']}")
                    print(f"    {'':30} \"{chunk_preview}...\"")
                else:
                    print(f"    {model:<30} No results")

        print(f"\n{'═'*70}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()