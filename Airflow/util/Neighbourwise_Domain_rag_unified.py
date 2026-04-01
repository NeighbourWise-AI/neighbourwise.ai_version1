#!/usr/bin/env python3
"""
neighbourwise_rag_unified.py — NeighbourWise AI Unified RAG Pipeline
═════════════════════════════════════════════════════════════════════
Combines all unstructured data operations into a single script:
  • S3 text/CSV/MD ingestion  (was: snowflake_unstructured_loader.py)
  • S3 PDF ingestion          (was: load_un.py)
  • Web page scraping         (was: extract_text.py)
  • Chunking + Embedding      (shared logic)
  • Hybrid semantic search    (was: chunk_search.py)

ALL chunks are stored in ONE Snowflake table:
    NEIGHBOURWISE_DOMAINS.RAW_UNSTRUCTURED.RAW_DOMAIN_CHUNKS

Each chunk is tagged with a DOMAIN column (crime, grocery, healthcare,
housing, etc.) so multiple domains coexist in one table and can be
searched individually or cross-domain.

COMMANDS
────────
  load-all     ★ Auto-discover S3 subfolders, derive domain from folder name, load all
  load-s3      Load text files (txt/md/csv/json) from a single S3 prefix
  load-pdf     Load PDF files from a single S3 prefix
  scrape       Scrape a web page, extract article text, chunk & load
  search       Hybrid semantic search (single query or interactive REPL)
  merge        Merge test table → production table
  download     Download a PDF from a URL and extract text locally

EXAMPLES
────────
  # ★ Load everything from all domain subfolders at once
  python neighbourwise_rag_unified.py load-all

  # Load only the Grocery subfolder
  python neighbourwise_rag_unified.py load-all --domain grocery

  # Preview what would be loaded (no writes)
  python neighbourwise_rag_unified.py load-all --dry-run

  # Search within a domain
  python neighbourwise_rag_unified.py search \\
      --domain grocery --query "supermarkets in Dorchester"

  # Search across ALL domains
  python neighbourwise_rag_unified.py search \\
      --domain all --query "what programs exist in Roxbury"

  # Merge tested chunks into production
  python neighbourwise_rag_unified.py merge --domain grocery

VALID DOMAINS
─────────────
  crime, grocery, healthcare, housing, schools,
  transit, restaurants, universities, bluebikes
  (or any custom name — just be consistent)

SETUP
─────
  pip install snowflake-connector-python boto3 pypdf requests \\
              beautifulsoup4 python-dotenv

  .env requires: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
                 SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
                 AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  (for S3 commands)
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import requests as http_requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════

DEFAULT_S3_BUCKET     = "neighborwise-ai-s3-bucket"
DEFAULT_DATABASE      = "NEIGHBOURWISE_DOMAINS"
DEFAULT_SCHEMA        = "RAW_UNSTRUCTURED"
DEFAULT_TABLE         = "RAW_DOMAIN_CHUNKS"   # ← test table; merge into RAW_DOMAIN_CHUNKS when ready
DEFAULT_EMBED_MODEL   = "e5-base-v2"
DEFAULT_CHUNK_SIZE    = 1000    # characters per chunk
DEFAULT_CHUNK_OVERLAP = 200     # overlap between consecutive chunks
DEFAULT_EMBED_BATCH   = 50      # chunks per Cortex embedding call
DEFAULT_INSERT_BATCH  = 200     # rows per Snowflake insert batch
DEFAULT_MIN_CHARS     = 150     # discard chunks shorter than this
DEFAULT_TOP_K         = 5

# E5 model prefix convention (required for correct retrieval calibration)
E5_MODELS         = {"e5-base-v2", "e5-large-v2"}
E5_PASSAGE_PREFIX = "passage: "
E5_QUERY_PREFIX   = "query: "

# Fully qualified table name (test)
FQN = f"{DEFAULT_DATABASE}.{DEFAULT_SCHEMA}.{DEFAULT_TABLE}"

# Production table (merge target)
PROD_TABLE = DEFAULT_TABLE
PROD_FQN   = FQN


# ═════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE CONNECTION
# ═════════════════════════════════════════════════════════════════════════════

def sf_connect():
    """Connect to Snowflake using .env credentials."""
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


def ensure_schema(cur):
    """Create RAW_UNSTRUCTURED schema if it doesn't exist."""
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DEFAULT_DATABASE}.{DEFAULT_SCHEMA};")
    cur.execute(f"USE SCHEMA {DEFAULT_DATABASE}.{DEFAULT_SCHEMA};")
    print(f"  Schema {DEFAULT_DATABASE}.{DEFAULT_SCHEMA} ready.")


def ensure_table(cur):
    """
    Create the unified RAW_DOMAIN_CHUNKS table if it doesn't exist.
    All domains share this single table, differentiated by the DOMAIN column.
    """
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {FQN} (
            chunk_id        NUMBER AUTOINCREMENT PRIMARY KEY,
            domain          VARCHAR,
            source_file     VARCHAR,
            chunk_index     NUMBER,
            chunk_text      VARCHAR,
            chunk_embedding VECTOR(FLOAT, 768),
            loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
    """)
    print(f"  Table {FQN} ready.")


# ═════════════════════════════════════════════════════════════════════════════
# TEXT CHUNKING  (hybrid: markdown-aware + character-window + page-aware)
# ═════════════════════════════════════════════════════════════════════════════

def _char_chunk(text: str, chunk_size: int, overlap: int) -> List[str]:
    """Character-window chunking with whitespace-aware splitting."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        if end < len(text):
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


def chunk_text(text: str, chunk_size: int, overlap: int,
               filename: str = "") -> List[str]:
    """
    Hybrid chunker that handles three document structures:

    1. Markdown (## / ### headings)  → split on heading boundaries,
       sub-chunk oversized sections with heading re-prepended.
    2. PDF page markers (--- Page N ---) → split on page boundaries,
       sub-chunk large pages.
    3. Everything else (CSV, JSON, plain text) → pure character windowing.
    """
    # ── Check for markdown headings ──────────────────────────────────────
    HEADING_RE  = re.compile(r"(?m)^#{1,3} ")
    SECTION_RE  = re.compile(r"(?=\n#{1,3} )")
    HEADING_CAP = re.compile(r"(#{1,3} [^\n]+\n)")

    is_markdown = (
        filename.lower().endswith((".md", ".markdown"))
        or bool(HEADING_RE.search(text))
    )

    if is_markdown:
        sections = SECTION_RE.split(text)
        chunks = []
        for section in sections:
            section = section.strip()
            if not section:
                continue
            if len(section) <= chunk_size:
                chunks.append(section)
            else:
                m = HEADING_CAP.match(section)
                heading = m.group(1).strip() if m else ""
                for j, sub in enumerate(_char_chunk(section, chunk_size, overlap)):
                    if j > 0 and heading and not sub.startswith("#"):
                        chunks.append(heading + " (cont.)\n" + sub)
                    else:
                        chunks.append(sub)
        return chunks

    # ── Check for PDF page markers ───────────────────────────────────────
    PAGE_RE = re.compile(r"(?=--- Page \d+ ---)")
    sections = PAGE_RE.split(text)

    if len(sections) > 1:
        chunks = []
        for section in sections:
            section = section.strip()
            if not section:
                continue
            if len(section) <= chunk_size:
                chunks.append(section)
            else:
                lines = section.split('\n', 1)
                header = lines[0].strip() if lines[0].startswith('---') else ""
                for j, sub in enumerate(_char_chunk(section, chunk_size, overlap)):
                    if j > 0 and header and not sub.startswith('---'):
                        chunks.append(f"{header} (cont.)\n{sub}")
                    else:
                        chunks.append(sub)
        return chunks

    # ── Fallback: plain character windowing ──────────────────────────────
    return _char_chunk(text, chunk_size, overlap)


# ═════════════════════════════════════════════════════════════════════════════
# PDF TEXT EXTRACTION
# ═════════════════════════════════════════════════════════════════════════════

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pypdf. Adds page markers."""
    import pypdf

    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    pages = []
    for i, page in enumerate(reader.pages):
        try:
            text = page.extract_text() or ""
            text = text.strip()
            if text:
                pages.append(f"--- Page {i+1} ---\n{text}")
        except Exception as e:
            print(f"  [warn] Could not read page {i}: {e}")
    return "\n\n".join(pages)


def extract_text_from_pdf_file(pdf_path: Path) -> str:
    """Extract text from a local PDF file using pdfplumber."""
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


# ═════════════════════════════════════════════════════════════════════════════
# WEB SCRAPING  (from extract_text.py)
# ═════════════════════════════════════════════════════════════════════════════

def fetch_html(url: str) -> str:
    """Fetch raw HTML from a URL."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; NeighbourWiseBot/1.0)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = http_requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def _strip_noise(soup) -> None:
    """Remove scripts, styles, and common chrome elements."""
    from bs4 import Tag

    for t in soup(["script", "style", "noscript", "iframe", "svg"]):
        t.decompose()

    selectors = [
        "header", "footer", "nav", "aside", ".sidebar",
        ".site-header", ".site-footer", ".navigation", ".menu",
        ".widget", ".share", ".social", ".post-navigation",
    ]
    for sel in selectors:
        for t in soup.select(sel):
            t.decompose()


def _find_main_container(soup):
    """Heuristic: prefer <article>, then walk up from <h1>, else <body>."""
    from bs4 import Tag

    article = soup.find("article")
    if isinstance(article, Tag):
        return article

    h1 = soup.find("h1")
    if isinstance(h1, Tag):
        cur = h1
        for _ in range(6):
            if cur.parent and isinstance(cur.parent, Tag):
                cur = cur.parent
        return cur

    body = soup.body
    return body if isinstance(body, Tag) else soup


STOP_PHRASES = [
    "Leave a Reply", "Post navigation",
    "Loading Comments", "Write a Comment",
]


def _iter_text_nodes(container):
    """Yield tags in reading order until stop phrases."""
    from bs4 import Tag

    for el in container.descendants:
        if isinstance(el, Tag):
            txt = el.get_text(" ", strip=True)
            if txt and any(p.lower() in txt.lower() for p in STOP_PHRASES):
                return
            yield el


def _normalize_whitespace(text: str) -> str:
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def scrape_to_markdown(url: str) -> str:
    """
    Scrape a web page and convert to clean markdown-like text.
    Returns the cleaned text suitable for chunking.
    """
    from bs4 import BeautifulSoup, Tag

    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    _strip_noise(soup)
    container = _find_main_container(soup)

    out = []
    seen = set()

    for el in _iter_text_nodes(container):
        if not isinstance(el, Tag):
            continue

        if el.name in {"h1", "h2", "h3", "h4"}:
            text = el.get_text(" ", strip=True)
            if text and text not in seen:
                level = {"h1": "#", "h2": "##", "h3": "###", "h4": "####"}[el.name]
                out.append(f"{level} {text}")
                out.append("")
                seen.add(text)

        elif el.name == "p":
            text = el.get_text(" ", strip=True)
            if text and text not in seen:
                out.append(text)
                out.append("")
                seen.add(text)

        elif el.name in {"ul", "ol"}:
            if el.find_parent(["ul", "ol"]) is not None:
                continue
            items = []
            for li in el.find_all("li", recursive=False):
                t = li.get_text(" ", strip=True)
                if t:
                    items.append(t)
            if items:
                for idx, t in enumerate(items, start=1):
                    prefix = "-" if el.name == "ul" else f"{idx}."
                    out.append(f"{prefix} {t}")
                out.append("")

        elif el.name == "blockquote":
            t = el.get_text(" ", strip=True)
            if t and t not in seen:
                out.append("> " + t)
                out.append("")
                seen.add(t)

    return _normalize_whitespace("\n".join(out))


# ═════════════════════════════════════════════════════════════════════════════
# S3 HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _s3_client():
    """Create an S3 client from environment variables."""
    import boto3

    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-2"),
    )
    return session.client("s3")


def list_s3_objects(bucket: str, prefix: str, pattern: str) -> list:
    """List S3 objects matching a regex pattern."""
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    regex = re.compile(pattern)
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if regex.search(obj["Key"]):
                objects.append(obj)
    return objects


def read_s3_text(bucket: str, key: str) -> str:
    """Read an S3 object as UTF-8 text."""
    s3 = _s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8", errors="replace")


def read_s3_bytes(bucket: str, key: str) -> bytes:
    """Read an S3 object as raw bytes (for PDFs)."""
    s3 = _s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def list_s3_subfolders(bucket: str, prefix: str) -> List[str]:
    """
    List immediate subfolders under an S3 prefix.
    e.g. prefix="unstructured-data/" → ["Grocery/", "Housing/", "Transit/"]
    """
    s3 = _s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    folders = []
    for cp in resp.get("CommonPrefixes", []):
        folders.append(cp["Prefix"])
    return sorted(folders)


def domain_from_s3_key(key: str, base_prefix: str) -> str:
    """
    Derive domain name from the first subfolder after base_prefix.

    Examples:
        key="unstructured-data/Grocery/file.pdf", base="unstructured-data/"
        → "GROCERY"

        key="unstructured-data/Housing/report.pdf", base="unstructured-data/"
        → "HOUSING"
    """
    relative = key[len(base_prefix):]          # "Grocery/file.pdf"
    parts = relative.split("/")
    if len(parts) >= 2 and parts[0]:
        return parts[0].upper().strip()        # "GROCERY"
    return "UNKNOWN"


# ═════════════════════════════════════════════════════════════════════════════
# EMBEDDING via Snowflake Cortex
# ═════════════════════════════════════════════════════════════════════════════

def _add_passage_prefix(text: str, model: str) -> str:
    """Prepend 'passage: ' for e5 models (required for correct calibration)."""
    return (E5_PASSAGE_PREFIX + text) if model in E5_MODELS else text


def embed_batch(cur, texts: List[str], model: str) -> List[list]:
    """Embed a batch of texts in one Snowflake round-trip using UNION ALL."""
    if not texts:
        return []

    parts = []
    for i, text in enumerate(texts):
        safe = _add_passage_prefix(text, model).replace("'", "''")[:2000]
        parts.append(
            f"SELECT {i} AS idx, "
            f"SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model}', '{safe}') AS vec"
        )

    sql = "\nUNION ALL\n".join(parts) + "\nORDER BY idx"
    cur.execute(sql)

    embeddings = []
    for _, vec in cur.fetchall():
        if isinstance(vec, str):
            vec = json.loads(vec)
        embeddings.append(vec)
    return embeddings


def embed_all_chunks(cur, chunks: List[str], model: str,
                     batch_size: int) -> List[list]:
    """
    Embed all chunks with progress bar and retry logic (3 retries per batch).
    """
    total = len(chunks)
    embeddings = []
    batches = [chunks[i:i + batch_size] for i in range(0, total, batch_size)]
    n_batches = len(batches)
    bar_width = 40
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
                _progress(b_idx, n_batches, total, len(embeddings),
                          bar_width, t0, suffix=f" retry {attempt} ({exc})")
                time.sleep(wait)

        embeddings.extend(batch_embs)
        _progress(b_idx + 1, n_batches, total, len(embeddings), bar_width, t0)

    print()
    return embeddings


def _progress(done_batches, total_batches, total_chunks, done_chunks,
              bar_width, t0, suffix=""):
    """Render an inline progress bar."""
    pct = done_batches / total_batches if total_batches else 1
    filled = int(bar_width * pct)
    bar = "█" * filled + "░" * (bar_width - filled)
    elapsed = time.time() - t0
    eta_str = ""
    if done_chunks > 0 and pct < 1:
        eta = elapsed / pct * (1 - pct)
        eta_str = f"  ETA {eta:.0f}s"
    sys.stdout.write(
        f"\r  [{bar}] {done_chunks}/{total_chunks} chunks "
        f"({pct * 100:.1f}%){eta_str}{suffix}   "
    )
    sys.stdout.flush()


# ═════════════════════════════════════════════════════════════════════════════
# INSERT to Snowflake  (temp-table workaround for VECTOR casting)
# ═════════════════════════════════════════════════════════════════════════════

def insert_chunks(cur, conn, domain: str, source_file: str,
                  chunks: List[str], embeddings: List[list],
                  batch_size: int) -> None:
    """
    Insert chunks with domain tag into the unified RAW_DOMAIN_CHUNKS table.
    Uses a temp table because Snowflake disallows VECTOR casting in VALUES.
    """
    tmp = f"{DEFAULT_DATABASE}.{DEFAULT_SCHEMA}.TMP_CHUNK_STAGE"
    total = len(chunks)
    done = 0

    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {tmp} (
            domain         VARCHAR,
            source_file    VARCHAR,
            chunk_index    NUMBER,
            chunk_text     VARCHAR,
            embedding_json VARCHAR
        );
    """)

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        rows = [
            (domain.upper(), source_file, start + i, chunk, json.dumps(emb))
            for i, (chunk, emb) in enumerate(
                zip(chunks[start:end], embeddings[start:end])
            )
        ]

        cur.executemany(
            f"INSERT INTO {tmp} "
            f"(domain, source_file, chunk_index, chunk_text, embedding_json) "
            f"VALUES (%s, %s, %s, %s, %s)",
            rows,
        )

        cur.execute(f"""
            INSERT INTO {FQN}
                (domain, source_file, chunk_index, chunk_text, chunk_embedding)
            SELECT
                domain, source_file, chunk_index, chunk_text,
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


# ═════════════════════════════════════════════════════════════════════════════
# DEDUP HELPER
# ═════════════════════════════════════════════════════════════════════════════

def get_loaded_sources(cur, domain: str) -> set:
    """Return set of source_file names already loaded for a domain."""
    try:
        cur.execute(
            f"SELECT DISTINCT source_file FROM {FQN} "
            f"WHERE domain = '{domain.upper()}';"
        )
        return {row[0] for row in cur.fetchall()}
    except Exception:
        return set()


# ═════════════════════════════════════════════════════════════════════════════
# COMMON LOAD HELPER  (chunk → embed → insert)
# ═════════════════════════════════════════════════════════════════════════════

def load_text_to_snowflake(
    cur, conn, text: str, domain: str, source_file: str,
    chunk_size: int, chunk_overlap: int, min_chars: int,
    embed_model: str, embed_batch: int, insert_batch: int,
) -> int:
    """
    Chunk a text, embed via Cortex, and insert into the unified table.
    Returns the number of chunks loaded.
    """
    chunks = [
        c for c in chunk_text(text, chunk_size, chunk_overlap, filename=source_file)
        if len(c) >= min_chars
    ]
    print(f"  {len(chunks)} chunk(s) [size={chunk_size}, overlap={chunk_overlap}]")

    if not chunks:
        print("  Skipping — no valid chunks.")
        return 0

    print(f"  Embedding ({embed_model}, batch={embed_batch})...")
    embeddings = embed_all_chunks(cur, chunks, embed_model, embed_batch)

    print(f"  Loading to Snowflake...")
    insert_chunks(cur, conn, domain, source_file, chunks, embeddings, insert_batch)

    print(f"  ✓ {len(chunks)} chunks loaded")
    return len(chunks)


# ═════════════════════════════════════════════════════════════════════════════
# COMMAND: load-s3  (text/CSV/MD/JSON files from S3)
# ═════════════════════════════════════════════════════════════════════════════

def cmd_load_s3(args):
    """Load text-based files from S3 into the unified table."""
    domain = args.domain.upper()
    bucket = args.s3_bucket

    print(f"\n{'='*60}")
    print(f"  LOAD-S3 — domain: {domain}")
    print(f"  Source : s3://{bucket}/{args.s3_prefix}")
    print(f"  Table  : {FQN}")
    print(f"{'='*60}")

    objects = list_s3_objects(bucket, args.s3_prefix, args.pattern)
    print(f"  Found {len(objects)} matching file(s).")

    if not objects:
        print("  Nothing to do.")
        return

    conn = sf_connect()
    cur = conn.cursor()

    try:
        ensure_schema(cur)
        ensure_table(cur)

        already_loaded = get_loaded_sources(cur, domain)
        if already_loaded:
            print(f"  Already loaded for {domain}: {len(already_loaded)} file(s)")

        total_chunks = 0
        skipped = 0

        for obj in objects:
            key = obj["Key"]
            if key in already_loaded:
                print(f"\n  ── {key}  ⏭ SKIP (already loaded)")
                skipped += 1
                continue

            print(f"\n  ── {key}")
            text = read_s3_text(bucket, key)

            if args.dry_run:
                preview_chunks = chunk_text(
                    text, args.chunk_size, args.chunk_overlap, filename=key
                )
                print(f"  {len(preview_chunks)} chunk(s) (dry run)")
                for i, c in enumerate(preview_chunks[:3]):
                    print(f"  chunk[{i}]: {c[:120]!r} …")
                continue

            n = load_text_to_snowflake(
                cur, conn, text, domain, key,
                args.chunk_size, args.chunk_overlap, args.min_chars,
                args.embed_model, args.embed_batch, args.insert_batch,
            )
            total_chunks += n

        _print_summary(cur, domain, total_chunks, skipped, args.dry_run)

    finally:
        cur.close()
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# COMMAND: load-pdf  (PDF files from S3)
# ═════════════════════════════════════════════════════════════════════════════

def cmd_load_pdf(args):
    """Load PDF files from S3 — extract text, chunk, embed, insert."""
    domain = args.domain.upper()
    bucket = args.s3_bucket

    print(f"\n{'='*60}")
    print(f"  LOAD-PDF — domain: {domain}")
    print(f"  Source : s3://{bucket}/{args.s3_prefix}")
    print(f"  Table  : {FQN}")
    print(f"{'='*60}")

    objects = list_s3_objects(bucket, args.s3_prefix, args.pattern)
    print(f"  Found {len(objects)} PDF file(s).")

    if not objects:
        print("  Nothing to do.")
        return

    conn = sf_connect()
    cur = conn.cursor()

    try:
        ensure_schema(cur)
        ensure_table(cur)

        already_loaded = get_loaded_sources(cur, domain)
        if already_loaded:
            print(f"  Already loaded for {domain}: {len(already_loaded)} file(s)")

        total_chunks = 0
        skipped = 0

        for obj in objects:
            key = obj["Key"]
            size_mb = obj["Size"] / (1024 * 1024)

            if key in already_loaded:
                print(f"\n  ── {key}  ⏭ SKIP (already loaded)")
                skipped += 1
                continue

            print(f"\n  ── {os.path.basename(key)}  ({size_mb:.1f} MB)")
            print(f"  Extracting text from PDF...")

            pdf_bytes = read_s3_bytes(bucket, key)
            text = extract_text_from_pdf_bytes(pdf_bytes)

            if not text.strip():
                print(f"  [skip] No extractable text (scanned/image PDF?).")
                continue

            print(f"  {len(text)} characters extracted")

            if args.dry_run:
                preview_chunks = chunk_text(
                    text, args.chunk_size, args.chunk_overlap, filename=key
                )
                print(f"  {len(preview_chunks)} chunk(s) (dry run)")
                for i, c in enumerate(preview_chunks[:3]):
                    print(f"  chunk[{i}]: {c[:120]!r} …")
                continue

            n = load_text_to_snowflake(
                cur, conn, text, domain, key,
                args.chunk_size, args.chunk_overlap, args.min_chars,
                args.embed_model, args.embed_batch, args.insert_batch,
            )
            total_chunks += n

        _print_summary(cur, domain, total_chunks, skipped, args.dry_run)

    finally:
        cur.close()
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# COMMAND: load-all  (auto-discover subfolders, derive domain, load everything)
# ═════════════════════════════════════════════════════════════════════════════

def cmd_load_all(args):
    """
    Auto-discover subfolders under the S3 prefix and load all files.

    S3 structure expected:
        s3://bucket/unstructured-data/
            Grocery/    → domain = GROCERY
            Housing/    → domain = HOUSING
            Transit/    → domain = TRANSIT

    Each subfolder's files (PDF, txt, md, csv, json) are chunked, embedded,
    and loaded into RAW_DOMAIN_CHUNKS_TEST with the folder name as DOMAIN.
    """
    bucket = args.s3_bucket
    base_prefix = args.s3_prefix
    if not base_prefix.endswith("/"):
        base_prefix += "/"

    print(f"\n{'='*65}")
    print(f"  LOAD-ALL — auto-discover domains from S3 subfolders")
    print(f"  Source : s3://{bucket}/{base_prefix}")
    print(f"  Table  : {FQN}")
    print(f"{'='*65}")

    # ── Discover subfolders (each = one domain) ──────────────────────────
    subfolders = list_s3_subfolders(bucket, base_prefix)
    if not subfolders:
        print(f"  No subfolders found under s3://{bucket}/{base_prefix}")
        return

    # Derive domain names from folder names
    domain_map = {}
    for folder in subfolders:
        # folder = "unstructured-data/Grocery/"
        # strip base prefix → "Grocery/"  → "GROCERY"
        relative = folder[len(base_prefix):].rstrip("/")
        domain_name = relative.upper().strip()
        if domain_name:
            domain_map[domain_name] = folder

    print(f"\n  Discovered {len(domain_map)} domain(s):")
    for domain, folder in domain_map.items():
        print(f"    {domain:<15} ← s3://{bucket}/{folder}")

    # ── Optional: filter to a single domain if --domain was provided ─────
    if args.domain:
        filter_domain = args.domain.upper()
        if filter_domain not in domain_map:
            print(f"\n  ⚠ Domain '{filter_domain}' not found in subfolders.")
            print(f"  Available: {', '.join(domain_map.keys())}")
            return
        domain_map = {filter_domain: domain_map[filter_domain]}
        print(f"\n  Filtered to domain: {filter_domain}")

    print()

    # ── Connect to Snowflake ─────────────────────────────────────────────
    conn = sf_connect()
    cur = conn.cursor()

    try:
        ensure_schema(cur)
        ensure_table(cur)

        grand_total_chunks = 0
        grand_total_skipped = 0
        domain_summaries = {}

        for domain, folder_prefix in domain_map.items():
            print(f"\n{'─'*65}")
            print(f"  DOMAIN: {domain}")
            print(f"  Scanning s3://{bucket}/{folder_prefix}")
            print(f"{'─'*65}")

            # List ALL files in this domain folder (PDFs + text)
            all_objects = list_s3_objects(
                bucket, folder_prefix, r".*\.(pdf|txt|md|csv|json)$"
            )
            if not all_objects:
                print(f"  No files found. Skipping.")
                domain_summaries[domain] = {"chunks": 0, "files": 0, "skipped": 0}
                continue

            # Separate PDFs from text files
            pdf_objects = [o for o in all_objects if o["Key"].lower().endswith(".pdf")]
            txt_objects = [o for o in all_objects if not o["Key"].lower().endswith(".pdf")]

            print(f"  Found {len(pdf_objects)} PDF(s) + {len(txt_objects)} text file(s)")

            # Check what's already loaded for this domain
            already_loaded = get_loaded_sources(cur, domain)
            if already_loaded:
                print(f"  Already loaded: {len(already_loaded)} source(s)")

            domain_chunks = 0
            domain_skipped = 0

            # ── Process text files ───────────────────────────────────────
            for obj in txt_objects:
                key = obj["Key"]
                if key in already_loaded:
                    print(f"\n  ── {os.path.basename(key)}  ⏭ SKIP")
                    domain_skipped += 1
                    continue

                print(f"\n  ── {os.path.basename(key)}  (text)")
                text = read_s3_text(bucket, key)

                if args.dry_run:
                    preview = chunk_text(
                        text, args.chunk_size, args.chunk_overlap, filename=key
                    )
                    print(f"  {len(preview)} chunk(s) (dry run)")
                    continue

                n = load_text_to_snowflake(
                    cur, conn, text, domain, key,
                    args.chunk_size, args.chunk_overlap, args.min_chars,
                    args.embed_model, args.embed_batch, args.insert_batch,
                )
                domain_chunks += n

            # ── Process PDF files ────────────────────────────────────────
            for obj in pdf_objects:
                key = obj["Key"]
                size_mb = obj["Size"] / (1024 * 1024)

                if key in already_loaded:
                    print(f"\n  ── {os.path.basename(key)}  ⏭ SKIP")
                    domain_skipped += 1
                    continue

                print(f"\n  ── {os.path.basename(key)}  ({size_mb:.1f} MB, PDF)")
                print(f"  Extracting text...")

                pdf_bytes = read_s3_bytes(bucket, key)
                text = extract_text_from_pdf_bytes(pdf_bytes)

                if not text.strip():
                    print(f"  [skip] No extractable text (scanned/image PDF?).")
                    continue

                print(f"  {len(text)} characters extracted")

                if args.dry_run:
                    preview = chunk_text(
                        text, args.chunk_size, args.chunk_overlap, filename=key
                    )
                    print(f"  {len(preview)} chunk(s) (dry run)")
                    continue

                n = load_text_to_snowflake(
                    cur, conn, text, domain, key,
                    args.chunk_size, args.chunk_overlap, args.min_chars,
                    args.embed_model, args.embed_batch, args.insert_batch,
                )
                domain_chunks += n

            grand_total_chunks += domain_chunks
            grand_total_skipped += domain_skipped
            domain_summaries[domain] = {
                "chunks": domain_chunks,
                "files": len(all_objects),
                "skipped": domain_skipped,
            }

        # ── Grand summary ────────────────────────────────────────────────
        print(f"\n\n{'═'*65}")
        print(f"  ✅  LOAD-ALL COMPLETE")
        print(f"  Table : {FQN}")
        print(f"{'═'*65}")
        print(f"\n  {'Domain':<15} {'Files':<8} {'Chunks':<10} {'Skipped'}")
        print(f"  {'-'*15} {'-'*8} {'-'*10} {'-'*8}")
        for domain, info in domain_summaries.items():
            print(f"  {domain:<15} {info['files']:<8} "
                  f"{info['chunks']:<10} {info['skipped']}")
        print(f"  {'-'*15} {'-'*8} {'-'*10} {'-'*8}")
        print(f"  {'TOTAL':<15} "
              f"{sum(d['files'] for d in domain_summaries.values()):<8} "
              f"{grand_total_chunks:<10} {grand_total_skipped}")

        if not args.dry_run:
            cur.execute(f"SELECT COUNT(*) FROM {FQN};")
            total_rows = cur.fetchone()[0]

            cur.execute(
                f"SELECT domain, COUNT(*) FROM {FQN} "
                f"GROUP BY domain ORDER BY domain;"
            )
            print(f"\n  Table totals ({FQN}):")
            for row in cur.fetchall():
                print(f"    {row[0]:<15} {row[1]} chunks")
            print(f"    {'─'*30}")
            print(f"    {'ALL':<15} {total_rows} chunks")
        else:
            print(f"\n  (Dry run — nothing was written to Snowflake)")

        print(f"{'═'*65}")

    finally:
        cur.close()
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# COMMAND: scrape  (web page → chunk → embed → load)
# ═════════════════════════════════════════════════════════════════════════════

def cmd_scrape(args):
    """Scrape a web page, extract article text, chunk & load to Snowflake."""
    domain = args.domain.upper()
    url = args.url

    print(f"\n{'='*60}")
    print(f"  SCRAPE — domain: {domain}")
    print(f"  URL   : {url}")
    print(f"  Table : {FQN}")
    print(f"{'='*60}")

    print(f"  Fetching page...")
    text = scrape_to_markdown(url)
    print(f"  Extracted {len(text)} characters of clean text")

    if not text.strip():
        print("  No usable text found on the page.")
        return

    # Optionally save locally
    if args.save_local:
        outdir = Path(args.outdir)
        outdir.mkdir(parents=True, exist_ok=True)
        md_path = outdir / "scraped_text.md"
        md_path.write_text(text + "\n", encoding="utf-8")
        print(f"  Saved locally: {md_path}")

    if args.dry_run:
        chunks = chunk_text(text, args.chunk_size, args.chunk_overlap, filename=url)
        print(f"  {len(chunks)} chunk(s) (dry run)")
        for i, c in enumerate(chunks[:5]):
            print(f"  chunk[{i}]: {c[:120]!r} …")
        return

    conn = sf_connect()
    cur = conn.cursor()

    try:
        ensure_schema(cur)
        ensure_table(cur)

        # Use the URL as source_file identifier
        source_name = url[:200]

        n = load_text_to_snowflake(
            cur, conn, text, domain, source_name,
            args.chunk_size, args.chunk_overlap, args.min_chars,
            args.embed_model, args.embed_batch, args.insert_batch,
        )
        _print_summary(cur, domain, n, 0, False)

    finally:
        cur.close()
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# COMMAND: download  (PDF → local text extraction, no Snowflake)
# ═════════════════════════════════════════════════════════════════════════════

def cmd_download(args):
    """Download a PDF from a URL, extract text, save locally."""
    outdir = Path(args.outdir) / args.domain
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  DOWNLOAD — domain: {args.domain}")
    print(f"{'='*60}")

    # Download
    filename = args.url.split("/")[-1]
    if not filename.endswith(".pdf"):
        filename += ".pdf"
    filename = http_requests.utils.unquote(filename)
    filename = re.sub(r'[^\w\-.]', '_', filename)
    pdf_path = outdir / filename

    print(f"  Downloading: {args.url}")
    resp = http_requests.get(args.url, timeout=60, stream=True)
    resp.raise_for_status()
    with open(pdf_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    size_kb = pdf_path.stat().st_size / 1024
    print(f"  Saved: {pdf_path} ({size_kb:.1f} KB)")

    # Extract text
    text = extract_text_from_pdf_file(pdf_path)
    txt_path = pdf_path.with_suffix('.txt')
    txt_path.write_text(text, encoding='utf-8')
    print(f"  Text saved: {txt_path}")

    print(f"\n  ✓ Done. Text ready for loading.")
    print(f"  Next step: python neighbourwise_rag_unified.py load-s3 "
          f"--s3-prefix <upload-to-s3-first> --domain {args.domain}")
    print(f"  Or load locally extracted .txt files with load-pdf if needed.")


# ═════════════════════════════════════════════════════════════════════════════
# COMMAND: search  (hybrid semantic search)
# ═════════════════════════════════════════════════════════════════════════════

def embed_query(cur, query: str, model: str) -> list:
    """Embed a query. Prepends 'query: ' for e5 models."""
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
    """Extract meaningful words (4+ chars, no stopwords) for keyword boosting."""
    stopwords = {
        "what", "who", "when", "where", "how", "which", "does", "did",
        "the", "and", "for", "are", "was", "were", "from", "with", "that",
        "this", "have", "has", "had", "tell", "about", "give", "list", "find",
        "boston", "cambridge", "somerville", "neighborhood", "area", "city",
    }
    words = re.findall(r"[a-zA-Z]{4,}", query.lower())
    return [w for w in words if w not in stopwords]


def search_chunks(cur, domain: str, query_vector: list, top_k: int,
                  raw_query: str = "",
                  source_filter: Optional[str] = None,
                  min_similarity: float = 0.0) -> list:
    """
    Hybrid search: 65% vector cosine similarity + 35% keyword match.
    Use domain='all' to search across all domains.
    """
    vec_json = json.dumps(query_vector)

    domain_clause = (
        f"AND domain = '{domain.upper()}'"
        if domain.lower() != 'all' else ""
    )
    source_clause = (
        f"AND source_file ILIKE '%{source_filter}%'"
        if source_filter else ""
    )

    terms = _keyword_terms(raw_query) if raw_query else []
    n_terms = len(terms) if terms else 1
    kw_parts = (
        " + ".join(
            [f"IFF(LOWER(chunk_text) ILIKE '%{t}%', 1, 0)" for t in terms]
        )
        if terms else "0"
    )

    sql = f"""
        WITH base AS (
            SELECT
                chunk_id, domain, source_file, chunk_index, chunk_text,
                VECTOR_COSINE_SIMILARITY(
                    chunk_embedding,
                    PARSE_JSON('{vec_json}')::VECTOR(FLOAT, 768)
                ) AS vec_score,
                ({kw_parts}) AS kw_hits,
                ({kw_parts}) / {n_terms}.0 AS kw_score
            FROM {FQN}
            WHERE 1=1 {domain_clause} {source_clause}
        )
        SELECT
            chunk_id, domain, source_file, chunk_index, chunk_text,
            vec_score, kw_score,
            ROUND(kw_hits) AS keyword_matches,
            (vec_score * 0.65 + kw_score * 0.35) AS similarity
        FROM base
        WHERE vec_score >= {min_similarity}
        ORDER BY similarity DESC
        LIMIT {top_k};
    """
    cur.execute(sql)
    columns = [col[0].lower() for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


# ── Search display helpers ─────────────────────────────────────────────────

SCORE_BARS = {
    0.90: "●●●●● excellent",
    0.80: "●●●●○ good",
    0.70: "●●●○○ fair",
    0.60: "●●○○○ weak",
    0.00: "●○○○○ poor",
}


def _score_bar(sim: float) -> str:
    for threshold, label in sorted(SCORE_BARS.items(), reverse=True):
        if sim >= threshold:
            return label
    return "○○○○○ poor"


def print_results(results: list, query: str, top_k: int) -> None:
    print(f"\n{'═'*65}")
    print(f"  Query : {query}")
    print(f"  Found : {len(results)} result(s)  (top {top_k} requested)")
    print(f"{'═'*65}\n")

    if not results:
        print("  No matching chunks found. Try a different query or broader domain.")
        return

    for rank, r in enumerate(results, 1):
        sim = float(r["similarity"])
        vec_s = float(r.get("vec_score", sim))
        kw_s = float(r.get("kw_score", 0))
        kw_hits = int(float(r.get("keyword_matches", 0)))
        domain = r.get("domain", "?")
        bar = _score_bar(sim)
        text = r["chunk_text"]

        preview = textwrap.fill(
            text[:400] + ("…" if len(text) > 400 else ""),
            width=68, initial_indent="  ", subsequent_indent="  ",
        )

        print(f"  #{rank}  {bar}  hybrid: {sim:.4f}  "
              f"(vec={vec_s:.4f}  kw={kw_s:.2f}  hits={kw_hits})")
        print(f"  Domain: {domain}  Source: {r['source_file']}  "
              f"chunk: {r['chunk_index']}")
        print(f"  {'─'*56}")
        print(preview)
        print()


def save_csv(results: list, path: str, query: str) -> None:
    """Save search results to CSV."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "similarity", "domain", "chunk_id",
                         "source_file", "chunk_index", "chunk_text",
                         "query", "retrieved_at"])
        ts = datetime.utcnow().isoformat()
        for rank, r in enumerate(results, 1):
            writer.writerow([
                rank,
                f"{float(r['similarity']):.6f}",
                r.get("domain", ""),
                r["chunk_id"],
                r["source_file"],
                r["chunk_index"],
                r["chunk_text"],
                query,
                ts,
            ])
    print(f"\n  ✓ Results saved to: {path}")


def cmd_search(args):
    """Hybrid semantic search over the unified table."""
    domain = args.domain
    model = args.embed_model

    conn = sf_connect()
    cur = conn.cursor()

    try:
        if args.query:
            # ── Single query mode ───────────────────────────────────────
            print(f"\n  Embedding query ({model})...", end="", flush=True)
            vec = embed_query(cur, args.query, model)
            print(" done.")

            results = search_chunks(
                cur, domain, vec, args.top_k,
                raw_query=args.query,
                source_filter=args.source,
                min_similarity=args.min_sim,
            )
            print_results(results, args.query, args.top_k)

            if args.output:
                save_csv(results, args.output, args.query)

        else:
            # ── Interactive REPL mode ───────────────────────────────────
            domain_label = (
                domain.upper() if domain.lower() != 'all' else 'ALL DOMAINS'
            )
            print(f"\n{'═'*65}")
            print(f"  NeighbourWise Chunk Search — {domain_label}")
            print(f"  Table : {FQN}")
            print(f"  Model : {model}   Top-K : {args.top_k}")
            print(f"  Type 'exit' or Ctrl-C to quit")
            print(f"{'═'*65}\n")

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
                        raw_query=query,
                        source_filter=args.source,
                        min_similarity=args.min_sim,
                    )
                    print_results(results, query, args.top_k)

                    if args.output:
                        save_csv(results, args.output, query)

                except Exception as exc:
                    print(f"\n  ⚠ Error: {exc}\n")

    finally:
        cur.close()
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# SUMMARY HELPER
# ═════════════════════════════════════════════════════════════════════════════

def _print_summary(cur, domain: str, total_chunks: int,
                   skipped: int, dry_run: bool) -> None:
    """Print a final summary after a load operation."""
    if dry_run:
        print(f"\n  (Dry run — nothing was written to Snowflake)")
        return

    cur.execute(
        f"SELECT COUNT(*) FROM {FQN} WHERE domain = '{domain.upper()}';"
    )
    domain_rows = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {FQN};")
    total_rows = cur.fetchone()[0]

    print(f"\n{'='*60}")
    print(f"  ✅ Load complete")
    print(f"  Table               : {FQN}")
    print(f"  New chunks this run : {total_chunks}")
    print(f"  Skipped (dedup)     : {skipped}")
    print(f"  Domain '{domain}' total : {domain_rows} chunks")
    print(f"  All domains total   : {total_rows} chunks")
    print(f"{'='*60}")


# ═════════════════════════════════════════════════════════════════════════════
# COMMAND: merge  (test table → production table)
# ═════════════════════════════════════════════════════════════════════════════

def cmd_merge(args):
    """
    Merge rows from RAW_DOMAIN_CHUNKS_TEST into RAW_DOMAIN_CHUNKS (production).
    Optionally filter by --domain. Uses INSERT ... SELECT to avoid duplicates
    by checking (domain, source_file, chunk_index) combinations.
    """
    domain_filter = args.domain

    print(f"\n{'='*65}")
    print(f"  MERGE — test → production")
    print(f"  From : {FQN}")
    print(f"  To   : {PROD_FQN}")
    if domain_filter:
        print(f"  Domain filter: {domain_filter.upper()}")
    print(f"{'='*65}")

    conn = sf_connect()
    cur = conn.cursor()

    try:
        # Ensure production table exists with the DOMAIN column
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PROD_FQN} (
                chunk_id        NUMBER AUTOINCREMENT PRIMARY KEY,
                domain          VARCHAR,
                source_file     VARCHAR,
                chunk_index     NUMBER,
                chunk_text      VARCHAR,
                chunk_embedding VECTOR(FLOAT, 768),
                loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """)

        # Count rows to merge
        domain_clause = (
            f"WHERE domain = '{domain_filter.upper()}'"
            if domain_filter else ""
        )
        cur.execute(f"SELECT COUNT(*) FROM {FQN} {domain_clause};")
        test_rows = cur.fetchone()[0]
        print(f"\n  Rows in test table: {test_rows}")

        if test_rows == 0:
            print("  Nothing to merge.")
            return

        if args.dry_run:
            # Show what would be merged
            cur.execute(
                f"SELECT domain, source_file, COUNT(*) AS chunks "
                f"FROM {FQN} {domain_clause} "
                f"GROUP BY domain, source_file ORDER BY domain, source_file;"
            )
            print(f"\n  {'Domain':<15} {'Source File':<50} Chunks")
            print(f"  {'-'*15} {'-'*50} {'-'*6}")
            for row in cur.fetchall():
                print(f"  {row[0]:<15} {row[1]:<50} {row[2]}")
            print(f"\n  (Dry run — nothing was merged)")
            return

        # Insert only rows that don't already exist in production
        # (dedup on domain + source_file + chunk_index)
        domain_test_clause = (
            f"AND t.domain = '{domain_filter.upper()}'"
            if domain_filter else ""
        )

        merge_sql = f"""
            INSERT INTO {PROD_FQN}
                (domain, source_file, chunk_index, chunk_text, chunk_embedding)
            SELECT
                t.domain, t.source_file, t.chunk_index,
                t.chunk_text, t.chunk_embedding
            FROM {FQN} t
            WHERE 1=1 {domain_test_clause}
              AND NOT EXISTS (
                  SELECT 1 FROM {PROD_FQN} p
                  WHERE p.domain      = t.domain
                    AND p.source_file  = t.source_file
                    AND p.chunk_index  = t.chunk_index
              );
        """
        cur.execute(merge_sql)
        merged = cur.rowcount
        conn.commit()

        # Optionally clean up test table after merge
        if args.cleanup:
            cur.execute(f"DELETE FROM {FQN} {domain_clause};")
            conn.commit()
            print(f"  Cleaned up test table ({test_rows} rows removed)")

        cur.execute(f"SELECT COUNT(*) FROM {PROD_FQN};")
        prod_total = cur.fetchone()[0]

        print(f"\n  ✅ Merge complete")
        print(f"  Rows merged (new)   : {merged}")
        print(f"  Rows skipped (dedup): {test_rows - merged}")
        print(f"  Production total    : {prod_total} chunks")

    finally:
        cur.close()
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def _add_common_args(parser):
    """Add arguments shared across load commands."""
    parser.add_argument("--domain", required=True,
                        help="Domain tag (crime, grocery, healthcare, housing, etc.)")
    parser.add_argument("--embed-model", default=DEFAULT_EMBED_MODEL,
                        help=f"Embedding model (default: {DEFAULT_EMBED_MODEL})")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Characters per chunk (default: {DEFAULT_CHUNK_SIZE})")
    parser.add_argument("--chunk-overlap", type=int, default=DEFAULT_CHUNK_OVERLAP,
                        help=f"Overlap between chunks (default: {DEFAULT_CHUNK_OVERLAP})")
    parser.add_argument("--embed-batch", type=int, default=DEFAULT_EMBED_BATCH,
                        help=f"Chunks per embedding call (default: {DEFAULT_EMBED_BATCH})")
    parser.add_argument("--insert-batch", type=int, default=DEFAULT_INSERT_BATCH,
                        help=f"Rows per insert batch (default: {DEFAULT_INSERT_BATCH})")
    parser.add_argument("--min-chars", type=int, default=DEFAULT_MIN_CHARS,
                        help=f"Min chunk length to keep (default: {DEFAULT_MIN_CHARS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview chunks without writing to Snowflake")


def main():
    parser = argparse.ArgumentParser(
        description="NeighbourWise Unified RAG Pipeline — load, scrape, search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Examples:
          python neighbourwise_rag_unified.py load-all                          # auto-discover & load everything
          python neighbourwise_rag_unified.py load-all --dry-run                # preview without writing
          python neighbourwise_rag_unified.py load-s3  --s3-prefix proximity/grocery/ --domain grocery
          python neighbourwise_rag_unified.py load-pdf --s3-prefix unstructured-data/ --domain housing
          python neighbourwise_rag_unified.py scrape   --url "https://..." --domain grocery
          python neighbourwise_rag_unified.py search   --domain grocery --query "supermarkets Dorchester"
          python neighbourwise_rag_unified.py search   --domain all --query "programs in Roxbury"
          python neighbourwise_rag_unified.py download --url "https://..." --domain crime --outdir ./rag_docs
          python neighbourwise_rag_unified.py merge    --domain grocery          # merge tested chunks to production
          python neighbourwise_rag_unified.py merge    --domain grocery --cleanup # merge + clear test table
        """),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── load-all (★ primary command) ──────────────────────────────────────
    la = subparsers.add_parser(
        "load-all",
        help="Auto-discover S3 subfolders → derive domain from folder name → load all"
    )
    la.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET)
    la.add_argument("--s3-prefix", default="unstructured-data/",
                    help="Base S3 prefix containing domain subfolders "
                         "(default: unstructured-data/)")
    _add_common_args(la)
    # load-all doesn't need --domain (auto-derived), so remove it and re-add
    # Actually _add_common_args adds --domain as required; we need to patch it
    # We'll handle this differently — make --domain optional for load-all
    for action in la._actions:
        if hasattr(action, 'dest') and action.dest == 'domain':
            action.required = False
            action.default = None
            action.help = ("Override: load only this domain subfolder "
                           "(omit to load all discovered domains)")
            break

    # ── load-s3 ───────────────────────────────────────────────────────────
    ls = subparsers.add_parser(
        "load-s3", help="Load text/CSV/MD/JSON files from S3"
    )
    ls.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET)
    ls.add_argument("--s3-prefix", required=True,
                    help="S3 key prefix (e.g. proximity/grocery/)")
    ls.add_argument("--pattern", default=r".*\.(txt|md|json|csv)$",
                    help="Regex to filter S3 keys (default: txt/md/json/csv)")
    _add_common_args(ls)

    # ── load-pdf ──────────────────────────────────────────────────────────
    lp = subparsers.add_parser(
        "load-pdf", help="Load PDF files from S3 (extract → chunk → embed)"
    )
    lp.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET)
    lp.add_argument("--s3-prefix", required=True,
                    help="S3 key prefix (e.g. unstructured-data/)")
    lp.add_argument("--pattern", default=r".*\.pdf$",
                    help="Regex to filter S3 keys (default: *.pdf)")
    _add_common_args(lp)

    # ── scrape ────────────────────────────────────────────────────────────
    sc = subparsers.add_parser(
        "scrape", help="Scrape web page → chunk → embed → load"
    )
    sc.add_argument("--url", required=True,
                    help="URL of the web page to scrape")
    sc.add_argument("--save-local", action="store_true",
                    help="Also save scraped text as .md locally")
    sc.add_argument("--outdir", default="./scraped",
                    help="Directory for local save (default: ./scraped)")
    _add_common_args(sc)

    # ── search ────────────────────────────────────────────────────────────
    sr = subparsers.add_parser(
        "search", help="Hybrid semantic search over loaded chunks"
    )
    sr.add_argument("--query", default=None,
                    help="Query string (omit for interactive REPL)")
    sr.add_argument("--domain", required=True,
                    help="Domain to search (or 'all' for cross-domain)")
    sr.add_argument("--top-k", type=int, default=DEFAULT_TOP_K,
                    help=f"Number of results (default: {DEFAULT_TOP_K})")
    sr.add_argument("--min-sim", type=float, default=0.0,
                    help="Minimum similarity threshold (default: 0.0)")
    sr.add_argument("--source", default=None,
                    help="Filter to specific source file (substring match)")
    sr.add_argument("--output", default=None,
                    help="Optional CSV file path to save results")
    sr.add_argument("--embed-model", default=DEFAULT_EMBED_MODEL)

    # ── merge ──────────────────────────────────────────────────────────
    mg = subparsers.add_parser(
        "merge", help="Merge test table into production RAW_DOMAIN_CHUNKS"
    )
    mg.add_argument("--domain", default=None,
                    help="Only merge a specific domain (omit for all)")
    mg.add_argument("--cleanup", action="store_true",
                    help="Delete merged rows from test table after merge")
    mg.add_argument("--dry-run", action="store_true",
                    help="Show what would be merged without writing")

    # ── download ──────────────────────────────────────────────────────────
    dl = subparsers.add_parser(
        "download", help="Download PDF from URL and extract text locally"
    )
    dl.add_argument("--url", required=True,
                    help="URL of the PDF to download")
    dl.add_argument("--domain", required=True,
                    help="Domain name (used for output folder organization)")
    dl.add_argument("--outdir", default="./rag_docs",
                    help="Base output directory (default: ./rag_docs)")

    # ── Dispatch ──────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.command == "load-all":
        cmd_load_all(args)
    elif args.command == "load-s3":
        cmd_load_s3(args)
    elif args.command == "load-pdf":
        cmd_load_pdf(args)
    elif args.command == "scrape":
        cmd_scrape(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "download":
        cmd_download(args)
    elif args.command == "merge":
        cmd_merge(args)


if __name__ == "__main__":
    main()