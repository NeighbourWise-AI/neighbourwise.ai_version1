import argparse
import json
import os
import re
import sys
import time
from typing import List, Optional

import boto3
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_S3_BUCKET     = "neighborwise-ai-s3-bucket"
DEFAULT_S3_PREFIX     = "proximity/grocery/"
DEFAULT_DATABASE      = "NEIGHBOURWISE_DOMAINS"   # target DB (visible in screenshot)
DEFAULT_SCHEMA        = "RAW_UNSTRUCTURED"         # new schema to auto-create
DEFAULT_TABLE         = "RAW_PROXIMITY_GROCERY_CHUNKS"
DEFAULT_EMBED_MODEL   = "e5-base-v2"
DEFAULT_CHUNK_SIZE    = 1000   # characters per chunk
DEFAULT_CHUNK_OVERLAP = 200    # overlap between chunks
DEFAULT_EMBED_BATCH   = 50     # chunks embedded per single SQL round-trip
DEFAULT_INSERT_BATCH  = 200    # rows inserted per executemany call
DEFAULT_MIN_CHARS     = 150    # discard chunks shorter than this (noise)

# ── Snowflake connection ───────────────────────────────────────────────────────

def sf_connect(database: str, schema: str):
    """
    Connect to Snowflake.
    database / schema are passed explicitly so the script controls them
    independently of whatever SNOWFLAKE_DATABASE / SNOWFLAKE_SCHEMA are set
    to in .env (those may still point to INTERMEDIATE or another schema).
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
        schema=schema,
        role=os.environ.get("SNOWFLAKE_ROLE"),
        # Increase network timeout to avoid drops on large embed batches
        network_timeout=120,
        login_timeout=60,
    )

# ── Schema + Table DDL ────────────────────────────────────────────────────────

def ensure_schema(cur, database: str, schema: str) -> None:
    """Create RAW_UNSTRUCTURED schema inside NEIGHBOURWISE_DOMAINS if missing."""
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema};")
    cur.execute(f"USE SCHEMA {database}.{schema};")
    print(f"  Schema {database}.{schema} is ready.")


def ensure_chunks_table(cur, database: str, schema: str, table_name: str) -> None:
    """Create the chunks table using a fully-qualified name — no ambiguity."""
    fqn = f"{database}.{schema}.{table_name}"
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            chunk_id        NUMBER AUTOINCREMENT PRIMARY KEY,
            source_file     VARCHAR,
            chunk_index     NUMBER,
            chunk_text      VARCHAR,
            chunk_embedding VECTOR(FLOAT, 768),   -- matches snowflake-arctic-embed-m
            loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
    )
    print(f"  Table {fqn} is ready.")

# ── Text chunking ─────────────────────────────────────────────────────────────

def _char_chunk(text: str, chunk_size: int, overlap: int) -> List[str]:
    """
    Pure character-window chunking with whitespace-aware splitting.
    Used as a fallback for sections that exceed max chunk size.
    """
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        if end < len(text):
            while end > start and not text[end].isspace():
                end -= 1
            if end == start:
                end = start + chunk_size
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end - overlap
    return chunks


def chunk_text(text, chunk_size, overlap, filename=""):
    """
    Hybrid chunker: structural for markdown, character-window for everything else.

    Markdown: splits on heading boundaries (## / ###) so each store section
    gets its own chunk starting with its heading. Sections longer than
    chunk_size are sub-chunked with _char_chunk, with the heading re-prepended.

    Non-markdown (CSV, JSON, etc.): plain character-window chunking as before.
    """
    import re
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
                m       = HEADING_CAP.match(section)
                heading = m.group(1).strip() if m else ""
                for j, sub in enumerate(_char_chunk(section, chunk_size, overlap)):
                    if j > 0 and heading and not sub.startswith("#"):
                        chunks.append(heading + " (cont.)\n" + sub)
                    else:
                        chunks.append(sub)
        return chunks

    return _char_chunk(text, chunk_size, overlap)

# ── S3 helpers ────────────────────────────────────────────────────────────────

def _s3_client(aws_key_id, aws_secret_key, aws_region):
    session = boto3.Session(
        aws_access_key_id=aws_key_id,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region,
    )
    return session.client("s3")


def list_s3_objects(bucket, prefix, pattern, aws_key_id, aws_secret_key, aws_region):
    s3 = _s3_client(aws_key_id, aws_secret_key, aws_region)
    paginator = s3.get_paginator("list_objects_v2")
    regex = re.compile(pattern)
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if regex.search(obj["Key"]):
                objects.append(obj)
    return objects


def read_s3_object(bucket, key, aws_key_id, aws_secret_key, aws_region) -> str:
    s3 = _s3_client(aws_key_id, aws_secret_key, aws_region)
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8", errors="replace")

# ── Batch embedding via Snowflake Cortex ──────────────────────────────────────

# e5-base-v2 is a SYMMETRIC bi-encoder trained with explicit prefixes.
# "passage: " must be prepended to stored chunks at ingest time.
# "query: "   must be prepended to user queries at search time.
# Snowflake Cortex passes the full string to the model unchanged.
E5_PASSAGE_PREFIX = "passage: "
E5_QUERY_PREFIX   = "query: "
E5_MODELS         = {"e5-base-v2", "e5-large-v2"}


def _add_passage_prefix(text: str, model: str) -> str:
    return (E5_PASSAGE_PREFIX + text) if model in E5_MODELS else text


def embed_batch(cur, texts: List[str], model: str) -> List[list]:
    """
    Embed a batch of texts in ONE round-trip using a multi-row SELECT + UNION ALL.
    Prepends "passage: " for e5 models so ingest and query vectors are aligned.
    Returns a list of 768-dim float vectors in the same order as `texts`.
    """
    if not texts:
        return []

    parts = []
    for i, text in enumerate(texts):
        safe = _add_passage_prefix(text, model).replace("'", "''")
        safe = safe[:2000]   # Cortex 512-token hard limit (~2000 chars)
        parts.append(
            f"SELECT {i} AS idx, "
            f"SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model}', '{safe}') AS vec"
        )

    sql = "\nUNION ALL\n".join(parts) + "\nORDER BY idx"
    cur.execute(sql)
    rows = cur.fetchall()          # [(0, vec), (1, vec), ...]

    embeddings = []
    for _, vec in rows:
        if isinstance(vec, str):
            vec = json.loads(vec)
        embeddings.append(vec)
    return embeddings


def embed_chunks_in_batches(cur, chunks: List[str], model: str,
                             batch_size: int) -> List[list]:
    """
    Drives embed_batch() across all chunks with a live progress bar.
    Retries each batch up to 3 times on transient errors.
    """
    total      = len(chunks)
    embeddings = []
    batches    = [chunks[i:i+batch_size] for i in range(0, total, batch_size)]
    n_batches  = len(batches)

    bar_width = 40
    t0 = time.time()

    for b_idx, batch in enumerate(batches):
        # ── retry loop ──────────────────────────────────────────────────────
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

        # ── progress bar ────────────────────────────────────────────────────
        _progress(b_idx + 1, n_batches, total, len(embeddings), bar_width, t0)

    print()   # newline after progress bar
    return embeddings


def _progress(done_batches, total_batches, total_chunks, done_chunks,
              bar_width, t0, suffix=""):
    pct      = done_batches / total_batches if total_batches else 1
    filled   = int(bar_width * pct)
    bar      = "█" * filled + "░" * (bar_width - filled)
    elapsed  = time.time() - t0
    eta_str  = ""
    if done_chunks > 0 and pct < 1:
        eta = elapsed / pct * (1 - pct)
        eta_str = f"  ETA {eta:.0f}s"
    sys.stdout.write(
        f"\r  [{bar}] {done_chunks}/{total_chunks} chunks "
        f"({pct*100:.1f}%){eta_str}{suffix}   "
    )
    sys.stdout.flush()

def insert_chunks(cur, conn, database: str, schema: str, table_name: str,
                  source_file: str,
                  chunks: List[str],
                  embeddings: List[list],
                  batch_size: int) -> None:
    """
    Snowflake does NOT allow VECTOR casting inside a VALUES clause
    (error 002013: Invalid data type [VECTOR(FLOAT, 768)] in VALUES clause).

    Workaround: stage each batch into a plain-VARCHAR temp table, then
    INSERT INTO ... SELECT with the PARSE_JSON()::VECTOR cast in the SELECT —
    Snowflake allows the cast there.
    """
    fqn   = f"{database}.{schema}.{table_name}"
    tmp   = f"{database}.{schema}.TMP_CHUNK_STAGE"
    total = len(chunks)
    done  = 0

    # Create a lightweight temporary staging table (plain VARCHAR for embedding)
    cur.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {tmp} (
            source_file    VARCHAR,
            chunk_index    NUMBER,
            chunk_text     VARCHAR,
            embedding_json VARCHAR
        );
        """
    )

    for start in range(0, total, batch_size):
        end  = min(start + batch_size, total)
        rows = [
            (source_file, start + i, chunk, json.dumps(emb))
            for i, (chunk, emb) in enumerate(
                zip(chunks[start:end], embeddings[start:end])
            )
        ]

        # Step 1 — load plain strings into temp table (no VECTOR type, no cast)
        cur.executemany(
            f"INSERT INTO {tmp} (source_file, chunk_index, chunk_text, embedding_json) "
            f"VALUES (%s, %s, %s, %s)",
            rows,
        )

        # Step 2 — cast VECTOR inside SELECT, write to final table
        cur.execute(
            f"""
            INSERT INTO {fqn}
                (source_file, chunk_index, chunk_text, chunk_embedding)
            SELECT
                source_file,
                chunk_index,
                chunk_text,
                PARSE_JSON(embedding_json)::VECTOR(FLOAT, 768)
            FROM {tmp};
            """
        )

        # Clear staging table before next batch
        cur.execute(f"TRUNCATE TABLE {tmp};")

        conn.commit()
        done += len(rows)
        sys.stdout.write(f"\r  Inserting … {done}/{total} rows committed   ")
        sys.stdout.flush()

    cur.execute(f"DROP TABLE IF EXISTS {tmp};")
    print()   # newline after progress

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Load unstructured S3 files into Snowflake via chunking + Cortex embeddings."
    )
    p.add_argument("--s3-bucket",     default=DEFAULT_S3_BUCKET)
    p.add_argument("--s3-prefix",     default=DEFAULT_S3_PREFIX)
    p.add_argument("--aws-region",    default="us-east-2")
    p.add_argument("--database",      default=DEFAULT_DATABASE,
                   help="Target Snowflake database (default: NEIGHBOURWISE_DOMAINS)")
    p.add_argument("--schema",        default=DEFAULT_SCHEMA,
                   help="Target Snowflake schema — created automatically if missing (default: RAW_UNSTRUCTURED)")
    p.add_argument("--table",         default=DEFAULT_TABLE)
    p.add_argument("--embed-model",   default=DEFAULT_EMBED_MODEL,
                   help="Snowflake Cortex embedding model name")
    p.add_argument("--chunk-size",    type=int, default=DEFAULT_CHUNK_SIZE)
    p.add_argument("--chunk-overlap", type=int, default=DEFAULT_CHUNK_OVERLAP)
    p.add_argument("--embed-batch",   type=int, default=DEFAULT_EMBED_BATCH,
                   help="Chunks per embedding round-trip (default 50)")
    p.add_argument("--insert-batch",  type=int, default=DEFAULT_INSERT_BATCH,
                   help="Rows per INSERT batch (default 200)")
    p.add_argument("--min-chars",     type=int, default=DEFAULT_MIN_CHARS,
                   help="Skip chunks shorter than this many chars (default 150)")
    p.add_argument("--pattern",       default=r".*\.(txt|md|json|csv)$",
                   help="Regex to filter S3 object keys")
    p.add_argument("--dry-run",       action="store_true",
                   help="List files and chunk counts without writing to Snowflake")
    return p.parse_args()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    db     = args.database.upper()
    schema = args.schema.upper()
    fqn    = f"{db}.{schema}.{args.table}"

    aws_key_id     = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    print(f"\nTarget location : {fqn}")
    print(f"Scanning s3://{args.s3_bucket}/{args.s3_prefix} …")
    objects = list_s3_objects(
        bucket=args.s3_bucket,
        prefix=args.s3_prefix,
        pattern=args.pattern,
        aws_key_id=aws_key_id,
        aws_secret_key=aws_secret_key,
        aws_region=args.aws_region,
    )
    print(f"Found {len(objects)} matching file(s).")

    if not objects:
        print("Nothing to do. Exiting.")
        return

    conn = sf_connect(database=db, schema=schema)
    cur  = conn.cursor()

    try:
        if not args.dry_run:
            # ── Auto-create RAW_UNSTRUCTURED schema if it doesn't exist ──────
            ensure_schema(cur, db, schema)
            ensure_chunks_table(cur, db, schema, args.table)

        total_chunks_run = 0

        for obj in objects:
            key = obj["Key"]
            print(f"\n── Processing: s3://{args.s3_bucket}/{key}")

            text = read_s3_object(
                bucket=args.s3_bucket,
                key=key,
                aws_key_id=aws_key_id,
                aws_secret_key=aws_secret_key,
                aws_region=args.aws_region,
            )

            chunks = [
                c for c in
                chunk_text(text, args.chunk_size, args.chunk_overlap, filename=key)
                if len(c) >= args.min_chars
            ]
            print(f"  {len(chunks)} chunk(s)  "
                  f"[size={args.chunk_size}, overlap={args.chunk_overlap}]")

            if args.dry_run:
                for i, c in enumerate(chunks[:3]):
                    print(f"  chunk[{i}]: {c[:120]!r} …")
                continue

            # ── Embed (batched, with progress bar) ──────────────────────────
            print(f"  Embedding with '{args.embed_model}' "
                  f"(batch={args.embed_batch}) …")
            embeddings = embed_chunks_in_batches(
                cur, chunks, args.embed_model, args.embed_batch
            )

            # ── Insert (batched, with progress) ─────────────────────────────
            insert_chunks(
                cur, conn,
                database=db,
                schema=schema,
                table_name=args.table,
                source_file=key,
                chunks=chunks,
                embeddings=embeddings,
                batch_size=args.insert_batch,
            )

            total_chunks_run += len(chunks)
            print(f"  ✓ Done: {len(chunks)} chunks from {key}")

        if not args.dry_run:
            cur.execute(f"SELECT COUNT(*) FROM {fqn};")
            total_rows = cur.fetchone()[0]
            print(f"\n✅  Run complete.")
            print(f"   Schema  : {db}.{schema}")
            print(f"   Table   : {fqn}")
            print(f"   Chunks inserted this run : {total_chunks_run}")
            print(f"   Total rows in table      : {total_rows}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()