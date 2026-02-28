# dags/grocery_unstructured_scrape_dag.py
"""
grocery_unstructured_scrape_dag

Pipeline:
1) Fetch HTML from https://www.bitchesgetriches.com/grocery-stores/
2) Extract readable article text -> Markdown (clean_text.md)
3) Upload to S3:
   s3://neighborwise-ai-s3-bucket/proximity/grocery/unstructured/clean_text_YYYY-MM-DD.md
"""

from __future__ import annotations

import os
import re
import datetime as dt
from pathlib import Path
from typing import Optional

import boto3
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago


# -------------------------
# Load .env (optional)
# -------------------------
load_dotenv()


# =========================
# CONFIG
# =========================
PAGE_URL = "https://www.bitchesgetriches.com/grocery-stores/"

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
S3_BUCKET = Variable.get("s3_bucket", default_var="neighborwise-ai-s3-bucket")
S3_PREFIX = Variable.get(
    "s3_prefix_grocery_unstructured",
    default_var="proximity/grocery/unstructured/",
)

# Local output dir inside container (safe writable path)
OUT_DIR = "/opt/airflow/data/_out_grocery_unstructured"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

STOP_PHRASES = [
    # stop before comments/footer blocks
    "Leave a Reply",
    "Post navigation",
    "Loading Comments",
    "Write a Comment",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# =========================
# Helpers (AWS/S3)
# =========================
def upload_bytes_to_s3(data: bytes, bucket: str, key: str, content_type: str) -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)  # uses env vars automatically
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


# =========================
# Helpers (Scrape/Clean)
# =========================
def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=45)
    r.raise_for_status()
    return r.text


def strip_noise(soup: BeautifulSoup) -> None:
    for t in soup(["script", "style", "noscript", "iframe", "svg"]):
        t.decompose()

    # best-effort remove common layout chrome
    selectors = [
        "header",
        "footer",
        "nav",
        "aside",
        ".sidebar",
        ".site-header",
        ".site-footer",
        ".navigation",
        ".menu",
        ".widget",
        ".share",
        ".social",
        ".post-navigation",
    ]
    for sel in selectors:
        for t in soup.select(sel):
            t.decompose()


def find_main_container(soup: BeautifulSoup):
    article = soup.find("article")
    if article:
        return article
    h1 = soup.find("h1")
    if h1 and h1.parent:
        cur = h1
        for _ in range(6):
            if cur.parent:
                cur = cur.parent
        return cur
    return soup.body or soup


def normalize_whitespace(text: str) -> str:
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def to_markdown_like(container) -> str:
    out = []
    seen = set()

    def add(line: str = ""):
        out.append(line)

    def should_stop(txt: str) -> bool:
        low = (txt or "").lower()
        return any(p.lower() in low for p in STOP_PHRASES)

    # iterate descendants and format block-ish tags
    for el in container.descendants:
        if not getattr(el, "name", None):
            continue

        # Stop when we hit comment/footer marker
        txt_all = el.get_text(" ", strip=True)
        if txt_all and should_stop(txt_all):
            break

        if el.name in {"h1", "h2", "h3", "h4"}:
            text = el.get_text(" ", strip=True)
            if text and text not in seen:
                level = {"h1": "#", "h2": "##", "h3": "###", "h4": "####"}[el.name]
                add(f"{level} {text}")
                add()
                seen.add(text)

        elif el.name == "p":
            text = el.get_text(" ", strip=True)
            if text and text not in seen:
                add(text)
                add()
                seen.add(text)

        elif el.name in {"ul", "ol"}:
            # avoid nested list duplicates
            if el.find_parent(["ul", "ol"]) is not None:
                continue
            items = []
            for li in el.find_all("li", recursive=False):
                t = li.get_text(" ", strip=True)
                if t and not should_stop(t):
                    items.append(t)
            if items:
                for idx, t in enumerate(items, start=1):
                    prefix = "-" if el.name == "ul" else f"{idx}."
                    add(f"{prefix} {t}")
                add()

        elif el.name == "blockquote":
            t = el.get_text(" ", strip=True)
            if t and t not in seen and not should_stop(t):
                add("> " + t)
                add()
                seen.add(t)

    return normalize_whitespace("\n".join(out))


# =========================
# Task 1: Scrape -> clean_text.md
# =========================
def task_scrape_to_md(**context):
    os.makedirs(OUT_DIR, exist_ok=True)

    html = fetch_html(PAGE_URL)
    soup = BeautifulSoup(html, "html.parser")

    strip_noise(soup)
    container = find_main_container(soup)

    md_text = to_markdown_like(container)

    md_path = os.path.join(OUT_DIR, "grocery_clean_text.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_text + "\n")

    context["ti"].xcom_push(key="grocery_clean_md_path", value=md_path)
    return {"grocery_clean_md_path": md_path, "chars": len(md_text)}


# =========================
# Task 2: Upload clean_text.md to S3 (date in filename)
# =========================
def task_upload_md_to_s3(**context):
    ti = context["ti"]
    md_path = ti.xcom_pull(task_ids="scrape_to_md", key="grocery_clean_md_path")
    if not md_path or not os.path.exists(md_path):
        raise FileNotFoundError(f"Missing cleaned markdown at {md_path}")

    run_date = dt.date.today().isoformat()
    filename = f"grocery_clean_text_{run_date}.md"
    s3_key = f"{S3_PREFIX}{filename}"

    with open(md_path, "rb") as f:
        md_bytes = f.read()

    upload_bytes_to_s3(md_bytes, S3_BUCKET, s3_key, content_type="text/markdown")

    ti.xcom_push(key="s3_bucket", value=S3_BUCKET)
    ti.xcom_push(key="s3_key", value=s3_key)
    ti.xcom_push(key="run_date", value=run_date)

    return {"s3_bucket": S3_BUCKET, "s3_key": s3_key}


# =========================
# DAG
# =========================
with DAG(
    dag_id="grocery_unstructured_scrape_dag",
    description="Scrape grocery-stores article -> clean markdown -> S3",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,  # run on-demand; change to cron if you want
    catchup=False,
    max_active_runs=1,
    tags=["neighborwise", "grocery", "unstructured", "s3", "scrape"],
) as dag:

    scrape_to_md = PythonOperator(
        task_id="scrape_to_md",
        python_callable=task_scrape_to_md,
        provide_context=True,
    )

    upload_md_to_s3 = PythonOperator(
        task_id="upload_md_to_s3",
        python_callable=task_upload_md_to_s3,
        provide_context=True,
    )

    scrape_to_md >> upload_md_to_s3