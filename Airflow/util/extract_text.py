#!/usr/bin/env python3
"""
Extract readable article text from:
https://www.bitchesgetriches.com/grocery-stores/

Outputs:
- cleaned_text.md  (nice, readable text with headings/lists)
- raw_text.txt     (plain text)

Usage:
  python scrape_grocery_stores.py --url "https://www.bitchesgetriches.com/grocery-stores/" --outdir out
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup, NavigableString, Tag


DEFAULT_URL = "https://www.bitchesgetriches.com/grocery-stores/"


def fetch_html(url: str) -> str:
    headers = {
        # A polite UA helps reduce random blocks
        "User-Agent": "Mozilla/5.0 (compatible; NeighbourWiseBot/1.0; +https://example.com/bot)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def strip_noise(soup: BeautifulSoup) -> None:
    # Remove obvious junk
    for t in soup(["script", "style", "noscript", "iframe", "svg"]):
        t.decompose()

    # Remove common site chrome blocks if present
    # (safe to ignore if they don't exist)
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


def find_main_container(soup: BeautifulSoup) -> Tag:
    """
    Heuristic:
    - Prefer <article> if present
    - Else find the parent container around the H1
    - Else fall back to <body>
    """
    article = soup.find("article")
    if isinstance(article, Tag):
        return article

    h1 = soup.find("h1")
    if isinstance(h1, Tag):
        # Walk up a bit to get the main block
        cur = h1
        for _ in range(6):
            if cur.parent and isinstance(cur.parent, Tag):
                cur = cur.parent
        return cur

    body = soup.body
    if isinstance(body, Tag):
        return body

    return soup


STOP_PHRASES = [
    # present on this page; good cutoff for comments + footer
    "Leave a Reply",
    "Post navigation",
    "Loading Comments",
    "Write a Comment",
]


def iter_text_nodes(container: Tag):
    """
    Yield elements in reading order until we hit stop phrases.
    """
    for el in container.descendants:
        if isinstance(el, Tag):
            # Cut off once we reach the comments area
            txt = el.get_text(" ", strip=True)
            if txt and any(p.lower() in txt.lower() for p in STOP_PHRASES):
                return
            yield el


def normalize_whitespace(text: str) -> str:
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def to_markdown_like(container: Tag) -> str:
    """
    Convert headings, paragraphs, and lists into a readable Markdown-ish output.
    """
    out = []
    seen = set()

    def add(line: str = ""):
        out.append(line)

    for el in iter_text_nodes(container):
        if not isinstance(el, Tag):
            continue

        # Avoid repeating nested text: only handle “block-ish” tags
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
            # Only process top-level lists (avoid nested duplicates)
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
                    add(f"{prefix} {t}")
                add()

        # Optional: keep simple “blockquote” formatting
        elif el.name == "blockquote":
            t = el.get_text(" ", strip=True)
            if t and t not in seen:
                add("> " + t)
                add()
                seen.add(t)

    return normalize_whitespace("\n".join(out))


def to_plain_text(container: Tag) -> str:
    txt = container.get_text("\n", strip=True)
    # Cut off at stop phrases in raw text too
    lower = txt.lower()
    cut = len(txt)
    for p in STOP_PHRASES:
        i = lower.find(p.lower())
        if i != -1:
            cut = min(cut, i)
    return normalize_whitespace(txt[:cut])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--outdir", default="out")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    html = fetch_html(args.url)
    soup = BeautifulSoup(html, "html.parser")

    strip_noise(soup)
    container = find_main_container(soup)

    md = to_markdown_like(container)
    raw = to_plain_text(container)

    (outdir / "cleaned_text.md").write_text(md + "\n", encoding="utf-8")
    (outdir / "raw_text.txt").write_text(raw + "\n", encoding="utf-8")

    print(f"Saved:\n- {outdir/'cleaned_text.md'}\n- {outdir/'raw_text.txt'}")


if __name__ == "__main__":
    main()