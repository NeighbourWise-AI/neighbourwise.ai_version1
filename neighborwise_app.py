"""
neighbourwise_app.py — NeighbourWise AI
═══════════════════════════════════════════════════════════════════════════════
Streamlit frontend — calls FastAPI backend for all data.

Tabs:
  🏠 Overview   — KPI cards, choropleth map, domain deep-dives
  💬 Ask        — chatbot (SQL + RAG + Graph + Web Search)
  📄 Report     — PDF report generator

Run:
    streamlit run neighbourwise_app.py

Configuration:
    Set API_BASE_URL environment variable to point to your FastAPI server.
    Defaults to http://localhost:8001 for local development.
    For deployment: export API_BASE_URL=https://your-api-domain.com
"""

import os
import time
import json
import requests
import pandas as pd
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — swap API_BASE_URL env var for deployment
# ══════════════════════════════════════════════════════════════════════════════

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8001").rstrip("/")

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NeighborWise AI",
    page_icon="🏘️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.block-container { padding: 1rem 2rem 2rem 2rem; }
#MainMenu, footer, header { visibility: hidden; }


.hero-card {
    background: linear-gradient(135deg, #1e3a5f 0%, #2d6a4f 45%, #52b788 100%);
    padding: 1.6rem 2rem; border-radius: 18px; color: white;
    margin-bottom: 1.2rem;
    box-shadow: 0 8px 24px rgba(30,58,95,0.3);
    position: relative; overflow: hidden;
}
.hero-card::before {
    content: '🏘️'; position: absolute; right: 1.5rem; top: 50%;
    transform: translateY(-50%); font-size: 4.5rem; opacity: 0.12;
}
.hero-title {
    font-family: 'DM Serif Display', serif; font-size: 1.7rem;
    font-weight: 400; margin-bottom: 0.25rem;
}
.hero-subtitle { font-size: 0.88rem; opacity: 0.85; line-height: 1.5; max-width: 640px; }

.metric-card {
    background: rgba(255,255,255,0.06); padding: 0.9rem 1.1rem;
    border-radius: 14px; border: 1.5px solid rgba(255,255,255,0.6);
    margin-bottom: 0.7rem; height: 110px; overflow: hidden;
}
.metric-label {
    font-size: 0.7rem; font-weight: 600; color: rgba(255,255,255,0.5);
    text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 0.2rem;
}
.metric-value {
    font-family: 'DM Serif Display', serif; font-size: 1.2rem;
    color: #e2e8f0; line-height: 1.2;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
}
.metric-sub { 
    font-size: 0.7rem; color: rgba(255,255,255,0.35); margin-top: 0.15rem;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}

.section-card {
    background: rgba(255,255,255,0.04); padding: 1.1rem 1.2rem 0.9rem;
    border-radius: 14px; border: 1.5px solid rgba(255,255,255,0.6);
    margin-bottom: 1rem;
}
.section-title {
    font-family: 'DM Serif Display', serif; font-size: 1.05rem;
    color: #e2e8f0; margin-bottom: 3px;
}
.section-subtitle { font-size: 0.76rem; color: rgba(255,255,255,0.4); margin-bottom: 8px; }

.narrative-box {
    background: rgba(16,185,129,0.08); border: 1px solid rgba(16,185,129,0.2);
    border-left: 4px solid #10B981;
    padding: 0.8rem 1rem; border-radius: 10px; margin-bottom: 0.8rem;
    font-size: 0.88rem; line-height: 1.6; color: #e2e8f0;
}
.narrative-box-blue {
    background: rgba(96,165,250,0.08); border: 1px solid rgba(96,165,250,0.2);
    border-left: 4px solid #60a5fa;
    padding: 0.8rem 1rem; border-radius: 10px; margin-bottom: 0.8rem;
    font-size: 0.88rem; line-height: 1.6; color: #e2e8f0;
}
.narrative-title {
    font-family: 'DM Serif Display', serif; font-size: 0.95rem;
    color: #e2e8f0; margin-bottom: 0.25rem;
}

.stTabs [data-baseweb="tab-list"] { gap: 6px; }
.stTabs [data-baseweb="tab"] {
    height: 42px; padding: 0 18px; border-radius: 10px;
    font-family: 'DM Sans', sans-serif; font-size: 13px; font-weight: 500;
}
.stTabs [aria-selected="true"] {
    background-color: rgba(96,165,250,0.15) !important;
    font-weight: 700 !important;
}

.gen-btn button {
    background: linear-gradient(135deg, #1e3a5f, #2d6a4f) !important;
    color: white !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    font-size: 14px !important; padding: 12px !important; width: 100%;
}
.dl-btn button {
    background: linear-gradient(135deg, #10B981, #059669) !important;
    color: white !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    font-size: 14px !important; padding: 12px !important; width: 100%;
}
.ex-btn button {
    background: rgba(255,255,255,0.05) !important;
    color: #cbd5e1 !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 10px !important; font-size: 12px !important;
    font-weight: 500 !important; padding: 8px 10px !important;
    white-space: normal !important; word-wrap: break-word !important;
    height: auto !important; min-height: 50px !important;
    line-height: 1.4 !important; text-align: center !important;
}
.ex-btn button:hover {
    border-color: #60a5fa !important; color: #e2e8f0 !important;
    background: rgba(96,165,250,0.1) !important;
}

.score-badge {
    display: inline-block; padding: 3px 10px;
    border-radius: 999px; font-size: 0.72rem; font-weight: 600;
}
.badge-data   { background: rgba(59,130,246,0.2);  color: #93c5fd; }
.badge-chart  { background: rgba(16,185,129,0.2);  color: #6ee7b7; }
.badge-web    { background: rgba(245,158,11,0.2);  color: #fcd34d; }
.badge-report { background: rgba(139,92,246,0.2);  color: #c4b5fd; }
.badge-image  { background: rgba(236,72,153,0.2);  color: #f9a8d4; }
.badge-graph  { background: rgba(251,146,60,0.2);  color: #fed7aa; }

/* ── Cost Tracker ─────────────────────────────────────────────────── */
.ct-panel {
    background: rgba(13,15,20,0.97);
    border: 1px solid rgba(79,255,176,0.2);
    border-radius: 14px;
    padding: 0;
    margin-top: 1.4rem;
    overflow: hidden;
}
.ct-header {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 16px;
    background: rgba(79,255,176,0.05);
    border-bottom: 1px solid rgba(79,255,176,0.1);
}
.ct-header-title {
    font-size: 11px; font-weight: 700; letter-spacing: 0.1em;
    text-transform: uppercase; color: #4fffb0;
}
.ct-session-stat { margin-left: auto; font-size: 10px; color: rgba(255,255,255,0.3); font-family: monospace; }
.ct-session-stat b { color: #4fffb0; }
.ct-summary-row { display: grid; grid-template-columns: repeat(5, 1fr); border-bottom: 1px solid rgba(255,255,255,0.05); }
.ct-summary-tile { padding: 10px 14px; border-right: 1px solid rgba(255,255,255,0.05); }
.ct-summary-tile:last-child { border-right: none; }
.ct-tile-label { font-size: 9px; letter-spacing: 0.09em; text-transform: uppercase; color: rgba(255,255,255,0.28); margin-bottom: 3px; font-family: monospace; }
.ct-tile-value { font-size: 13px; font-weight: 700; color: #e2e8f0; font-family: monospace; }
.ct-tile-value.green  { color: #4fffb0; }
.ct-tile-value.blue   { color: #60a5fa; }
.ct-tile-value.yellow { color: #fbbf24; }
.ct-tile-value.orange { color: #fb923c; }
.ct-log-wrap { max-height: 220px; overflow-y: auto; }
.ct-log-header {
    display: grid; grid-template-columns: 24px 1fr 112px 72px 82px 70px 58px;
    gap: 6px; padding: 6px 14px 4px; font-size: 9px; letter-spacing: 0.1em;
    text-transform: uppercase; color: rgba(255,255,255,0.22);
    border-bottom: 1px solid rgba(255,255,255,0.05); font-family: monospace;
    position: sticky; top: 0; background: rgba(13,15,20,0.97);
}
.ct-log-row {
    display: grid; grid-template-columns: 24px 1fr 112px 72px 82px 70px 58px;
    gap: 6px; padding: 6px 14px; font-size: 10.5px; color: rgba(255,255,255,0.5);
    border-bottom: 1px solid rgba(255,255,255,0.04); align-items: center; font-family: monospace;
}
.ct-log-row:last-child { border-bottom: none; }
.ct-log-row:nth-child(even) { background: rgba(255,255,255,0.012); }
.ct-num   { color: rgba(255,255,255,0.18); text-align: right; font-size: 9px; }
.ct-query { color: #cbd5e1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.ct-model { color: #00d4ff; font-size: 9.5px; }
.ct-tok   { color: #e2e8f0; text-align: right; }
.ct-cost  { color: #4fffb0; text-align: right; font-weight: 700; }
.ct-type  { color: rgba(255,255,255,0.32); font-size: 9px; }
.ct-lat   { color: rgba(255,255,255,0.28); text-align: right; }
.ct-empty { padding: 18px 14px; font-size: 11px; color: rgba(255,255,255,0.18); text-align: center; font-style: italic; font-family: monospace; }

</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# API HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def api_get(path: str, params: dict = None) -> dict:
    """GET request to FastAPI. Returns {} on error."""
    try:
        r = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error [{path}]: {e}")
        return {}


def api_post(path: str, payload: dict = None, timeout: int = 30) -> dict:
    """POST request to FastAPI. Returns {} on error."""
    try:
        r = requests.post(f"{API_BASE_URL}{path}", json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error [{path}]: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# CACHED DATA LOADERS
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600)
def load_bluebikes_stations(neighborhood: str):
    return api_get(f"/overview/transit/bluebikes-stations/{neighborhood}")

@st.cache_data(ttl=3600)
def load_route_lines(neighborhood: str):
    return api_get(f"/overview/transit/route-lines/{neighborhood}")

@st.cache_data(ttl=3600)
def load_stop_sequence(neighborhood: str):
    return api_get(f"/overview/transit/stop-sequence/{neighborhood}")

@st.cache_data(ttl=3600)
def load_transit_stops(neighborhood: str):
    return api_get(f"/overview/transit/stops/{neighborhood}")

@st.cache_data(ttl=3600)
def load_transit_routes(neighborhood: str):
    return api_get(f"/overview/transit/routes/{neighborhood}")

@st.cache_data(ttl=3600)
def load_crime_history(neighborhood: str):
    return api_get(f"/overview/safety/crime-history/{neighborhood}")

@st.cache_data(ttl=3600)
def load_neighborhood_boundary(neighborhood: str):
    return api_get(f"/overview/safety/neighborhood-boundary/{neighborhood}")

@st.cache_data(ttl=3600)
def load_hotspot_map(neighborhood: str):
    return api_get(f"/overview/safety/hotspot-map/{neighborhood}")

@st.cache_data(ttl=3600)
def load_neighborhoods():
    data = api_get("/overview/neighborhoods")
    return data.get("neighborhoods", [])


@st.cache_data(ttl=3600)
def load_kpis(neighborhood: str = None):
    params = {"neighborhood": neighborhood} if neighborhood and neighborhood != "ALL" else {}
    return api_get("/overview/kpis", params=params)


@st.cache_data(ttl=3600)
def load_map():
    return api_get("/overview/map")


@st.cache_data(ttl=3600)
def load_crime_summary():
    return api_get("/overview/crime-summary")


@st.cache_data(ttl=3600)
def load_domain(domain: str, neighborhood: str = None):
    params = {"neighborhood": neighborhood} if neighborhood and neighborhood != "ALL" else {}
    return api_get(f"/overview/domain/{domain.lower()}", params=params)

@st.cache_data(ttl=3600)
def load_domain_matrix(neighborhood: str = None, limit: int = 20):
    params = {}
    if neighborhood and neighborhood != "ALL":
        params["neighborhood"] = neighborhood
    params["limit"] = limit
    return api_get("/overview/domain-matrix", params=params)

@st.cache_data(ttl=3600)
def load_neighbors(neighborhood: str, limit: int = 8):
    return api_get(f"/overview/neighbors/{neighborhood}", params={"limit": limit})

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

DOMAIN_OPTIONS = [
    "ALL", "Safety", "Housing", "Transit", "Grocery",
    "Healthcare", "Schools", "Restaurants", "Universities", "Bluebikes"
]

SAFETY_COLORS = {
    "EXCELLENT":    [30, 132, 73,  180],
    "GOOD":         [130, 224, 170, 180],
    "MODERATE":     [241, 196, 15,  180],
    "HIGH CONCERN": [192, 57,  43,  180],
}

TREND_COLORS = {
    "increasing": "#E45756",
    "stable":     "#F58518",
    "decreasing": "#54A24B",
}

INTENT_BADGES = {
    "data_query":  ("🔍", "Data Query",  "badge-data"),
    "chart":       ("📊", "Chart",       "badge-chart"),
    "web_search":  ("🌐", "Web Search",  "badge-web"),
    "report":      ("📄", "Report",      "badge-report"),
    "image":       ("🏙️", "Images",      "badge-image"),
    "graph_query": ("🕸️", "Graph Query", "badge-graph"),
}

EXAMPLES = [
    ("🏥", "How many hospitals in Dorchester?"),
    ("🛡️", "Top 5 safest neighborhoods in Boston"),
    ("🔄", "Compare Back Bay and Roxbury across all domains"),
    ("🎓", "Moving to Roxbury as a student — good idea?"),
    ("🚇", "Which neighborhoods have no subway access?"),
    ("🌐", "Latest MBTA service delays"),
]

REPORT_ITEMS = [
    ("📊", "Domain Scorecard",  "9 domains, 0–100"),
    ("📈", "4 Charts",          "Radar · bar · trend · comparison"),
    ("🏙️", "DALL-E Images",    "4 AI-generated visuals"),
    ("🔮", "SARIMAX Forecast",  "6-month crime prediction"),
    ("📝", "AI Narratives",     "Cortex Mistral + Claude"),
    ("📚", "RAG Context",       "Lifestyle & character"),
]

REPORT_STEPS = [
    "📡 Fetching domain data...",
    "🔮 Running SARIMAX forecast...",
    "📚 RAG lifestyle context...",
    "✍️  Cortex narratives...",
    "📊 Generating charts...",
    "🏙️  DALL-E images...",
    "📄 Assembling PDF...",
]

# ══════════════════════════════════════════════════════════════════════════════
# COST TRACKER HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _esc(s: str) -> str:
    """Minimal HTML-escape for safe injection."""
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")


def _init_cost_tracker():
    if "ct_queries" not in st.session_state:
        st.session_state.ct_queries        = []
        st.session_state.ct_session_tokens = 0
        st.session_state.ct_session_cost   = 0.0


def _log_query_cost(query_text: str, result: dict, elapsed: float):
    _init_cost_tracker()
    usage = result.get("llm_usage") or {}
    if not usage:
        return
    total_tokens = usage.get("total_tokens", 0)
    total_cost   = usage.get("total_cost_usd", 0.0)
    total_lat    = usage.get("total_latency_s", elapsed)
    num_calls    = usage.get("num_llm_calls", 0)
    calls        = usage.get("calls", [])
    route_type   = (result.get("routing") or {}).get("intent", result.get("type", "—"))
    st.session_state.ct_queries.append({
        "n":         len(st.session_state.ct_queries) + 1,
        "query":     query_text,
        "tokens":    total_tokens,
        "cost":      total_cost,
        "latency":   total_lat,
        "calls":     calls,
        "num_calls": num_calls,
        "route":     route_type,
    })
    st.session_state.ct_session_tokens += total_tokens
    st.session_state.ct_session_cost   += total_cost


def _render_cost_tracker():
    _init_cost_tracker()
    queries      = st.session_state.ct_queries
    total_tokens = st.session_state.ct_session_tokens
    total_cost   = st.session_state.ct_session_cost
    num_queries  = len(queries)
    avg_cost     = (total_cost / num_queries) if num_queries else 0.0
    last_lat     = queries[-1]["latency"] if queries else 0.0
    last_route   = queries[-1]["route"]   if queries else "—"

    header_html = (
        f'<div class="ct-header">'
        f'<span style="font-size:14px;">💰</span>'
        f'<span class="ct-header-title">Session Cost Tracker</span>'
        f'<span class="ct-session-stat">'
        f'<b>{num_queries}</b> quer{"y" if num_queries==1 else "ies"} &nbsp;·&nbsp; '
        f'<b>{total_tokens:,}</b> tokens &nbsp;·&nbsp; '
        f'<b>${total_cost:.6f}</b> total'
        f'</span></div>'
    )
    tiles_html = (
        f'<div class="ct-summary-row">'
        f'<div class="ct-summary-tile"><div class="ct-tile-label">Session Tokens</div><div class="ct-tile-value blue">{total_tokens:,}</div></div>'
        f'<div class="ct-summary-tile"><div class="ct-tile-label">Session Cost</div><div class="ct-tile-value green">${total_cost:.4f}</div></div>'
        f'<div class="ct-summary-tile"><div class="ct-tile-label">Avg / Query</div><div class="ct-tile-value">${avg_cost:.5f}</div></div>'
        f'<div class="ct-summary-tile"><div class="ct-tile-label">Last Latency</div><div class="ct-tile-value yellow">{last_lat:.1f}s</div></div>'
        f'<div class="ct-summary-tile"><div class="ct-tile-label">Last Route</div><div class="ct-tile-value orange">{last_route}</div></div>'
        f'</div>'
    )

    if not queries:
        log_html = '<div class="ct-empty">No queries yet — results will appear here after each search.</div>'
    else:
        col_headers = (
            '<div class="ct-log-header">'
            '<span>#</span><span>Query</span><span>Model</span>'
            '<span style="text-align:right">Tokens</span>'
            '<span style="text-align:right">Cost</span>'
            '<span>Purpose</span><span style="text-align:right">Latency</span>'
            '</div>'
        )
        rows_html = ""
        for q in reversed(queries):
            calls = q.get("calls") or []
            if calls:
                for i, call in enumerate(calls):
                    tok    = (call.get("input_tokens",0) or 0) + (call.get("output_tokens",0) or 0)
                    cost_c = call.get("cost_usd", 0.0) or 0.0
                    model  = str(call.get("model","—")).replace("claude-sonnet-4-6","claude-4.6 ✦")
                    purpose= call.get("purpose","—")
                    lat_c  = call.get("latency_s", 0.0) or 0.0
                    q_label= _esc(q["query"][:42]+"…" if len(q["query"])>42 else q["query"]) if i==0 else f'↳ call {i+1}'
                    num_label = str(q["n"]) if i==0 else ""
                    rows_html += (
                        f'<div class="ct-log-row">'
                        f'<span class="ct-num">{num_label}</span>'
                        f'<span class="ct-query" title="{_esc(q["query"])}">{q_label}</span>'
                        f'<span class="ct-model">{_esc(model[:14])}</span>'
                        f'<span class="ct-tok">{tok:,}</span>'
                        f'<span class="ct-cost">${cost_c:.6f}</span>'
                        f'<span class="ct-type">{_esc(purpose)}</span>'
                        f'<span class="ct-lat">{lat_c:.2f}s</span>'
                        f'</div>'
                    )
            else:
                rows_html += (
                    f'<div class="ct-log-row">'
                    f'<span class="ct-num">{q["n"]}</span>'
                    f'<span class="ct-query" title="{_esc(q["query"])}">{_esc(q["query"][:42]+"…" if len(q["query"])>42 else q["query"])}</span>'
                    f'<span class="ct-model">mixed</span>'
                    f'<span class="ct-tok">{q["tokens"]:,}</span>'
                    f'<span class="ct-cost">${q["cost"]:.6f}</span>'
                    f'<span class="ct-type">{_esc(q["route"])}</span>'
                    f'<span class="ct-lat">{q["latency"]:.2f}s</span>'
                    f'</div>'
                )
        log_html = f'<div class="ct-log-wrap">{col_headers}{rows_html}</div>'

    st.markdown(
        f'<div class="ct-panel">{header_html}{tiles_html}{log_html}</div>',
        unsafe_allow_html=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def render_metric_cards(items: list):
    cols = st.columns(len(items))
    for col, (label, value, sub) in zip(cols, items):
        col.markdown(
            f'<div class="metric-card">'
            f'<div class="metric-label">{label}</div>'
            f'<div class="metric-value">{value}</div>'
            f'<div class="metric-sub">{sub}</div></div>',
            unsafe_allow_html=True,
        )


def render_assistant_message(msg: dict, key_prefix: str):
     # ── Guardrail block ───────────────────────────────────────────────────────
    if msg.get("type") == "blocked":
        st.markdown(
            f'<div class="narrative-box" style="border-left-color:#f87171;'
            f'background:rgba(248,113,113,0.08);">'
            f'<div class="narrative-title" style="color:#f87171;">🛡️ Query Blocked</div>'
            f'{msg["content"]}'
            f'</div>',
            unsafe_allow_html=True,
        )
        return
    st.markdown(msg["content"])

    # Chart image
    if msg.get("chart_path") and Path(msg["chart_path"]).exists():
        st.image(msg["chart_path"], use_container_width=True)

    # DALL-E images
    if msg.get("image_paths"):
        valid = [p for p in msg["image_paths"] if Path(p).exists()]
        for i in range(0, len(valid), 2):
            c1, c2 = st.columns(2)
            if i < len(valid):     c1.image(valid[i],     use_container_width=True)
            if i + 1 < len(valid): c2.image(valid[i + 1], use_container_width=True)

    # SQL expander
    if msg.get("sql"):
        with st.expander(f"📊 SQL [{key_prefix}]", expanded=False):
            st.code(msg["sql"], language="sql")

    # Results table
    if msg.get("results") and isinstance(msg["results"], list) and msg["results"]:
        with st.expander(f"📋 {len(msg['results'])} rows [{key_prefix}]", expanded=False):
            st.dataframe(pd.DataFrame(msg["results"][:50]), use_container_width=True)

    # RAG sources
    if msg.get("rag_chunks"):
        with st.expander(f"📄 Sources [{key_prefix}]", expanded=False):
            for i, c in enumerate(msg["rag_chunks"][:3]):
                d = c.get("DOMAIN", c.get("domain", "?"))
                t = c.get("CHUNK_TEXT", c.get("chunk_text", ""))[:250]
                st.caption(f"**[{d}]** {t}...")
                if i < len(msg["rag_chunks"]) - 1:
                    st.divider()

    # Routing metadata
    routing = msg.get("routing")
    if routing and isinstance(routing, dict):
        with st.expander(f"🔀 Routing [{key_prefix}]", expanded=False):
            st.caption(f"**Intent:** {routing.get('intent', '—')}  |  "
                       f"**Domains:** {', '.join(routing.get('detected_domains', [])) or 'none'}  |  "
                       f"**Neighborhoods:** {', '.join(routing.get('detected_neighborhoods', [])) or 'none'}")
            if routing.get("fallback_used"):
                st.caption(f"⚠️ Fallback: {routing['fallback_used']}")

    # Intent badge + elapsed
    rtype = msg.get("type", "")
    if rtype in INTENT_BADGES:
        icon, label, cls = INTENT_BADGES[rtype]
        elapsed = msg.get("elapsed")
        time_str = (f'<span style="color:rgba(255,255,255,0.3);font-size:11px;'
                    f'margin-left:8px;">{elapsed:.1f}s</span>') if elapsed else ""
        st.markdown(
            f'<span class="score-badge {cls}">{icon} {label}</span>{time_str}',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# HERO + SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<div class="hero-card">
    <div class="hero-title">NeighborWise AI — Neighborhood Intelligence</div>
    <div class="hero-subtitle">
        Crime safety forecasting, domain scoring, and neighborhood analysis across
        51 Boston &amp; Cambridge neighborhoods — powered by SARIMAX, DBSCAN, RAG,
        and Snowflake Cortex.
    </div>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown(
        '<div style="font-family:DM Serif Display,serif;font-size:1.15rem;'
        'color:#e2e8f0;margin-bottom:3px;">NeighborWise AI</div>'
        '<div style="font-size:0.75rem;color:rgba(255,255,255,0.4);margin-bottom:12px;">'
        'Boston · Cambridge · Greater Boston</div>',
        unsafe_allow_html=True,
    )
    st.divider()

    # Neighborhood filter
    neighborhoods_raw = load_neighborhoods()
    neighborhood_names = ["ALL"] + [n["name"] for n in neighborhoods_raw]
    selected_neighborhood = st.selectbox(
        "Neighborhood",
        options=neighborhood_names,
        index=0,
        help="Filter all KPIs and domain data to a specific neighborhood",
    )

    # Domain filter
    selected_domain = st.selectbox(
        "Domain",
        options=DOMAIN_OPTIONS,
        index=0,
        help="Select a domain to see deep-dive analytics on the home page",
    )

    st.divider()
    st.markdown(
        f'<div style="font-size:0.72rem;color:rgba(255,255,255,0.3);line-height:1.5;">'
        f'51 neighborhoods · 9 domains<br>'
        f'SQL (Mistral) + RAG (e5-base-v2)<br>'
        f'Validator (Claude Sonnet)<br>'
        f'<span style="color:rgba(255,255,255,0.15);">API: {API_BASE_URL}</span></div>',
        unsafe_allow_html=True,
    )

# Convenience flags
hood_filter = selected_neighborhood if selected_neighborhood != "ALL" else None
domain_filter = selected_domain if selected_domain != "ALL" else None

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════

tab_overview, tab_chat, tab_report = st.tabs([
    "🏠  Overview", "💬  Ask NeighborWise", "📄  Neighborhood Report",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# Three modes:
#   Mode 3: hood_filter AND NOT domain_filter  → Neighborhood Profile
#   Mode 2: domain_filter                      → Domain Deep Dive
#   Mode 1: neither                            → Home Page
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    import altair as alt

    # ── Always city-wide — never pass hood_filter to load_kpis ───────────────
    kpis          = load_kpis()          # FIX: no hood_filter here
    crime_summary = load_crime_summary()

    safest_list     = kpis.get("safest", [])
    affordable_list = kpis.get("most_affordable", [])
    transit_list    = kpis.get("best_transit", [])
    overall_list    = kpis.get("best_overall", [])

    safest_name  = safest_list[0]["neighborhood"]     if safest_list     else "—"
    afford_name  = affordable_list[0]["neighborhood"] if affordable_list else "—"
    afford_rent  = affordable_list[0].get("avg_monthly_rent") if affordable_list else None
    afford_sub   = f"Avg rent: ${afford_rent:,.0f}/mo" if afford_rent else "Highest affordability score"

    trend_summary = crime_summary.get("trend_summary", {})
    n_inc    = trend_summary.get("increasing", {}).get("neighborhood_count", 0)
    n_dec    = trend_summary.get("decreasing", {}).get("neighborhood_count", 0)
    n_stable = trend_summary.get("stable",     {}).get("neighborhood_count", 0)

    transit_sorted_kpi = sorted(
        transit_list,
        key=lambda x: (x.get("score", 0) or 0, x.get("total_routes", 0) or 0),
        reverse=True,
    )
    best_transit_name  = transit_sorted_kpi[0]["neighborhood"]        if transit_sorted_kpi else "—"
    best_transit_lines = transit_sorted_kpi[0].get("rapid_transit_lines") if transit_sorted_kpi else None
    transit_sub        = best_transit_lines if best_transit_lines else "Bus network only"

    total     = n_inc + n_dec + n_stable
    safer_pct = round((n_dec + n_stable) / total * 100) if total > 0 else 0        

    # ══════════════════════════════════════════════════════════════════════════
    # MODE 3: Neighborhood selected + ALL domains → Neighborhood Profile
    # ══════════════════════════════════════════════════════════════════════════
    if hood_filter and not domain_filter:

        matrix    = load_domain_matrix(hood_filter, limit=1)
        nbhd_data = matrix.get("neighborhoods", [{}])[0]

        safety_data  = load_domain("safety",     hood_filter)
        housing_data = load_domain("housing",    hood_filter)
        transit_data = load_domain("transit",    hood_filter)
        grocery_data = load_domain("grocery",    hood_filter)
        school_data  = load_domain("schools",    hood_filter)
        health_data  = load_domain("healthcare", hood_filter)

        safety_n  = safety_data.get("scores",         [{}])[0] if safety_data.get("scores")         else {}
        housing_n = housing_data.get("neighborhoods", [{}])[0] if housing_data.get("neighborhoods") else {}
        transit_n = transit_data.get("mbta",          [{}])[0] if transit_data.get("mbta")          else {}
        grocery_n = grocery_data.get("scores",        [{}])[0] if grocery_data.get("scores")        else {}
        school_n  = school_data.get("neighborhoods",  [{}])[0] if school_data.get("neighborhoods")  else {}
        health_n  = health_data.get("scores",         [{}])[0] if health_data.get("scores")         else {}

        master_grade = nbhd_data.get("master_grade", "—")
        master_score = nbhd_data.get("master_score", "—")
        top_strength = nbhd_data.get("top_strength", "—")
        top_weakness = nbhd_data.get("top_weakness", "—")
        city         = nbhd_data.get("city", "—")

        GRADE_BG = {
            "TOP PICK":       ("#1E8449", "#fff"),
            "SOLID CHOICE":   ("#2d6a4f", "#fff"),
            "MODERATE PICK":  ("#F1C40F", "#000"),
            "LIMITED APPEAL": ("#C0392B", "#fff"),
        }
        DOMAIN_COLORS = {
            "TRANSIT":      "#60a5fa", "SAFETY":      "#22c55e",
            "SCHOOLS":      "#a78bfa", "HOUSING":     "#34d399",
            "GROCERY":      "#fbbf24", "HEALTHCARE":  "#f472b6",
            "RESTAURANTS":  "#fb923c", "UNIVERSITIES":"#818cf8",
            "BLUEBIKES":    "#67e8f9",
        }
        grade_bg, grade_fg = GRADE_BG.get(master_grade, ("#475569", "#fff"))
        s_color = DOMAIN_COLORS.get(str(top_strength).upper(), "#64748b")

        # ── Hero ──────────────────────────────────────────────────────────────
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#1e3a5f,#2d6a4f);'
            f'border-radius:16px;padding:1.6rem 2rem;margin-bottom:1rem;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;">'

            f'<div style="flex:1;">'
            f'<div style="font-family:DM Serif Display,serif;font-size:2.2rem;color:#e2e8f0;margin-bottom:2px;">{hood_filter}</div>'
            f'<div style="color:rgba(255,255,255,0.5);font-size:0.85rem;margin-bottom:14px;">{city}, Massachusetts</div>'
            f'<div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;">'
            f'<span style="background:{grade_bg};color:{grade_fg};padding:5px 16px;'
            f'border-radius:999px;font-size:12px;font-weight:700;letter-spacing:0.04em;">{master_grade}</span>'
            f'<span style="background:{s_color}33;color:{s_color};border:1.5px solid {s_color}66;'
            f'padding:5px 16px;border-radius:999px;font-size:12px;font-weight:700;">'
            f'↑ Strength: {top_strength}</span>'
            f'<span style="background:rgba(248,113,113,0.2);color:#f87171;'
            f'border:1.5px solid rgba(248,113,113,0.4);'
            f'padding:5px 16px;border-radius:999px;font-size:12px;font-weight:700;">'
            f'↓ Weakness: {top_weakness}</span>'
            f'</div>'
            f'</div>'

            f'<div style="text-align:center;padding-left:2rem;">'
            f'<div style="font-family:DM Serif Display,serif;font-size:3.5rem;'
            f'color:#e2e8f0;line-height:1;">{int(master_score) if master_score != "—" else "—"}</div>'
            f'<div style="color:rgba(255,255,255,0.4);font-size:11px;margin-top:4px;'
            f'text-transform:uppercase;letter-spacing:0.08em;">Overall Score</div>'
            f'<div style="color:rgba(255,255,255,0.3);font-size:10px;margin-top:2px;">out of 100</div>'
            f'</div>'

            f'</div></div>',
            unsafe_allow_html=True,
        )

        # ── Domain Scorecard bar chart ─────────────────────────────────────────
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Domain Scorecard</div>'
            '<div class="section-subtitle">All 9 domains at a glance</div>',
            unsafe_allow_html=True,
        )
        domain_cols = ["Safety","Housing","Transit","Grocery","Healthcare",
                       "Schools","Restaurants","Universities","Bluebikes"]
        df_sc = pd.DataFrame([{"Domain": d, "Score": nbhd_data.get(d) or 0} for d in domain_cols])
        sc_bars = alt.Chart(df_sc).mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6).encode(
            x=alt.X("Domain:N", sort=domain_cols,
                    axis=alt.Axis(title=None, labelFontSize=12, labelFontWeight="bold", labelAngle=0)),
            y=alt.Y("Score:Q", scale=alt.Scale(domain=[0, 100]),
                    axis=alt.Axis(title="Score", grid=True, tickCount=5)),
            color=alt.Color("Score:Q",
                            scale=alt.Scale(domain=[0, 100], range=["#1e3a5f", "#52b788"]),
                            legend=None),
            tooltip=["Domain:N", alt.Tooltip("Score:Q", format=".1f")],
        )
        sc_text = alt.Chart(df_sc).mark_text(
            align="center", dy=-8, fontSize=12, fontWeight="bold", color="#e2e8f0",
        ).encode(
            x=alt.X("Domain:N", sort=domain_cols),
            y=alt.Y("Score:Q"),
            text=alt.Text("Score:Q", format=".0f"),
        )
        st.altair_chart(alt.layer(sc_bars, sc_text).properties(height=260), use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── Safety + Housing ──────────────────────────────────────────────────
        col_s, col_h = st.columns(2, gap="medium")

        with col_s:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown('<div class="section-title">🛡️ Safety</div>', unsafe_allow_html=True)
            if safety_n:
                sg = safety_n.get("safety_grade", "—")
                sg_color = {"EXCELLENT":"#1E8449","GOOD":"#82E0AA",
                            "MODERATE":"#F1C40F","HIGH CONCERN":"#C0392B"}.get(sg, "#64748b")
                yoy = safety_n.get("yoy_change_pct", 0) or 0
                yoy_color = "#22c55e" if yoy < 0 else "#ef4444"
                yoy_arrow = "↓" if yoy < 0 else "↑"
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;margin-bottom:12px;">'
                    f'<span style="font-family:DM Serif Display,serif;font-size:1.8rem;color:#e2e8f0;">{safety_n.get("safety_score","—")}</span>'
                    f'<span style="background:{sg_color}22;color:{sg_color};border:1px solid {sg_color}44;'
                    f'padding:4px 12px;border-radius:999px;font-weight:700;font-size:12px;align-self:center;">{sg}</span>'
                    f'</div>', unsafe_allow_html=True,
                )
                for label, val in [
                    ("Total Incidents", f'{safety_n.get("total_incidents","—"):,}' if safety_n.get("total_incidents") else "—"),
                    ("Violent Crimes",  f'{safety_n.get("violent_crimes","—")} ({safety_n.get("pct_violent","—")}%)'),
                    ("Most Common",     safety_n.get("most_common_offense","—") or "—"),
                    ("YoY Change",      f'<span style="color:{yoy_color}">{yoy_arrow} {abs(yoy):.1f}%</span>'),
                    ("Avg Monthly",     f'{safety_n.get("avg_monthly_incidents","—")} incidents'),
                ]:
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
                        f'border-bottom:1px solid rgba(255,255,255,0.06);">'
                        f'<span style="color:rgba(255,255,255,0.45);font-size:12px;">{label}</span>'
                        f'<span style="color:#e2e8f0;font-size:12px;font-weight:500;">{val}</span></div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Safety data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        with col_h:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown('<div class="section-title">🏠 Housing</div>', unsafe_allow_html=True)
            if housing_n:
                hg = housing_n.get("housing_grade", "—")
                hg_color = {"AFFORDABLE":"#1E8449","AVERAGE":"#F59E0B",
                            "PREMIUM":"#C0392B","BELOW_AVERAGE":"#7f8c8d"}.get(hg, "#64748b")
                rent = housing_n.get("avg_monthly_rent")
                sqft = housing_n.get("avg_price_per_sqft")
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;margin-bottom:12px;">'
                    f'<span style="font-family:DM Serif Display,serif;font-size:1.8rem;color:#e2e8f0;">{housing_n.get("housing_score","—")}</span>'
                    f'<span style="background:{hg_color}22;color:{hg_color};border:1px solid {hg_color}44;'
                    f'padding:4px 12px;border-radius:999px;font-weight:700;font-size:12px;align-self:center;">{hg}</span>'
                    f'</div>', unsafe_allow_html=True,
                )
                for label, val in [
                    ("Avg Monthly Rent", f'${rent:,.0f}/mo' if rent else "—"),
                    ("Price per Sqft",   f'${sqft:.2f}' if sqft else "—"),
                    ("Avg Living Area",  f'{housing_n.get("avg_living_area_sqft","—")} sqft'),
                    ("Avg Bedrooms",     f'{housing_n.get("avg_bedrooms","—")} bd'),
                    ("Property Age",     f'{housing_n.get("avg_property_age","—")} yrs'),
                    ("Total Properties", f'{housing_n.get("total_properties","—"):,}' if housing_n.get("total_properties") else "—"),
                ]:
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
                        f'border-bottom:1px solid rgba(255,255,255,0.06);">'
                        f'<span style="color:rgba(255,255,255,0.45);font-size:12px;">{label}</span>'
                        f'<span style="color:#e2e8f0;font-size:12px;font-weight:500;">{val}</span></div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Housing data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        # ── Transit + Grocery ─────────────────────────────────────────────────
        col_t, col_g = st.columns(2, gap="medium")

        with col_t:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown('<div class="section-title">🚇 Transit</div>', unsafe_allow_html=True)
            if transit_n:
                tg = transit_n.get("transit_grade", "—")
                tg_color = {"EXCELLENT":"#1E8449","GOOD":"#82E0AA",
                            "MODERATE":"#F1C40F","LIMITED":"#C0392B"}.get(tg, "#64748b")
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;margin-bottom:12px;">'
                    f'<span style="font-family:DM Serif Display,serif;font-size:1.8rem;color:#e2e8f0;">{transit_n.get("transit_score","—")}</span>'
                    f'<span style="background:{tg_color}22;color:{tg_color};border:1px solid {tg_color}44;'
                    f'padding:4px 12px;border-radius:999px;font-weight:700;font-size:12px;align-self:center;">{tg}</span>'
                    f'</div>', unsafe_allow_html=True,
                )
                lines = transit_n.get("rapid_transit_lines")
                if lines:
                    LINE_PILL_COLORS_M3 = {
                        "red":      ("#ef4444","#fff"), "green":    ("#22c55e","#fff"),
                        "orange":   ("#f97316","#fff"), "blue":     ("#3b82f6","#fff"),
                        "mattapan": ("#ef4444","#fff"),
                    }
                    pills = ""
                    for line in lines.split(","):
                        line = line.strip()
                        bg, fg = "#64748b", "#fff"
                        for k, (b, f) in LINE_PILL_COLORS_M3.items():
                            if k in line.lower(): bg, fg = b, f; break
                        pills += (f'<span style="background:{bg};color:{fg};padding:2px 8px;'
                                  f'border-radius:999px;font-size:10px;font-weight:700;'
                                  f'margin-right:4px;">{line}</span>')
                    st.markdown(f'<div style="margin-bottom:8px;">{pills}</div>', unsafe_allow_html=True)
                for label, val in [
                    ("Total Stops",         transit_n.get("total_stops", "—")),
                    ("Rapid Transit Stops", transit_n.get("rapid_transit_stops", "—")),
                    ("Bus Stops",           transit_n.get("bus_stops", "—")),
                    ("Total Routes",        transit_n.get("total_routes", "—")),
                    ("% Accessible",        f'{transit_n.get("pct_accessible_stops","—")}%'),
                ]:
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
                        f'border-bottom:1px solid rgba(255,255,255,0.06);">'
                        f'<span style="color:rgba(255,255,255,0.45);font-size:12px;">{label}</span>'
                        f'<span style="color:#e2e8f0;font-size:12px;font-weight:500;">{val}</span></div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Transit data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        with col_g:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown('<div class="section-title">🛒 Grocery</div>', unsafe_allow_html=True)
            if grocery_n:
                gg = grocery_n.get("grocery_grade", "—")
                gg_color = {"WELL_STOCKED":"#1E8449","ADEQUATE":"#82E0AA",
                            "MODERATE":"#F1C40F","FOOD_DESERT":"#C0392B"}.get(gg, "#64748b")
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;margin-bottom:12px;">'
                    f'<span style="font-family:DM Serif Display,serif;font-size:1.8rem;color:#e2e8f0;">{grocery_n.get("grocery_score","—")}</span>'
                    f'<span style="background:{gg_color}22;color:{gg_color};border:1px solid {gg_color}44;'
                    f'padding:4px 12px;border-radius:999px;font-weight:700;font-size:12px;align-self:center;">{gg.replace("_"," ").title()}</span>'
                    f'</div>', unsafe_allow_html=True,
                )
                for label, val in [
                    ("Total Stores",       grocery_n.get("total_stores", "—")),
                    ("Supermarkets",       grocery_n.get("supermarkets", "—")),
                    ("Pharmacies",         grocery_n.get("pharmacies", "—")),
                    ("Farmers Markets",    grocery_n.get("farmers_markets", "—")),
                    ("Stores per Sq Mile", f'{grocery_n.get("stores_per_sqmile","—")}'),
                    ("% Essential",        f'{grocery_n.get("pct_essential","—")}%'),
                ]:
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
                        f'border-bottom:1px solid rgba(255,255,255,0.06);">'
                        f'<span style="color:rgba(255,255,255,0.45);font-size:12px;">{label}</span>'
                        f'<span style="color:#e2e8f0;font-size:12px;font-weight:500;">{val}</span></div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Grocery data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        # ── Schools + Healthcare ──────────────────────────────────────────────
        col_sc, col_hc = st.columns(2, gap="medium")

        with col_sc:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown('<div class="section-title">🏫 Schools</div>', unsafe_allow_html=True)
            if school_n:
                sg = school_n.get("school_grade", "—")
                sg_color = {"EXCELLENT":"#1E8449","GOOD":"#82E0AA",
                            "MODERATE":"#F1C40F","LIMITED":"#C0392B"}.get(sg, "#64748b")
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;margin-bottom:12px;">'
                    f'<span style="font-family:DM Serif Display,serif;font-size:1.8rem;color:#e2e8f0;">{school_n.get("school_score","—")}</span>'
                    f'<span style="background:{sg_color}22;color:{sg_color};border:1px solid {sg_color}44;'
                    f'padding:4px 12px;border-radius:999px;font-weight:700;font-size:12px;align-self:center;">{sg}</span>'
                    f'</div>', unsafe_allow_html=True,
                )
                for label, val in [
                    ("Total Schools", school_n.get("total_schools", "—")),
                    ("Public",        school_n.get("public", "—")),
                    ("Private",       school_n.get("private", "—")),
                    ("Charter",       school_n.get("charter", "—")),
                    ("Elementary",    school_n.get("elementary", "—")),
                    ("Middle School", school_n.get("middle", "—")),
                    ("High School",   school_n.get("high_school", "—")),
                ]:
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
                        f'border-bottom:1px solid rgba(255,255,255,0.06);">'
                        f'<span style="color:rgba(255,255,255,0.45);font-size:12px;">{label}</span>'
                        f'<span style="color:#e2e8f0;font-size:12px;font-weight:500;">{val}</span></div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("School data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        with col_hc:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown('<div class="section-title">🏥 Healthcare</div>', unsafe_allow_html=True)
            if health_n:
                hg = health_n.get("healthcare_grade", "—")
                hg_color = {"EXCELLENT":"#1E8449","GOOD":"#82E0AA",
                            "MODERATE":"#F1C40F","LIMITED":"#C0392B"}.get(hg, "#64748b")
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;margin-bottom:12px;">'
                    f'<span style="font-family:DM Serif Display,serif;font-size:1.8rem;color:#e2e8f0;">{health_n.get("healthcare_score","—")}</span>'
                    f'<span style="background:{hg_color}22;color:{hg_color};border:1px solid {hg_color}44;'
                    f'padding:4px 12px;border-radius:999px;font-weight:700;font-size:12px;align-self:center;">{hg}</span>'
                    f'</div>', unsafe_allow_html=True,
                )
                for label, val in [
                    ("Total Facilities",  health_n.get("total_facilities", "—")),
                    ("Hospitals",         health_n.get("hospitals", "—")),
                    ("Clinics",           health_n.get("clinics", "—")),
                    ("Bed Capacity",      health_n.get("total_bed_capacity", "—")),
                    ("Facilities/SqMile", f'{health_n.get("facilities_per_sqmile","—")}'),
                    ("Density Score",     f'{health_n.get("density_score","—")}'),
                ]:
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
                        f'border-bottom:1px solid rgba(255,255,255,0.06);">'
                        f'<span style="color:rgba(255,255,255,0.45);font-size:12px;">{label}</span>'
                        f'<span style="color:#e2e8f0;font-size:12px;font-weight:500;">{val}</span></div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Healthcare data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        # ── Geographic Neighbors Comparison ───────────────────────────────────
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            f'<div class="section-title">Neighbors of {hood_filter}</div>'
            f'<div class="section-subtitle">8 geographically closest neighborhoods · overall score comparison</div>',
            unsafe_allow_html=True,
        )

        neighbors_data = load_neighbors(hood_filter, limit=8)
        neighbors      = neighbors_data.get("neighbors", [])

        if neighbors and nbhd_data:
            # Build dataframe including the selected neighborhood itself
            rows = [{
                "neighborhood": hood_filter,
                "master_score": nbhd_data.get("master_score") or 0,
                "master_grade": nbhd_data.get("master_grade", ""),
                "distance_km":  0,
                "is_selected":  True,
            }] + [{
                "neighborhood": n["neighborhood"],
                "master_score": n.get("master_score") or 0,
                "master_grade": n.get("master_grade", ""),
                "distance_km":  n.get("distance_km") or 0,
                "is_selected":  False,
            } for n in neighbors]

            df_nb = pd.DataFrame(rows)
            df_nb["is_selected"] = df_nb["is_selected"].astype(bool)

            GRADE_COLORS_NB = {
                "TOP PICK":       "#22c55e",
                "SOLID CHOICE":   "#60a5fa",
                "MODERATE PICK":  "#fbbf24",
                "LIMITED APPEAL": "#f87171",
            }
            df_nb["color"] = df_nb.apply(
                lambda r: "#ffffff" if r["is_selected"]
                else GRADE_COLORS_NB.get(r["master_grade"], "#64748b"),
                axis=1
            )
            df_nb["label_color"] = df_nb["is_selected"].apply(
                lambda x: "#52b788" if x else "#e2e8f0"
            )
            df_nb["opacity"] = df_nb["is_selected"].apply(lambda x: 1.0 if x else 0.6)
            df_nb["dist_label"] = df_nb["distance_km"].apply(
                lambda x: "← selected" if x == 0 else f"{x:.1f} km away"
            )

            bars = alt.Chart(df_nb).mark_bar(
                cornerRadiusTopRight=5, cornerRadiusBottomRight=5,
            ).encode(
                y=alt.Y("neighborhood:N",
                        sort=alt.EncodingSortField("master_score", order="descending"),
                        axis=alt.Axis(title=None, labelFontSize=12, labelLimit=200)),
                x=alt.X("master_score:Q",
                        scale=alt.Scale(domain=[0, 100]),
                        axis=alt.Axis(title="Overall Score", grid=True)),
                color=alt.Color("color:N", scale=None, legend=None),
                opacity=alt.Opacity("opacity:Q", scale=None, legend=None),
                tooltip=[
                    alt.Tooltip("neighborhood:N",  title="Neighborhood"),
                    alt.Tooltip("master_score:Q",  title="Overall Score", format=".1f"),
                    alt.Tooltip("master_grade:N",  title="Grade"),
                    alt.Tooltip("dist_label:N",    title="Distance"),
                ],
            )

            score_labels = alt.Chart(df_nb).mark_text(
                align="left", dx=4, fontSize=11, fontWeight="bold",
            ).encode(
                y=alt.Y("neighborhood:N",
                        sort=alt.EncodingSortField("master_score", order="descending")),
                x=alt.X("master_score:Q"),
                text=alt.Text("master_score:Q", format=".0f"),
                color=alt.condition(
                    alt.datum.is_selected == True,
                    alt.value("#52b788"),
                    alt.value("#e2e8f0"),
                ),
            )

            st.altair_chart(
                alt.layer(bars, score_labels).properties(height=320),
                use_container_width=True,
            )

            # Best and worst among neighbors
            best_nb  = max(neighbors, key=lambda x: x.get("master_score") or 0)
            worst_nb = min(neighbors, key=lambda x: x.get("master_score") or 0)
            this_score = nbhd_data.get("master_score") or 0
            better_count = sum(1 for n in neighbors if (n.get("master_score") or 0) < this_score)

            avg_neighbor_score = round(sum(n.get("master_score") or 0 for n in neighbors) / len(neighbors), 1)
            score_vs_avg = int(this_score) - int(avg_neighbor_score)
            vs_avg_str = f'+{score_vs_avg} above' if score_vs_avg >= 0 else f'{score_vs_avg} below'
            best_grade  = best_nb.get("master_grade", "—")
            worst_grade = worst_nb.get("master_grade", "—")

            st.markdown(
                f'<div class="narrative-box" style="margin-top:8px;line-height:1.9;">'
                f'<div style="margin-bottom:4px;">'
                f'📍 <b>{hood_filter}</b> scores <b>{int(this_score)}/100</b> — '
                f'<b>{vs_avg_str}</b> the neighborhood average of <b>{avg_neighbor_score}</b> '
                f'and outperforms <b>{better_count} of {len(neighbors)}</b> nearest neighbors.</div>'
                f'<div style="margin-bottom:4px;">'
                f'🏆 Top performer nearby: <b>{best_nb["neighborhood"]}</b> '
                f'({int(best_nb["master_score"] or 0)}/100 · {best_grade}) — '
                f'{round(best_nb.get("distance_km") or 0, 1)} km away.</div>'
                f'<div>'
                f'📉 Lowest nearby: <b>{worst_nb["neighborhood"]}</b> '
                f'({int(worst_nb["master_score"] or 0)}/100 · {worst_grade}) — '
                f'{round(worst_nb.get("distance_km") or 0, 1)} km away.</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.info("Neighbor data not available.")

        st.markdown('</div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # MODE 2: Domain selected → Domain Deep Dive
    # ══════════════════════════════════════════════════════════════════════════
    elif domain_filter:

        domain_data = load_domain(domain_filter, hood_filter)

        st.markdown(
            f'<div class="narrative-box-blue">'
            f'<div class="narrative-title">🔍 {domain_filter} Deep Dive'
            f'{f" — {hood_filter}" if hood_filter else " — All Neighborhoods"}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── SAFETY ────────────────────────────────────────────────────────────
        if domain_filter == "Safety":
            scores    = domain_data.get("scores", [])
            hotspots  = domain_data.get("hotspots", [])
            narrative = domain_data.get("narrative", [])
            forecasts = domain_data.get("forecasts", [])

            # ══ CITY-WIDE VIEW ═══════════════════════════════════════════════
            if not hood_filter:

                # ── City-wide KPI cards (5) ───────────────────────────────────
                if scores:
                    df_s = pd.DataFrame(scores)
                    safest_s   = df_s.loc[df_s["safety_score"].idxmax()]
                    riskiest_s = df_s.loc[df_s["safety_score"].idxmin()]
                    avg_score  = round(df_s["safety_score"].mean(), 1)
                    n_exc      = int((df_s["safety_grade"] == "EXCELLENT").sum())
                    n_concern  = int((df_s["safety_grade"] == "HIGH CONCERN").sum())
                    render_metric_cards([
                        ("Safest Neighborhood",  safest_s["neighborhood"],   f'Score: {safest_s["safety_score"]:.0f} · {safest_s["safety_grade"]}'),
                        ("Highest Concern",       riskiest_s["neighborhood"], f'Score: {riskiest_s["safety_score"]:.0f} · {riskiest_s["safety_grade"]}'),
                        ("City Avg Safety Score", f'{avg_score}',             "Across all neighborhoods"),
                        ("Excellent Rated",        f'{n_exc}',                "Neighborhoods"),
                        ("High Concern",           f'{n_concern}',            "Neighborhoods"),
                    ])

                # ── Trend KPI cards (3 colored) ───────────────────────────────
                st.markdown(
                    f'<div style="display:flex;gap:12px;margin-bottom:1rem;">'

                    f'<div class="metric-card" style="flex:1;border-color:rgba(239,68,68,0.4);">'
                    f'<div class="metric-label" style="color:rgba(239,68,68,0.7);">📈 Rising</div>'
                    f'<div class="metric-value" style="color:#ef4444;">{n_inc}</div>'
                    f'<div class="metric-sub">Neighborhoods with increasing crime</div>'
                    f'</div>'

                    f'<div class="metric-card" style="flex:1;border-color:rgba(251,191,36,0.4);">'
                    f'<div class="metric-label" style="color:rgba(251,191,36,0.7);">➡️ Stable</div>'
                    f'<div class="metric-value" style="color:#fbbf24;">{n_stable}</div>'
                    f'<div class="metric-sub">Neighborhoods holding steady</div>'
                    f'</div>'

                    f'<div class="metric-card" style="flex:1;border-color:rgba(34,197,94,0.4);">'
                    f'<div class="metric-label" style="color:rgba(34,197,94,0.7);">📉 Improving</div>'
                    f'<div class="metric-value" style="color:#22c55e;">{n_dec}</div>'
                    f'<div class="metric-sub">Neighborhoods with decreasing crime</div>'
                    f'</div>'

                    f'</div>',
                    unsafe_allow_html=True,
                )

                # Row 1: Safety grade donut + DBSCAN side by side
                col1, col2 = st.columns(2, gap="medium")
                with col1:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">Safety Grade Distribution</div>', unsafe_allow_html=True)
                    if scores:
                        df_s = pd.DataFrame(scores).drop_duplicates("neighborhood")
                        grade_counts = df_s.groupby("safety_grade").size().reset_index(name="count")
                        grade_counts["legend_label"] = grade_counts.apply(
                            lambda r: f'{r["safety_grade"]}  ({int(r["count"])})', axis=1
                        )
                        donut = alt.Chart(grade_counts).mark_arc(
                            innerRadius=80, outerRadius=160,
                            stroke="#1a1a2e", strokeWidth=2,
                        ).encode(
                            theta=alt.Theta("count:Q"),
                            color=alt.Color("legend_label:N",
                                scale=alt.Scale(
                                    domain=grade_counts["legend_label"].tolist(),
                                    range=["#86efac","#93c5fd","#fde68a","#fca5a5"]),
                                legend=alt.Legend(title=None, orient="bottom",
                                                  direction="horizontal",
                                                  labelFontSize=12, symbolSize=120)),
                            tooltip=["safety_grade:N",
                                     alt.Tooltip("count:Q", title="Neighborhoods")],
                        )
                        st.altair_chart(donut.properties(height=480), use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">DBSCAN Crime Hotspot Clusters</div>', unsafe_allow_html=True)
                    st.markdown('<div class="section-subtitle">Top 10 neighborhoods by highest crime cluster concentration</div>', unsafe_allow_html=True)
                    if hotspots:
                        df_h = pd.DataFrame(hotspots).drop_duplicates("neighborhood")
                        df_h = df_h.nlargest(10, "hotspot_clusters")
                        bars_h = alt.Chart(df_h).mark_bar(
                            cornerRadiusTopRight=4, cornerRadiusBottomRight=4,
                            color="#fca5a5", size=22,
                        ).encode(
                            y=alt.Y("neighborhood:N",
                                    sort=alt.EncodingSortField("hotspot_clusters", order="descending"),
                                    axis=alt.Axis(title=None, labelFontSize=10, labelLimit=140)),
                            x=alt.X("hotspot_clusters:Q",
                                    axis=alt.Axis(title="Crime Hotspot Clusters")),
                            tooltip=["neighborhood:N",
                                     alt.Tooltip("hotspot_clusters:Q", title="Clusters"),
                                     alt.Tooltip("hotspot_crime_share_pct:Q", format=".1f", title="Crime Share %"),
                                     alt.Tooltip("total_crimes:Q", title="Total Crimes")],
                        )
                        text_h = alt.Chart(df_h).mark_text(
                            align="left", dx=4, fontSize=15,
                            fontWeight="bold", color="#e2e8f0",
                        ).encode(
                            y=alt.Y("neighborhood:N",
                                    sort=alt.EncodingSortField("hotspot_clusters", order="descending")),
                            x=alt.X("hotspot_clusters:Q"),
                            text=alt.Text("hotspot_clusters:Q"),
                        )
                        st.altair_chart(
                            alt.layer(bars_h, text_h).properties(height=430),
                            use_container_width=True,
                        )
                    else:
                        st.info("No hotspot data available.")
                    st.markdown('</div>', unsafe_allow_html=True)

                # Row 2: Monthly crime trend line
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Monthly Crime Trend — Last 12 Months</div>', unsafe_allow_html=True)
                st.markdown('<div class="section-subtitle">Boston · Cambridge · Greater Boston</div>', unsafe_allow_html=True)
                crime_summary_data = load_crime_summary()
                monthly = crime_summary_data.get("monthly_by_city", [])
                if monthly:
                    df_m = pd.DataFrame(monthly)
                    df_m["month"] = pd.to_datetime(df_m["month"])
                    df_total = df_m.groupby("month")["crime_count"].sum().reset_index()
                    max_row = df_total.loc[df_total["crime_count"].idxmax()]
                    min_row = df_total.loc[df_total["crime_count"].idxmin()]
                    df_labels = pd.DataFrame([
                        {"month": max_row["month"], "crime_count": max_row["crime_count"], "label": f'{int(max_row["crime_count"]):,}'},
                        {"month": min_row["month"], "crime_count": min_row["crime_count"], "label": f'{int(min_row["crime_count"]):,}'},
                    ])
                    line = alt.Chart(df_total).mark_line(
                        color="#93c5fd", strokeWidth=2,
                        point={"filled": True, "size": 50, "color": "#93c5fd"},
                    ).encode(
                        x=alt.X("month:T", axis=alt.Axis(title="Month", labelAngle=0, format="%b %Y")),
                        y=alt.Y("crime_count:Q", axis=alt.Axis(title="Total Crimes", tickCount=8)),
                        tooltip=[
                            alt.Tooltip("month:T", title="Month", format="%b %Y"),
                            alt.Tooltip("crime_count:Q", title="Crimes", format=","),
                        ],
                    )
                    area = alt.Chart(df_total).mark_area(
                        color="#93c5fd", opacity=0.1,
                    ).encode(
                        x=alt.X("month:T"),
                        y=alt.Y("crime_count:Q"),
                    )
                    labels = alt.Chart(df_labels).mark_text(
                        fontSize=11, fontWeight="bold", dy=-12, color="#93c5fd",
                    ).encode(
                        x=alt.X("month:T"),
                        y=alt.Y("crime_count:Q"),
                        text=alt.Text("label:N"),
                    )
                    st.altair_chart(
                        alt.layer(area, line, labels).properties(height=420),
                        use_container_width=True,
                    )
                else:
                    st.info("Monthly crime trend data not available.")
                st.markdown('</div>', unsafe_allow_html=True)

                # Row 3: Full narrative table
                if narrative:
                    with st.expander("📋 Full Safety Narrative Table", expanded=False):
                        df_n = pd.DataFrame(narrative)
                        show_cols = [c for c in ["neighborhood","recent_trend","recent_avg_monthly",
                                                  "forecasted_count","hotspot_clusters","reliability"]
                                     if c in df_n.columns]
                        if show_cols:
                            df_n = df_n[show_cols].copy()
                            df_n.columns = ["Neighborhood","Trend","Avg Monthly",
                                            "Forecast","Hotspot Clusters","Reliability"][:len(show_cols)]
                            st.dataframe(df_n, use_container_width=True, hide_index=True)

            # ══ SINGLE NEIGHBORHOOD — SARIMAX + DBSCAN ═══════════════════════
            else:
                nbhd_score = scores[0]    if scores    else {}
                nbhd_nar   = narrative[0] if narrative else {}
                nbhd_hs    = hotspots[0]  if hotspots  else {}
                df_fc      = pd.DataFrame(forecasts) if forecasts else pd.DataFrame()

                # ── Derived values ─────────────────────────────────────────────
                trend       = nbhd_nar.get("recent_trend", "—")
                trend_emoji = {"increasing":"📈","decreasing":"📉","stable":"➡️"}.get(trend, "")
                mape        = nbhd_nar.get("train_mape")
                mape_str    = f"{mape:.1f}%" if mape else "N/A"
                yoy         = nbhd_score.get("yoy_change_pct", 0) or 0
                yoy_color   = "#22c55e" if yoy < 0 else "#ef4444"
                yoy_arrow   = "↓" if yoy < 0 else "↑"
                incidents_12m = nbhd_score.get("incidents_last_12m")
                violent_crime = nbhd_score.get("most_common_violent_offense") or nbhd_score.get("most_common_offense") or "—"
                hs_pct      = float(nbhd_hs.get("hotspot_crime_share_pct") or 0)
                next_forecast = df_fc.iloc[0]["forecasted_count"] if not df_fc.empty else None

                # Compute safety rank from all scores
                all_safety_data = load_domain("safety")
                all_safety_scores = all_safety_data.get("scores", [])
                if all_safety_scores:
                    df_all_s = pd.DataFrame(all_safety_scores).drop_duplicates("neighborhood")
                    df_all_s = df_all_s.sort_values("safety_score", ascending=False).reset_index(drop=True)
                    rank_row = df_all_s[df_all_s["neighborhood"].str.upper() == hood_filter.upper()]
                    safety_rank = int(rank_row.index[0]) + 1 if not rank_row.empty else "—"
                    total_nbhds = len(df_all_s)
                else:
                    safety_rank, total_nbhds = "—", "—"

                # ── Row 1: 4 KPI cards ─────────────────────────────────────────
                render_metric_cards([
                    ("Safety Score",
                     f'{nbhd_score.get("safety_score","—")}',
                     nbhd_score.get("safety_grade","—")),

                    ("Safety Rank",
                     f'#{safety_rank}',
                     f'of {total_nbhds} neighborhoods'),

                    (f'YoY Change',
                     f'<span style="color:{yoy_color}">{yoy_arrow} {abs(yoy):.1f}%</span>',
                     "Crime vs prior year"),

                    ("Total Incidents",
                     f'{incidents_12m:,}' if incidents_12m else "—",
                     "Last 12 months"),
                ])

                # ── Row 2: 3 KPI cards ─────────────────────────────────────────
                cols3 = st.columns(3)
                cards3 = [
                    ("Top Reported Incident",
                     violent_crime,
                     "Most commonly reported incident"),

                    ("Forecast — Next Month",
                     f'{int(next_forecast):,}' if next_forecast else "—",
                     "SARIMAX predicted crimes"),

                    ("Hotspot Crime Share",
                     f'{hs_pct:.1f}%',
                     f'{nbhd_hs.get("hotspot_clusters","—")} DBSCAN clusters'),
                ]
                for col, (label, value, sub) in zip(cols3, cards3):
                    col.markdown(
                        f'<div class="metric-card">'
                        f'<div class="metric-label">{label}</div>'
                        f'<div class="metric-value">{value}</div>'
                        f'<div class="metric-sub">{sub}</div></div>',
                        unsafe_allow_html=True,
                    )

                # ── AI narrative box ───────────────────────────────────────────
                if nbhd_nar.get("safety_narrative"):
                    st.markdown(
                        f'<div class="narrative-box-blue">'
                        f'<div class="narrative-title">{hood_filter} — AI Safety Summary</div>'
                        f'{nbhd_nar["safety_narrative"]}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                # ── Historical + SARIMAX forecast chart ───────────────────────
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown(
                    f'<div class="section-title">Historical Crime + 6-Month Forecast — {hood_filter}</div>'
                    f'<div class="section-subtitle">Blue = historical monthly crimes · Orange = SARIMAX forecast · Shaded band = 95% CI · MAPE: {mape_str}</div>',
                    unsafe_allow_html=True,
                )

                with st.spinner("Loading historical data..."):
                    history_data = load_crime_history(hood_filter)

                history = history_data.get("history", [])

                if history or not df_fc.empty:
                    # Build historical dataframe
                    if history:
                        df_hist = pd.DataFrame(history)
                        df_hist.columns = [c.lower() for c in df_hist.columns]
                        df_hist["year_month"] = pd.to_datetime(df_hist["year_month"])
                        df_hist["type"] = "Historical"
                        df_hist = df_hist.rename(columns={"crime_count": "count"})
                    else:
                        df_hist = pd.DataFrame(columns=["year_month", "count", "type"])

                    # Build forecast dataframe
                    if not df_fc.empty:
                        df_fc_plot = df_fc.copy()
                        df_fc_plot["year_month"] = pd.to_datetime(df_fc_plot["forecast_month"])
                        df_fc_plot["count"] = df_fc_plot["forecasted_count"]
                        df_fc_plot["type"] = "Forecast"
                    else:
                        df_fc_plot = pd.DataFrame(columns=["year_month", "count", "type"])

                    # Combined for line chart
                    df_comb = pd.concat([
                        df_hist[["year_month", "count", "type"]],
                        df_fc_plot[["year_month", "count", "type"]],
                    ])

                    # Main line chart
                    line = alt.Chart(df_comb).mark_line(point=True).encode(
                        x=alt.X("year_month:T", title="Month",
                                axis=alt.Axis(format="%b %Y", labelAngle=-30)),
                        y=alt.Y("count:Q", title="Crime Count"),
                        color=alt.Color(
                            "type:N",
                            scale=alt.Scale(
                                domain=["Historical", "Forecast"],
                                range=["#4C78A8", "#F58518"],
                            ),
                            legend=alt.Legend(title="", orient="top-left",
                                             labelColor="#e2e8f0", titleColor="#e2e8f0"),
                        ),
                        tooltip=[
                            alt.Tooltip("year_month:T", title="Month", format="%b %Y"),
                            alt.Tooltip("count:Q",      title="Crimes"),
                            alt.Tooltip("type:N",       title="Type"),
                        ],
                    )

                    # CI band on forecast only
                    if not df_fc_plot.empty and "lower_ci" in df_fc_plot.columns and "upper_ci" in df_fc_plot.columns:
                        ci_band = alt.Chart(df_fc_plot).mark_area(
                            opacity=0.2, color="#F58518",
                        ).encode(
                            x=alt.X("year_month:T"),
                            y=alt.Y("lower_ci:Q", title=""),
                            y2=alt.Y2("upper_ci:Q"),
                        )
                        chart = (ci_band + line).properties(height=350)
                    else:
                        chart = line.properties(height=350)

                    st.altair_chart(chart, use_container_width=True)
                    st.caption(
                        f"Model accuracy (MAPE): {mape_str} · "
                        f"{len(history)} months of historical data · "
                        f"{len(df_fc)} months forecasted"
                    )
                else:
                    st.info("No historical or forecast data available.")

                st.markdown('</div>', unsafe_allow_html=True)

                # ── Crime breakdown + DBSCAN side by side ──────────────────────
                col1, col2 = st.columns(2, gap="medium")

                with col1:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="section-title">Crime Incidents — {hood_filter}</div>'
                        f'<div class="section-subtitle">Each point is a crime incident · last 12 months · '
                        f'red = hotspot · grey = dispersed</div>',
                        unsafe_allow_html=True,
                    )

                    with st.spinner("Loading crime map..."):
                        hotspot_data  = load_hotspot_map(hood_filter)
                        boundary_data = load_neighborhood_boundary(hood_filter)

                    points      = hotspot_data.get("points", [])
                    coordinates = boundary_data.get("coordinates", [])

                    if points:
                        df_pts = pd.DataFrame(points)
                        df_pts.columns = [c.lower() for c in df_pts.columns]
                        df_pts["point_type"] = df_pts["is_noise"].map(
                            {True: "Dispersed", False: "Hotspot"}
                        )

                        scatter = alt.Chart(df_pts).mark_circle(opacity=0.55).encode(
                            longitude="lng:Q",
                            latitude="lat:Q",
                            color=alt.Color(
                                "point_type:N",
                                scale=alt.Scale(
                                    domain=["Hotspot", "Dispersed"],
                                    range=["#E45756", "#AAAAAA"],
                                ),
                                legend=alt.Legend(
                                    title="Type", orient="top-left",
                                    labelColor="#e2e8f0", titleColor="#e2e8f0",
                                    labelFontSize=11,
                                ),
                            ),
                            size=alt.condition(
                                alt.datum.point_type == "Dispersed",
                                alt.value(10), alt.value(20),
                            ),
                            tooltip=[
                                alt.Tooltip("point_type:N",  title="Zone"),
                                alt.Tooltip("description:N", title="Offense"),
                                alt.Tooltip("date:N",        title="Date"),
                                alt.Tooltip("cluster_id:Q",  title="Cluster #"),
                            ],
                        )

                        if coordinates:
                            df_bnd = pd.DataFrame(coordinates)
                            df_bnd = pd.concat([df_bnd, df_bnd.iloc[[0]]], ignore_index=True)
                            df_bnd["order"] = range(len(df_bnd))
                            boundary = alt.Chart(df_bnd).mark_line(
                                color="#FFFFFF", strokeWidth=2, opacity=0.7,
                            ).encode(
                                longitude="lng:Q",
                                latitude="lat:Q",
                                order="order:O",
                            )
                            chart = (boundary + scatter).properties(height=420)
                        else:
                            chart = scatter.properties(height=420)

                        st.altair_chart(chart, use_container_width=True)

                        hotspot_count = hotspot_data.get("hotspot_count", 0)
                        noise_count   = hotspot_data.get("noise_count", 0)
                        total_pts     = hotspot_data.get("total_points", 0)
                        cluster_count = hotspot_data.get("cluster_count", 0)
                        disp_pct      = max(0.0, 100.0 - hs_pct)

                        st.caption(
                            f"**{hotspot_count:,}** hotspot ({hs_pct:.1f}%) across "
                            f"**{cluster_count}** clusters | "
                            f"**{noise_count:,}** dispersed"
                        )
                    else:
                        st.info("No hotspot data available for this neighborhood.")
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        '<div class="section-title">Neighborhood Safety Score — Boston & Cambridge</div>'
                        '<div class="section-subtitle">Green = safer · Red = higher concern · Selected neighborhood highlighted</div>',
                        unsafe_allow_html=True,
                    )

                    map_data = load_map()
                    features = map_data.get("features", [])

                    if features:
                        import pydeck as pdk

                        for f in features:
                            grade = f["properties"].get("safety_grade", "")
                            nbhd_name = f["properties"].get("neighborhood", "")
                            is_selected = nbhd_name.upper() == hood_filter.upper()

                            base_color = SAFETY_COLORS.get(
                                str(grade).strip().upper(), [160, 160, 160, 140]
                            )
                            if is_selected:
                                f["properties"]["fill_color"] = base_color
                                f["properties"]["line_color"] = [0, 149, 255, 255]
                                f["properties"]["line_width"] = 50
                            else:
                                f["properties"]["fill_color"] = base_color
                                f["properties"]["line_color"] = [255, 255, 255, 120]
                                f["properties"]["line_width"] = 1

                        geojson = {"type": "FeatureCollection", "features": features}

                        layer = pdk.Layer(
                            "GeoJsonLayer",
                            data=geojson,
                            filled=True,
                            stroked=True,
                            pickable=True,
                            auto_highlight=True,
                            get_fill_color="properties.fill_color",
                            get_line_color="properties.line_color",
                            get_line_width="properties.line_width",
                            line_width_min_pixels=1,
                            line_width_max_pixels=4,
                        )

                        # Center on selected neighborhood
                        sel_feature = next(
                            (f for f in features
                             if f["properties"].get("neighborhood","").upper() == hood_filter.upper()),
                            None
                        )
                        center_lat = sel_feature["properties"]["latitude"] if sel_feature else 42.35
                        center_lng = sel_feature["properties"]["longitude"] if sel_feature else -71.08

                        view = pdk.ViewState(
                            latitude=center_lat,
                            longitude=center_lng,
                            zoom=11.5,
                            pitch=0,
                        )

                        deck = pdk.Deck(
                            layers=[layer],
                            initial_view_state=view,
                            tooltip={
                                "html": "<b>{neighborhood}</b><br/>"
                                        "Safety: <b>{safety_score}</b>/100 · {safety_grade}<br/>"
                                        "Overall: <b>{master_score}</b>/100",
                                "style": {
                                    "backgroundColor": "#1e293b",
                                    "color": "#e2e8f0",
                                    "fontSize": "12px",
                                    "borderRadius": "8px",
                                    "padding": "8px",
                                },
                            },
                            map_style="mapbox://styles/mapbox/dark-v10",
                        )
                        st.pydeck_chart(deck, use_container_width=True, height=420)

                        # Legend
                        l1, l2, l3, l4 = st.columns(4)
                        l1.markdown('<span style="color:#1E8449;">■</span> **Excellent**', unsafe_allow_html=True)
                        l2.markdown('<span style="color:#82E0AA;">■</span> **Good**',      unsafe_allow_html=True)
                        l3.markdown('<span style="color:#F1C40F;">■</span> **Moderate**',  unsafe_allow_html=True)
                        l4.markdown('<span style="color:#C0392B;">■</span> **High Concern**', unsafe_allow_html=True)

                    else:
                        st.info("Map data not available.")
                    st.markdown('</div>', unsafe_allow_html=True)

        # ── HOUSING ───────────────────────────────────────────────────────────
        elif domain_filter == "Housing":
            neighborhoods_data = domain_data.get("neighborhoods", [])
            grade_dist         = domain_data.get("grade_distribution", {})

            col1, col2 = st.columns([1.5, 1], gap="medium")
            with col1:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Affordability Scores</div>', unsafe_allow_html=True)
                st.caption("Higher score = more affordable")
                if neighborhoods_data:
                    df_h = pd.DataFrame(neighborhoods_data[:20])
                    bars = alt.Chart(df_h).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4,
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("housing_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="Housing Score")),
                        color=alt.Color("housing_grade:N",
                                        scale=alt.Scale(
                                            domain=["AFFORDABLE","AVERAGE","PREMIUM","BELOW_AVERAGE"],
                                            range=["#1E8449","#F59E0B","#C0392B","#7f8c8d"]),
                                        legend=alt.Legend(title="Grade", orient="bottom")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("housing_score:Q", format=".1f"),
                                 "housing_grade:N",
                                 alt.Tooltip("avg_monthly_rent:Q", title="Avg Rent $", format=",.0f"),
                                 alt.Tooltip("avg_price_per_sqft:Q", title="$/sqft", format=".2f")],
                    )
                    st.altair_chart(bars.properties(height=500), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            with col2:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Grade Distribution</div>', unsafe_allow_html=True)
                if grade_dist:
                    df_g = pd.DataFrame(list(grade_dist.items()), columns=["Grade", "Count"])
                    donut = alt.Chart(df_g).mark_arc(innerRadius=50, outerRadius=100).encode(
                        theta=alt.Theta("Count:Q"),
                        color=alt.Color("Grade:N",
                                        scale=alt.Scale(
                                            domain=["AFFORDABLE","AVERAGE","PREMIUM","BELOW_AVERAGE"],
                                            range=["#1E8449","#F59E0B","#C0392B","#7f8c8d"]),
                                        legend=alt.Legend(title=None)),
                        tooltip=["Grade:N","Count:Q"],
                    )
                    st.altair_chart(donut.properties(height=260), use_container_width=True)
                if neighborhoods_data:
                    st.markdown('<div class="section-title" style="margin-top:12px;">Rent vs Score</div>',
                                unsafe_allow_html=True)
                    df_scatter = pd.DataFrame([
                        n for n in neighborhoods_data
                        if n.get("avg_monthly_rent") and n.get("housing_score")
                    ])
                    if not df_scatter.empty:
                        scatter = alt.Chart(df_scatter).mark_circle(size=80).encode(
                            x=alt.X("avg_monthly_rent:Q", axis=alt.Axis(title="Avg Monthly Rent ($)")),
                            y=alt.Y("housing_score:Q", axis=alt.Axis(title="Affordability Score")),
                            color=alt.Color("housing_grade:N", legend=None),
                            tooltip=["neighborhood:N",
                                     alt.Tooltip("housing_score:Q", format=".1f"),
                                     alt.Tooltip("avg_monthly_rent:Q", format=",.0f", title="Rent $")],
                        )
                        st.altair_chart(scatter.properties(height=220), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

        # ── TRANSIT ───────────────────────────────────────────────────────────
        elif domain_filter == "Transit":
            mbta    = domain_data.get("mbta", [])
            summary = domain_data.get("summary", {})

            # ── KPI cards ─────────────────────────────────────────────────────
            if mbta:
                df_t = pd.DataFrame(mbta)

                if not hood_filter:
                    # City-wide KPIs
                    n_rapid     = int(df_t["has_rapid_transit"].sum())
                    avg_score   = round(df_t["transit_score"].mean(), 1)
                    total_stops = int(df_t["total_stops"].sum())
                    avg_access  = round(df_t["pct_accessible_stops"].mean(), 1)
                    top_row     = df_t.loc[df_t["transit_score"].idxmax()]

                    render_metric_cards([
                        ("With Rapid Transit",       f'{n_rapid}',
                                                     f'of {len(df_t)} neighborhoods'),
                        ("Avg Transit Score",         f'{avg_score}',
                                                     "Across all neighborhoods"),
                        ("Total Stops Citywide",      f'{total_stops:,}',
                                                     "Bus + Rapid + Rail + Ferry"),
                        ("Avg Accessibility",          f'{avg_access}%',
                                                     "Wheelchair accessible stops"),
                        ("Top Transit Neighborhood",   "Downtown",
                                                     f'Score: {top_row["transit_score"]:.0f} · {top_row["transit_grade"]}'),
                    ])

                else:
                    # Single neighborhood KPIs
                    nbhd_t = df_t.iloc[0] if not df_t.empty else {}

                    # Safety rank equivalent — transit rank
                    all_transit = load_domain("transit")
                    all_mbta    = all_transit.get("mbta", [])
                    if all_mbta:
                        df_all_t = pd.DataFrame(all_mbta).sort_values(
                            "transit_score", ascending=False
                        ).reset_index(drop=True)
                        rank_row = df_all_t[
                            df_all_t["neighborhood"].str.upper() == hood_filter.upper()
                        ]
                        transit_rank = int(rank_row.index[0]) + 1 if not rank_row.empty else "—"
                        total_nbhds  = len(df_all_t)
                    else:
                        transit_rank, total_nbhds = "—", "—"

                    has_rapid = nbhd_t.get("has_rapid_transit", False)
                    lines     = nbhd_t.get("rapid_transit_lines") or "Bus only"

                    render_metric_cards([
                        ("Transit Score",
                         f'{nbhd_t.get("transit_score","—")}',
                         nbhd_t.get("transit_grade","—")),

                        ("Transit Rank",
                         f'#{transit_rank}',
                         f'of {total_nbhds} neighborhoods'),

                        ("Rapid Transit",
                         "✅ Yes" if has_rapid else "🚌 Bus Only",
                         lines[:50] if has_rapid else "No rapid transit stops"),

                        ("Total Stops",
                         f'{nbhd_t.get("total_stops","—")}',
                         f'{nbhd_t.get("rapid_transit_stops","—")} rapid · '
                         f'{nbhd_t.get("bus_stops","—")} bus'),

                        ("% Accessible",
                         f'{nbhd_t.get("pct_accessible_stops","—")}%',
                         "Wheelchair accessible stops"),
                    ])

            # ══ CITY-WIDE VIEW ════════════════════════════════════════════════
            if not hood_filter:

                # Row 1: Choropleth map + score bar chart
                col1, col2 = st.columns(2, gap="medium")

                with col1:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        '<div class="section-title">Transit Score Map</div>'
                        '<div class="section-subtitle">Green = excellent · Red = limited · Hover for details</div>',
                        unsafe_allow_html=True,
                    )
                    map_data = load_map()
                    features = map_data.get("features", [])
                    if features and mbta:
                        import pydeck as pdk
                        TRANSIT_FILL = {
                            "EXCELLENT": [30,  132, 73,  200],
                            "GOOD":      [130, 224, 170, 180],
                            "MODERATE":  [241, 196, 15,  180],
                            "LIMITED":   [192, 57,  43,  200],
                        }
                        mbta_lookup = {
                            r["neighborhood"].upper(): r for r in mbta
                        }
                        transit_features = []
                        for f in features:
                            nbhd = f["properties"]["neighborhood"].upper()
                            mbta_row = mbta_lookup.get(nbhd)
                            if not mbta_row:
                                continue
                            grade = str(mbta_row.get("transit_grade", "MODERATE")).upper()
                            f["properties"]["transit_score"] = mbta_row.get("transit_score", 0)
                            f["properties"]["transit_grade"] = grade
                            f["properties"]["rapid_lines"]   = mbta_row.get("rapid_transit_lines") or "Bus only"
                            f["properties"]["total_stops"]   = mbta_row.get("total_stops", 0)
                            f["properties"]["fill_color"]    = TRANSIT_FILL.get(grade, [100, 100, 100, 140])
                            transit_features.append(f)

                        layer = pdk.Layer(
                            "GeoJsonLayer",
                            data={"type": "FeatureCollection", "features": transit_features},
                            filled=True, stroked=True, pickable=True, auto_highlight=True,
                            get_fill_color="properties.fill_color",
                            get_line_color=[255, 255, 255, 60],
                            line_width_min_pixels=1,
                        )
                        view = pdk.ViewState(latitude=42.35, longitude=-71.08, zoom=10.2, pitch=0)
                        deck = pdk.Deck(
                            layers=[layer],
                            initial_view_state=view,
                            tooltip={
                                "html": "<b>{neighborhood}</b><br/>"
                                        "Transit Score: <b>{transit_score}</b>/100 · <b>{transit_grade}</b><br/>"
                                        "Lines: <b>{rapid_lines}</b><br/>"
                                        "Total Stops: <b>{total_stops}</b>",
                                "style": {"backgroundColor":"#1e293b","color":"#e2e8f0",
                                          "fontSize":"12px","borderRadius":"8px","padding":"8px"},
                            },
                            map_style="mapbox://styles/mapbox/dark-v10",
                        )
                        st.pydeck_chart(deck, use_container_width=True, height=440)
                        l1, l2, l3, l4 = st.columns(4)
                        l1.markdown('<span style="color:#1E8449;">■</span> **Excellent**', unsafe_allow_html=True)
                        l2.markdown('<span style="color:#82E0AA;">■</span> **Good**',      unsafe_allow_html=True)
                        l3.markdown('<span style="color:#F1C40F;">■</span> **Moderate**',  unsafe_allow_html=True)
                        l4.markdown('<span style="color:#C0392B;">■</span> **Limited**',   unsafe_allow_html=True)
                    else:
                        st.info("Map data not available.")
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        '<div class="section-title">Transit Grade Distribution</div>'
                        '<div class="section-subtitle">All neighborhoods · colored by grade</div>',
                        unsafe_allow_html=True,
                    )
                    if mbta:
                        df_t = pd.DataFrame(mbta).drop_duplicates("neighborhood")
                        grade_counts = df_t.groupby("transit_grade").size().reset_index(name="count")
                        total_g = grade_counts["count"].sum()
                        grade_counts["pct"] = (grade_counts["count"] / total_g * 100).round(1)
                        grade_counts["legend_label"] = grade_counts.apply(
                            lambda r: f'{r["transit_grade"]}  ({r["count"]} · {r["pct"]:.1f}%)', axis=1
                        )

                        grade_order  = ["EXCELLENT", "GOOD", "MODERATE", "LIMITED"]
                        grade_colors = ["#1E8449", "#82E0AA", "#F1C40F", "#C0392B"]

                        donut = alt.Chart(grade_counts).mark_arc(
                            innerRadius=80, outerRadius=160,
                            stroke="#1a1a2e", strokeWidth=2,
                        ).encode(
                            theta=alt.Theta("count:Q"),
                            color=alt.Color("legend_label:N",
                                scale=alt.Scale(
                                    domain=grade_counts["legend_label"].tolist(),
                                    range=grade_colors[:len(grade_counts)]),
                                legend=alt.Legend(
                                    title=None, orient="bottom",
                                    direction="vertical",
                                    labelFontSize=12, symbolSize=120,
                                )),
                            tooltip=[
                                alt.Tooltip("transit_grade:N", title="Grade"),
                                alt.Tooltip("count:Q",         title="Neighborhoods"),
                                alt.Tooltip("pct:Q",           title="%", format=".1f"),
                            ],
                        )

                        # Pct labels on slices
                        labels = alt.Chart(grade_counts).mark_text(
                            radius=185, fontSize=12,
                            fontWeight="bold", color="#e2e8f0",
                        ).encode(
                            theta=alt.Theta("count:Q", stack=True),
                            text=alt.Text("pct:Q", format=".1f"),
                        )

                        st.altair_chart(
                            alt.layer(donut, labels).properties(height=480),
                            use_container_width=True,
                        )

                    st.markdown('</div>', unsafe_allow_html=True)

                # Row 2: Stop type breakdown stacked bar
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown(
                    '<div class="section-title">Stop Type Breakdown — Top 20 Neighborhoods</div>'
                    '<div class="section-subtitle">Rapid transit · commuter rail · bus · ferry</div>',
                    unsafe_allow_html=True,
                )
                if mbta:
                    df_t = pd.DataFrame(mbta[:20])
                    df_melt = df_t[[
                        "neighborhood", "rapid_transit_stops",
                        "commuter_rail_stops", "bus_stops", "ferry_stops"
                    ]].melt(id_vars="neighborhood", var_name="Stop Type", value_name="Count")

                    STOP_TYPE_LABELS = {
                        "rapid_transit_stops": "Rapid Transit",
                        "commuter_rail_stops": "Commuter Rail",
                        "bus_stops":           "Bus",
                        "ferry_stops":         "Ferry",
                    }
                    df_melt["Stop Type"] = df_melt["Stop Type"].map(STOP_TYPE_LABELS)

                    stacked = alt.Chart(df_melt).mark_bar().encode(
                        y=alt.Y("neighborhood:N",
                                sort=alt.EncodingSortField("Count", order="descending"),
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=140)),
                        x=alt.X("Count:Q", axis=alt.Axis(title="Stop Count")),
                        color=alt.Color("Stop Type:N",
                                        scale=alt.Scale(
                                            domain=["Rapid Transit","Commuter Rail","Bus","Ferry"],
                                            range=["#1d4ed8","#a78bfa","#60a5fa","#34d399"]),
                                        legend=alt.Legend(title=None, orient="bottom",
                                                          direction="horizontal")),
                        tooltip=["neighborhood:N","Stop Type:N","Count:Q"],
                    )
                    st.altair_chart(stacked.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            # ══ SINGLE NEIGHBORHOOD ═══════════════════════════════════════════
            else:
                nbhd_t = df_t.iloc[0] if mbta else {}

                # AI narrative
                desc = nbhd_t.get("description") if mbta else None
                if desc:
                    st.markdown(
                        f'<div class="narrative-box-blue">'
                        f'<div class="narrative-title">{hood_filter} — Transit Summary</div>'
                        f'{desc}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                # Row 1: Stop map + route list
                col1, col2 = st.columns(2, gap="medium")

                with col1:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="section-title">Transit Coverage — {hood_filter}</div>'
                        f'<div class="section-subtitle">Stops within neighborhood · in sequence order</div>',
                        unsafe_allow_html=True,
                    )

                    with st.spinner("Loading stop sequences..."):
                        seq_data = load_stop_sequence(hood_filter)

                    seq_routes = seq_data.get("routes", [])

                    if seq_routes:
                        TYPE_COLORS = {
                            "Heavy Rail (Subway)": "#1d4ed8",
                            "Light Rail":          "#059669",
                            "Commuter Rail":       "#7c3aed",
                            "Bus":                 "#475569",
                            "Ferry":               "#0891b2",
                        }
                        TYPE_ICONS = {
                            "Heavy Rail (Subway)": "🚇",
                            "Light Rail":          "🚊",
                            "Commuter Rail":       "🚆",
                            "Bus":                 "🚌",
                            "Ferry":               "⛴️",
                        }

                        # Sort — rapid first, bus last
                        TYPE_ORDER_RANK = {
                            "Heavy Rail (Subway)": 1,
                            "Light Rail":          2,
                            "Commuter Rail":       3,
                            "Ferry":               4,
                            "Bus":                 5,
                        }
                        seq_routes_sorted = sorted(
                            seq_routes,
                            key=lambda x: (
                                TYPE_ORDER_RANK.get(x["route_type"], 9),
                                -len(x["stops"])
                            )
                        )

                        html = '<div style="overflow-y:auto;max-height:460px;padding-right:4px;">'

                        for route in seq_routes_sorted:
                            rname  = route["route_name"]
                            rtype  = route["route_type"]
                            stops  = route["stops"]
                            color  = TYPE_COLORS.get(rtype, "#475569")
                            icon   = TYPE_ICONS.get(rtype, "🚌")

                            # Route header
                            html += (
                                f'<div style="margin:14px 0 8px;">'
                                f'<div style="display:flex;align-items:center;gap:6px;'
                                f'margin-bottom:6px;">'
                                f'<span style="background:{color};color:#fff;'
                                f'padding:2px 10px;border-radius:999px;'
                                f'font-size:10px;font-weight:700;">'
                                f'{icon} {rname}</span>'
                                f'<span style="color:rgba(255,255,255,0.25);font-size:10px;">'
                                f'{len(stops)} stop{"s" if len(stops)!=1 else ""} in {hood_filter}'
                                f'</span></div>'
                            )

                            # Stop chain
                            html += '<div style="display:flex;align-items:center;flex-wrap:wrap;gap:0;">'

                            for i, stop in enumerate(stops):
                                acc = " ♿" if stop["accessible"] else ""
                                stop_name = stop["stop_name"]
                                # Shorten long stop names
                                short = stop_name if len(stop_name) <= 22 else stop_name[:20] + "…"

                                # Stop bubble
                                html += (
                                    f'<div style="display:flex;align-items:center;">'

                                    f'<div style="background:rgba(255,255,255,0.06);'
                                    f'border:1.5px solid {color};'
                                    f'border-radius:8px;padding:4px 8px;'
                                    f'font-size:10px;color:#e2e8f0;'
                                    f'white-space:nowrap;" title="{stop_name}{acc}">'
                                    f'{short}{acc}'
                                    f'</div>'
                                )

                                # Arrow connector (not after last stop)
                                if i < len(stops) - 1:
                                    html += (
                                        f'<div style="color:{color};font-size:12px;'
                                        f'margin:0 2px;opacity:0.7;">→</div>'
                                    )

                                html += '</div>'

                            html += '</div>'  # stop chain
                            html += '</div>'  # route block

                            # Divider
                            html += '<div style="height:1px;background:rgba(255,255,255,0.05);margin:4px 0;"></div>'

                        html += '</div>'
                        st.markdown(html, unsafe_allow_html=True)

                        total_routes = len(seq_routes)
                        st.caption(
                            f"**{total_routes}** routes · direction: outbound · "
                            f"stops shown are within {hood_filter} only"
                        )
                    else:
                        st.info("No stop sequence data available.")
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="section-title">Routes Serving {hood_filter}</div>'
                        f'<div class="section-subtitle">All MBTA routes · grouped by type</div>',
                        unsafe_allow_html=True,
                    )

                    with st.spinner("Loading routes..."):
                        routes_data = load_transit_routes(hood_filter)

                    routes = routes_data.get("routes", [])

                    if routes:
                        TYPE_COLORS = {
                            "Heavy Rail":    ("#1d4ed8", "#dbeafe"),
                            "Light Rail":    ("#059669", "#d1fae5"),
                            "Commuter Rail": ("#7c3aed", "#ede9fe"),
                            "Ferry":         ("#0891b2", "#cffafe"),
                            "Bus":           ("#374151", "#e5e7eb"),
                        }
                        TYPE_ICONS = {
                            "Heavy Rail":    "🚇",
                            "Light Rail":    "🚊",
                            "Commuter Rail": "🚆",
                            "Ferry":         "⛴️",
                            "Bus":           "🚌",
                        }

                        from collections import defaultdict
                        grouped = defaultdict(list)
                        for r in routes:
                            grouped[r["route_type"]].append(r)

                        type_order = ["Heavy Rail","Light Rail","Commuter Rail","Ferry","Bus"]

                        html = '<div style="max-height:420px;overflow-y:auto;padding-right:4px;">'

                        for rtype in type_order:
                            if rtype not in grouped:
                                continue
                            bg, fg = TYPE_COLORS.get(rtype, ("#374151","#e5e7eb"))
                            icon   = TYPE_ICONS.get(rtype, "🚌")

                            # Section header
                            html += (
                                f'<div style="display:flex;align-items:center;gap:8px;'
                                f'margin:14px 0 8px;">'
                                f'<span style="background:{bg};color:{fg};padding:3px 12px;'
                                f'border-radius:999px;font-size:10px;font-weight:700;'
                                f'letter-spacing:0.05em;">{icon} {rtype.upper()}</span>'
                                f'<span style="color:rgba(255,255,255,0.25);font-size:10px;">'
                                f'{len(grouped[rtype])} route{"s" if len(grouped[rtype])>1 else ""}'
                                f'</span></div>'
                            )

                            # Route cards grid
                            html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:4px;">'
                            for r in grouped[rtype]:
                                acc_badge = (
                                    '<span style="font-size:9px;color:#4fffb0;'
                                    'margin-left:4px;">♿</span>'
                                    if r["accessible"] else ""
                                )
                                stop_bar_pct = min(100, r["stop_count"] * 4)
                                html += (
                                    f'<div style="background:rgba(255,255,255,0.04);'
                                    f'border:1px solid rgba(255,255,255,0.08);'
                                    f'border-radius:10px;padding:8px 10px;'
                                    f'position:relative;overflow:hidden;">'

                                    # Progress bar background
                                    f'<div style="position:absolute;bottom:0;left:0;'
                                    f'height:3px;width:{stop_bar_pct}%;'
                                    f'background:{bg};opacity:0.5;'
                                    f'border-radius:0 0 0 10px;"></div>'

                                    # Route name
                                    f'<div style="color:#e2e8f0;font-size:10px;'
                                    f'font-weight:600;line-height:1.3;margin-bottom:5px;">'
                                    f'{r["route_name"]}{acc_badge}</div>'

                                    # Stop count
                                    f'<div style="display:flex;align-items:center;'
                                    f'justify-content:space-between;">'
                                    f'<span style="color:rgba(255,255,255,0.35);'
                                    f'font-size:9px;text-transform:uppercase;'
                                    f'letter-spacing:0.05em;">stops in neighborhood</span>'
                                    f'<span style="color:{fg};background:{bg};'
                                    f'padding:1px 7px;border-radius:999px;'
                                    f'font-size:10px;font-weight:700;">'
                                    f'{r["stop_count"]}</span>'
                                    f'</div>'

                                    f'</div>'
                                )
                            html += '</div>'

                        html += '</div>'
                        st.markdown(html, unsafe_allow_html=True)
                    else:
                        st.info("No route data available.")

                # Row 2: Stop type donut + accessibility donut
                col3, col4 = st.columns(2, gap="medium")

                with col3:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">Stop Type Mix</div>', unsafe_allow_html=True)
                    if mbta:
                        df_types = pd.DataFrame([
                            {"Type": "Rapid Transit", "Count": nbhd_t.get("rapid_transit_stops", 0) or 0},
                            {"Type": "Commuter Rail", "Count": nbhd_t.get("commuter_rail_stops", 0) or 0},
                            {"Type": "Bus",           "Count": nbhd_t.get("bus_stops", 0) or 0},
                            {"Type": "Ferry",         "Count": nbhd_t.get("ferry_stops", 0) or 0},
                        ])
                        df_types = df_types[df_types["Count"] > 0]
                        total_s = df_types["Count"].sum()
                        df_types["Pct"] = (df_types["Count"] / total_s * 100).round(1)
                        df_types["legend_label"] = df_types.apply(
                            lambda r: f'{r["Type"]} ({r["Count"]})', axis=1
                        )

                        if not df_types.empty:
                            donut = alt.Chart(df_types).mark_arc(
                                innerRadius=55, outerRadius=95,
                                stroke="#1a1a2e", strokeWidth=2,
                            ).encode(
                                theta=alt.Theta("Count:Q"),
                                color=alt.Color("legend_label:N",
                                    scale=alt.Scale(
                                        domain=df_types["legend_label"].tolist(),
                                        range=["#1d4ed8","#a78bfa","#60a5fa","#34d399"][:len(df_types)]),
                                    legend=alt.Legend(title=None, orient="bottom",
                                                      direction="horizontal",
                                                      labelFontSize=11)),
                                tooltip=[
                                    alt.Tooltip("Type:N",  title="Type"),
                                    alt.Tooltip("Count:Q", title="Stops"),
                                    alt.Tooltip("Pct:Q",   title="%", format=".1f"),
                                ],
                            )

                            # Percentage labels on slices
                            text = alt.Chart(df_types).mark_text(
                                radius=115, fontSize=11,
                                fontWeight="bold", color="#e2e8f0",
                            ).encode(
                                theta=alt.Theta("Count:Q", stack=True),
                                text=alt.Text("Pct:Q", format=".0f"),
                            ).transform_calculate(
                                label='"%" + datum.Pct'
                            )

                            st.altair_chart(
                                alt.layer(donut, text).properties(height=390),
                                use_container_width=True,
                            )

                        # ── Rapid Transit stop details ─────────────────────
                        rapid_lines = nbhd_t.get("rapid_transit_lines")
                        if rapid_lines and rapid_lines != "Bus only":
                            st.markdown(
                                '<div style="margin-top:10px;">'
                                '<div style="color:rgba(255,255,255,0.4);font-size:10px;'
                                'text-transform:uppercase;letter-spacing:0.08em;'
                                'margin-bottom:6px;">Rapid Transit Lines</div>',
                                unsafe_allow_html=True,
                            )
                            LINE_COLORS = {
                                "red":      ("#ef4444","#fff"),
                                "green":    ("#22c55e","#fff"),
                                "orange":   ("#f97316","#fff"),
                                "blue":     ("#3b82f6","#fff"),
                                "mattapan": ("#ef4444","#fff"),
                                "silver":   ("#94a3b8","#fff"),
                            }
                            pills_html = '<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;">'
                            for line in rapid_lines.split(","):
                                line = line.strip()
                                bg, fg = "#64748b", "#fff"
                                for k, (b, f) in LINE_COLORS.items():
                                    if k in line.lower():
                                        bg, fg = b, f
                                        break
                                pills_html += (
                                    f'<span style="background:{bg};color:{fg};'
                                    f'padding:3px 10px;border-radius:999px;'
                                    f'font-size:11px;font-weight:700;">{line}</span>'
                                )
                            pills_html += '</div>'
                            st.markdown(pills_html, unsafe_allow_html=True)


                    st.markdown('</div>', unsafe_allow_html=True)

                with col4:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">Accessibility Breakdown</div>', unsafe_allow_html=True)
                    if mbta:
                        total_s   = nbhd_t.get("total_stops", 0) or 0
                        acc_pct   = nbhd_t.get("pct_accessible_stops", 0) or 0
                        acc_count = round(total_s * acc_pct / 100)
                        inacc     = total_s - acc_count
                        df_acc = pd.DataFrame([
                            {"Type": f"Accessible ({acc_pct:.1f}%)",     "Count": acc_count},
                            {"Type": f"Not Accessible ({100-acc_pct:.1f}%)", "Count": inacc},
                        ])
                        donut_acc = alt.Chart(df_acc).mark_arc(
                            innerRadius=55, outerRadius=95,
                            stroke="#1a1a2e", strokeWidth=2,
                        ).encode(
                            theta=alt.Theta("Count:Q"),
                            color=alt.Color("Type:N",
                                scale=alt.Scale(
                                    domain=[f"Accessible ({acc_pct:.1f}%)",
                                            f"Not Accessible ({100-acc_pct:.1f}%)"],
                                    range=["#22c55e","#475569"]),
                                legend=alt.Legend(title=None, orient="bottom",
                                                  direction="horizontal")),
                            tooltip=["Type:N", alt.Tooltip("Count:Q", title="Stops")],
                        )
                        st.altair_chart(donut_acc.properties(height=320), use_container_width=True)

                        # Stats
                        for label, val in [
                            ("Total Stops",          f'{total_s}'),
                            ("Accessible Stops",      f'{acc_count} ({acc_pct:.1f}%)'),
                            ("Rapid Transit Stops",   f'{nbhd_t.get("rapid_transit_stops","—")}'),
                            ("Bus Stops",             f'{nbhd_t.get("bus_stops","—")}'),
                            ("Total Routes",          f'{nbhd_t.get("total_routes","—")}'),
                        ]:
                            st.markdown(
                                f'<div style="display:flex;justify-content:space-between;'
                                f'padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.06);">'
                                f'<span style="color:rgba(255,255,255,0.45);font-size:12px;">{label}</span>'
                                f'<span style="color:#e2e8f0;font-size:12px;font-weight:500;">{val}</span>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )
                    st.markdown('</div>', unsafe_allow_html=True)

        # ── GROCERY ───────────────────────────────────────────────────────────
        elif domain_filter == "Grocery":
            scores   = domain_data.get("scores", [])
            hotspots = domain_data.get("hotspots", [])
            summary  = domain_data.get("summary", {})

            food_deserts = summary.get("food_desert_count", 0)
            if food_deserts:
                st.warning(f"⚠️ {food_deserts} neighborhoods classified as food deserts")

            col1, col2 = st.columns(2, gap="medium")
            with col1:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Grocery Access Scores</div>', unsafe_allow_html=True)
                if scores:
                    df_g = pd.DataFrame(scores[:20])
                    bars = alt.Chart(df_g).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4,
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("grocery_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="Grocery Score")),
                        color=alt.Color("grocery_grade:N",
                                        scale=alt.Scale(
                                            domain=["WELL_STOCKED","ADEQUATE","MODERATE","FOOD_DESERT"],
                                            range=["#1E8449","#82E0AA","#F1C40F","#C0392B"]),
                                        legend=alt.Legend(title="Grade", orient="bottom")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("grocery_score:Q", format=".1f"),
                                 "grocery_grade:N",
                                 alt.Tooltip("total_stores:Q", title="Total Stores"),
                                 alt.Tooltip("supermarkets:Q", title="Supermarkets"),
                                 alt.Tooltip("stores_per_sqmile:Q", format=".2f", title="Stores/sqmi")],
                    )
                    st.altair_chart(bars.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            with col2:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Store Cluster Hotspots</div>', unsafe_allow_html=True)
                if hotspots:
                    df_h = pd.DataFrame(hotspots[:20])
                    bars = alt.Chart(df_h).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#f59e0b",
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("clustered_store_share_pct:Q",
                                axis=alt.Axis(title="% Stores in Clusters")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("store_clusters:Q", title="Clusters"),
                                 alt.Tooltip("clustered_store_share_pct:Q", format=".1f", title="Clustered %"),
                                 "access_tier:N"],
                    )
                    st.altair_chart(bars.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

        # ── HEALTHCARE ────────────────────────────────────────────────────────
        elif domain_filter == "Healthcare":
            scores   = domain_data.get("scores", [])
            profiles = domain_data.get("access_profiles", [])
            hotspots = domain_data.get("hotspots", [])

            col1, col2 = st.columns(2, gap="medium")
            with col1:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Healthcare Scores</div>', unsafe_allow_html=True)
                if scores:
                    df_h = pd.DataFrame(scores[:20])
                    bars = alt.Chart(df_h).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#f472b6",
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("healthcare_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="Healthcare Score")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("healthcare_score:Q", format=".1f"),
                                 "healthcare_grade:N",
                                 alt.Tooltip("total_facilities:Q", title="Facilities"),
                                 alt.Tooltip("hospitals:Q", title="Hospitals"),
                                 alt.Tooltip("facilities_per_sqmile:Q", format=".2f", title="Fac/sqmi")],
                    )
                    st.altair_chart(bars.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            with col2:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Access Profile Scores</div>', unsafe_allow_html=True)
                if profiles:
                    df_p = pd.DataFrame(profiles[:15])
                    df_melt = df_p[["neighborhood","density_score","diversity_score","core_care_score"]].melt(
                        id_vars="neighborhood", var_name="Component", value_name="Score"
                    )
                    grouped = alt.Chart(df_melt).mark_bar().encode(
                        x=alt.X("Score:Q", axis=alt.Axis(title="Score")),
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=140)),
                        color=alt.Color("Component:N",
                                        scale=alt.Scale(
                                            domain=["density_score","diversity_score","core_care_score"],
                                            range=["#f472b6","#c084fc","#60a5fa"]),
                                        legend=alt.Legend(title=None, orient="bottom")),
                        tooltip=["neighborhood:N","Component:N",
                                 alt.Tooltip("Score:Q", format=".1f")],
                    )
                    st.altair_chart(grouped.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

        # ── SCHOOLS ───────────────────────────────────────────────────────────
        elif domain_filter == "Schools":
            neighborhoods_data = domain_data.get("neighborhoods", [])
            summary = domain_data.get("summary", {})

            st.markdown(
                f'<div class="narrative-box">'
                f'Total schools citywide: <b>{summary.get("total_schools_citywide", "—")}</b>'
                f'</div>', unsafe_allow_html=True,
            )
            col1, col2 = st.columns(2, gap="medium")
            with col1:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">School Scores</div>', unsafe_allow_html=True)
                if neighborhoods_data:
                    df_s = pd.DataFrame(neighborhoods_data[:20])
                    bars = alt.Chart(df_s).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#a78bfa",
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("school_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="School Score")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("school_score:Q", format=".1f"),
                                 "school_grade:N",
                                 alt.Tooltip("total_schools:Q", title="Total Schools"),
                                 alt.Tooltip("level_coverage_score:Q", format=".1f", title="Level Coverage")],
                    )
                    st.altair_chart(bars.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            with col2:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">School Type Mix</div>', unsafe_allow_html=True)
                if neighborhoods_data:
                    df_s = pd.DataFrame(neighborhoods_data[:15])
                    df_melt = df_s[["neighborhood","public","private","charter"]].melt(
                        id_vars="neighborhood", var_name="Type", value_name="Count"
                    )
                    stacked = alt.Chart(df_melt).mark_bar().encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=140)),
                        x=alt.X("Count:Q", axis=alt.Axis(title="School Count")),
                        color=alt.Color("Type:N",
                                        scale=alt.Scale(
                                            domain=["public","private","charter"],
                                            range=["#a78bfa","#60a5fa","#34d399"]),
                                        legend=alt.Legend(title=None, orient="bottom")),
                        tooltip=["neighborhood:N","Type:N","Count:Q"],
                    )
                    st.altair_chart(stacked.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

        # ── RESTAURANTS ───────────────────────────────────────────────────────
        elif domain_filter == "Restaurants":
            neighborhoods_data = domain_data.get("neighborhoods", [])
            summary = domain_data.get("summary", {})

            st.markdown(
                f'<div class="narrative-box">'
                f'Total restaurants citywide: <b>{summary.get("total_restaurants_citywide", "—")}</b> · '
                f'Avg rating: <b>{summary.get("avg_rating_citywide", "—")}</b>/5'
                f'</div>', unsafe_allow_html=True,
            )
            col1, col2 = st.columns(2, gap="medium")
            with col1:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Restaurant Scores</div>', unsafe_allow_html=True)
                if neighborhoods_data:
                    df_r = pd.DataFrame(neighborhoods_data[:20])
                    bars = alt.Chart(df_r).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#fb923c",
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("restaurant_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="Restaurant Score")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("restaurant_score:Q", format=".1f"),
                                 "restaurant_grade:N",
                                 alt.Tooltip("total_restaurants:Q", title="Total"),
                                 alt.Tooltip("avg_rating:Q", format=".2f", title="Avg Rating"),
                                 alt.Tooltip("cuisine_diversity:Q", title="Cuisine Types")],
                    )
                    st.altair_chart(bars.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            with col2:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Price Range Mix</div>', unsafe_allow_html=True)
                if neighborhoods_data:
                    df_r = pd.DataFrame(neighborhoods_data[:15])
                    df_melt = df_r[["neighborhood","budget","mid_range","upscale"]].melt(
                        id_vars="neighborhood", var_name="Price Range", value_name="Count"
                    )
                    stacked = alt.Chart(df_melt).mark_bar().encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=140)),
                        x=alt.X("Count:Q", axis=alt.Axis(title="Restaurant Count")),
                        color=alt.Color("Price Range:N",
                                        scale=alt.Scale(
                                            domain=["budget","mid_range","upscale"],
                                            range=["#34d399","#fb923c","#f472b6"]),
                                        legend=alt.Legend(title=None, orient="bottom")),
                        tooltip=["neighborhood:N","Price Range:N","Count:Q"],
                    )
                    st.altair_chart(stacked.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

        # ── UNIVERSITIES ──────────────────────────────────────────────────────
        elif domain_filter == "Universities":
            neighborhoods_data = domain_data.get("neighborhoods", [])
            summary = domain_data.get("summary", {})

            st.markdown(
                f'<div class="narrative-box">'
                f'{summary.get("neighborhoods_with_universities", 0)} neighborhoods have universities'
                f'</div>', unsafe_allow_html=True,
            )
            if neighborhoods_data:
                df_u    = pd.DataFrame(neighborhoods_data)
                df_with = df_u[df_u["total_universities"].fillna(0) > 0].copy()
                col1, col2 = st.columns(2, gap="medium")
                with col1:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">Education Scores</div>', unsafe_allow_html=True)
                    bars = alt.Chart(df_u.head(20)).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#818cf8",
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("education_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="Education Score")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("education_score:Q", format=".1f"),
                                 "education_grade:N",
                                 alt.Tooltip("total_universities:Q", title="Universities"),
                                 "university_names:N"],
                    )
                    st.altair_chart(bars.properties(height=460), use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">Neighborhoods With Universities</div>',
                                unsafe_allow_html=True)
                    if not df_with.empty:
                        for _, row in df_with.iterrows():
                            names = row.get("university_names") or "—"
                            st.markdown(
                                f'<div style="padding:6px 0;border-bottom:1px solid rgba(255,255,255,0.06);">'
                                f'<b style="color:#e2e8f0;">{row["neighborhood"]}</b> '
                                f'<span style="color:rgba(255,255,255,0.4);font-size:11px;">'
                                f'({int(row["total_universities"])} unis · score: {row["education_score"]:.0f})'
                                f'</span><br>'
                                f'<span style="color:rgba(255,255,255,0.55);font-size:11px;">{names}</span>'
                                f'</div>', unsafe_allow_html=True,
                            )
                    st.markdown('</div>', unsafe_allow_html=True)

        # ── BLUEBIKES ─────────────────────────────────────────────────────────
        elif domain_filter == "Bluebikes":
            neighborhoods_data = domain_data.get("neighborhoods", [])
            summary = domain_data.get("summary", {})

            # ── KPI cards ─────────────────────────────────────────────────────
            if neighborhoods_data:
                df_bb = pd.DataFrame(neighborhoods_data)

                if not hood_filter:
                    total_stations = int(df_bb["total_stations"].sum())
                    total_docks    = int(df_bb["total_docks"].sum())
                    avg_score      = round(df_bb["bikeshare_score"].mean(), 1)
                    top_row        = df_bb.loc[df_bb["bikeshare_score"].idxmax()]
                    n_with         = int((df_bb["total_stations"] > 0).sum())

                    render_metric_cards([
                        ("Total Stations",         f'{total_stations:,}',   "Across all neighborhoods"),
                        ("Total Docks",             f'{total_docks:,}',     "Bike capacity citywide"),
                        ("Avg BlueBikes Score",     f'{avg_score}',          "Across all neighborhoods"),
                        ("Neighborhoods Covered",   f'{n_with}',             f'of {len(df_bb)} neighborhoods'),
                        ("Top BlueBikes Neighborhood", top_row["neighborhood"],
                         f'Score: {top_row["bikeshare_score"]:.0f} · {top_row["total_stations"]} stations'),
                    ])
                else:
                    nbhd_bb = df_bb.iloc[0] if not df_bb.empty else {}

                    # Rank
                    all_bb_data = load_domain("bluebikes")
                    all_bb      = all_bb_data.get("neighborhoods", [])
                    if all_bb:
                        df_all_bb = pd.DataFrame(all_bb).sort_values(
                            "bikeshare_score", ascending=False
                        ).reset_index(drop=True)
                        rank_row = df_all_bb[
                            df_all_bb["neighborhood"].str.upper() == hood_filter.upper()
                        ]
                        bb_rank    = int(rank_row.index[0]) + 1 if not rank_row.empty else "—"
                        total_nbhds = len(df_all_bb)
                    else:
                        bb_rank, total_nbhds = "—", "—"

                    render_metric_cards([
                        ("BlueBikes Score",
                         f'{nbhd_bb.get("bikeshare_score","—")}',
                         nbhd_bb.get("bikeshare_grade","—")),

                        ("BlueBikes Rank",
                         f'#{bb_rank}',
                         f'of {total_nbhds} neighborhoods'),

                        ("Total Stations",
                         f'{nbhd_bb.get("total_stations","—")}',
                         "In this neighborhood"),

                        ("Total Docks",
                         f'{nbhd_bb.get("total_docks","—")}',
                         f'Avg {nbhd_bb.get("avg_docks_per_station","—")} per station'),

                        ("Stations per Sq Mile",
                         f'{nbhd_bb.get("stations_per_sqmile","—")}',
                         "Coverage density"),
                    ])

            # ══ CITY-WIDE VIEW ════════════════════════════════════════════════
            if not hood_filter:
                col1, col2 = st.columns(2, gap="medium")

                with col1:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        '<div class="section-title">BlueBikes Score Map</div>'
                        '<div class="section-subtitle">Green = excellent coverage · Red = limited · Hover for details</div>',
                        unsafe_allow_html=True,
                    )
                    map_data = load_map()
                    features = map_data.get("features", [])
                    if features and neighborhoods_data:
                        import pydeck as pdk
                        BB_FILL = {
                            "EXCELLENT": [30,  132, 73,  200],
                            "GOOD":      [130, 224, 170, 180],
                            "MODERATE":  [241, 196, 15,  180],
                            "LIMITED":   [192, 57,  43,  200],
                        }
                        bb_lookup = {r["neighborhood"].upper(): r for r in neighborhoods_data}
                        bb_features = []
                        for f in features:
                            nbhd    = f["properties"]["neighborhood"].upper()
                            bb_row  = bb_lookup.get(nbhd)
                            if not bb_row:
                                continue
                            grade = str(bb_row.get("bikeshare_grade", "MODERATE")).upper()
                            f["properties"]["bikeshare_score"]  = bb_row.get("bikeshare_score", 0)
                            f["properties"]["bikeshare_grade"]  = grade
                            f["properties"]["total_stations"]   = bb_row.get("total_stations", 0)
                            f["properties"]["total_docks"]      = bb_row.get("total_docks", 0)
                            f["properties"]["fill_color"]       = BB_FILL.get(grade, [100, 100, 100, 140])
                            bb_features.append(f)

                        layer = pdk.Layer(
                            "GeoJsonLayer",
                            data={"type": "FeatureCollection", "features": bb_features},
                            filled=True, stroked=True, pickable=True, auto_highlight=True,
                            get_fill_color="properties.fill_color",
                            get_line_color=[255, 255, 255, 60],
                            line_width_min_pixels=1,
                        )
                        deck = pdk.Deck(
                            layers=[layer],
                            initial_view_state=pdk.ViewState(
                                latitude=42.35, longitude=-71.08, zoom=10.2, pitch=0
                            ),
                            tooltip={
                                "html": "<b>{neighborhood}</b><br/>"
                                        "Score: <b>{bikeshare_score}</b>/100 · <b>{bikeshare_grade}</b><br/>"
                                        "Stations: <b>{total_stations}</b> · Docks: <b>{total_docks}</b>",
                                "style": {"backgroundColor":"#1e293b","color":"#e2e8f0",
                                          "fontSize":"12px","borderRadius":"8px","padding":"8px"},
                            },
                            map_style="mapbox://styles/mapbox/dark-v10",
                        )
                        st.pydeck_chart(deck, use_container_width=True, height=440)
                        l1, l2, l3, l4 = st.columns(4)
                        l1.markdown('<span style="color:#1E8449;">■</span> **Excellent**', unsafe_allow_html=True)
                        l2.markdown('<span style="color:#82E0AA;">■</span> **Good**',      unsafe_allow_html=True)
                        l3.markdown('<span style="color:#F1C40F;">■</span> **Moderate**',  unsafe_allow_html=True)
                        l4.markdown('<span style="color:#C0392B;">■</span> **Limited**',   unsafe_allow_html=True)
                    else:
                        st.info("Map data not available.")
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        '<div class="section-title">BlueBikes Grade Distribution</div>'
                        '<div class="section-subtitle">All neighborhoods · colored by grade</div>',
                        unsafe_allow_html=True,
                    )
                    if neighborhoods_data:
                        df_bb = pd.DataFrame(neighborhoods_data).drop_duplicates("neighborhood")
                        grade_counts = df_bb.groupby("bikeshare_grade").size().reset_index(name="count")
                        total_g = grade_counts["count"].sum()
                        grade_counts["pct"] = (grade_counts["count"] / total_g * 100).round(1)
                        grade_counts["legend_label"] = grade_counts.apply(
                            lambda r: f'{r["bikeshare_grade"]}  ({r["count"]} · {r["pct"]:.1f}%)', axis=1
                        )
                        grade_order  = ["EXCELLENT","GOOD","MODERATE","LIMITED"]
                        grade_colors = ["#1E8449","#82E0AA","#F1C40F","#C0392B"]

                        donut = alt.Chart(grade_counts).mark_arc(
                            innerRadius=80, outerRadius=160,
                            stroke="#1a1a2e", strokeWidth=2,
                        ).encode(
                            theta=alt.Theta("count:Q"),
                            color=alt.Color("legend_label:N",
                                scale=alt.Scale(
                                    domain=grade_counts["legend_label"].tolist(),
                                    range=grade_colors[:len(grade_counts)]),
                                legend=alt.Legend(title=None, orient="bottom",
                                                  direction="vertical",
                                                  labelFontSize=12, symbolSize=120)),
                            tooltip=[
                                alt.Tooltip("bikeshare_grade:N", title="Grade"),
                                alt.Tooltip("count:Q",           title="Neighborhoods"),
                                alt.Tooltip("pct:Q",             title="%", format=".1f"),
                            ],
                        )
                        labels = alt.Chart(grade_counts).mark_text(
                            radius=185, fontSize=12,
                            fontWeight="bold", color="#e2e8f0",
                        ).encode(
                            theta=alt.Theta("count:Q", stack=True),
                            text=alt.Text("pct:Q", format=".1f"),
                        )
                        st.altair_chart(
                            alt.layer(donut, labels).properties(height=480),
                            use_container_width=True,
                        )

                        # Top 5
                        st.markdown(
                            '<div style="color:rgba(255,255,255,0.4);font-size:10px;'
                            'text-transform:uppercase;letter-spacing:0.08em;'
                            'margin:10px 0 6px;">Top 5 by BlueBikes Score</div>',
                            unsafe_allow_html=True,
                        )
                        top5 = df_bb.nlargest(5, "bikeshare_score")
                        grade_color_map = dict(zip(grade_order, grade_colors))
                        for _, r in top5.iterrows():
                            grade = str(r["bikeshare_grade"]).upper()
                            color = grade_color_map.get(grade, "#475569")
                            st.markdown(
                                f'<div style="display:flex;justify-content:space-between;'
                                f'align-items:center;padding:5px 0;'
                                f'border-bottom:1px solid rgba(255,255,255,0.06);">'
                                f'<span style="color:#e2e8f0;font-size:12px;">{r["neighborhood"]}</span>'
                                f'<div style="display:flex;align-items:center;gap:8px;">'
                                f'<span style="color:rgba(255,255,255,0.4);font-size:11px;">'
                                f'{r["bikeshare_score"]:.0f}</span>'
                                f'<span style="background:{color};color:#fff;padding:1px 8px;'
                                f'border-radius:999px;font-size:9px;font-weight:700;">{grade}</span>'
                                f'</div></div>',
                                unsafe_allow_html=True,
                            )
                    st.markdown('</div>', unsafe_allow_html=True)

                # Row 2: Station size breakdown bar
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown(
                    '<div class="section-title">Station Size Mix — Top 20 Neighborhoods</div>'
                    '<div class="section-subtitle">Large · Medium · Small stations</div>',
                    unsafe_allow_html=True,
                )
                if neighborhoods_data:
                    df_bb = pd.DataFrame(neighborhoods_data[:20])
                    df_melt = df_bb[[
                        "neighborhood", "large_stations", "medium_stations", "small_stations"
                    ]].melt(id_vars="neighborhood", var_name="Size", value_name="Count")
                    SIZE_LABELS = {
                        "large_stations":  "Large (≥23 docks)",
                        "medium_stations": "Medium (15-22 docks)",
                        "small_stations":  "Small (<15 docks)",
                    }
                    df_melt["Size"] = df_melt["Size"].map(SIZE_LABELS)
                    stacked = alt.Chart(df_melt).mark_bar().encode(
                        y=alt.Y("neighborhood:N",
                                sort=alt.EncodingSortField("Count", order="descending"),
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=140)),
                        x=alt.X("Count:Q", axis=alt.Axis(title="Station Count")),
                        color=alt.Color("Size:N",
                                        scale=alt.Scale(
                                            domain=["Large (≥23 docks)","Medium (15-22 docks)","Small (<15 docks)"],
                                            range=["#1E8449","#82E0AA","#F1C40F"]),
                                        legend=alt.Legend(title=None, orient="bottom",
                                                          direction="horizontal")),
                        tooltip=["neighborhood:N","Size:N","Count:Q"],
                    )
                    st.altair_chart(stacked.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            # ══ SINGLE NEIGHBORHOOD ═══════════════════════════════════════════
            else:
                nbhd_bb = neighborhoods_data[0] if neighborhoods_data else {}

                # AI narrative
                desc = nbhd_bb.get("description") if neighborhoods_data else None
                if desc:
                    st.markdown(
                        f'<div class="narrative-box-blue">'
                        f'<div class="narrative-title">{hood_filter} — BlueBikes Summary</div>'
                        f'{desc}</div>',
                        unsafe_allow_html=True,
                    )

                col1, col2 = st.columns(2, gap="medium")

                with col1:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="section-title">Station Map — {hood_filter}</div>'
                        f'<div class="section-subtitle">'
                        f'🟢 Large &nbsp;·&nbsp; 🔵 Medium &nbsp;·&nbsp; 🟡 Small · size = dock count</div>',
                        unsafe_allow_html=True,
                    )

                    with st.spinner("Loading station map..."):
                        stations_data  = load_bluebikes_stations(hood_filter)
                        boundary_data  = load_neighborhood_boundary(hood_filter)

                    stations    = stations_data.get("stations", [])
                    coordinates = boundary_data.get("coordinates", [])

                    if stations:
                        import pydeck as pdk

                        TIER_COLORS = {
                            "LARGE":  [30,  132, 73,  230],
                            "MEDIUM": [96,  165, 250, 220],
                            "SMALL":  [241, 196, 15,  220],
                        }

                        df_st = pd.DataFrame(stations)
                        df_st.columns = [c.lower() for c in df_st.columns]
                        df_st["color"]  = df_st["capacity_tier"].map(TIER_COLORS)
                        df_st["radius"] = df_st["total_docks"] * 3

                        layers = []

                        if coordinates:
                            boundary_geojson = {
                                "type": "FeatureCollection",
                                "features": [{
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Polygon",
                                        "coordinates": [[
                                            [p["lng"], p["lat"]] for p in coordinates
                                        ]]
                                    },
                                    "properties": {}
                                }]
                            }
                            layers.append(pdk.Layer(
                                "GeoJsonLayer",
                                data=boundary_geojson,
                                filled=False, stroked=True,
                                get_line_color=[255, 255, 255, 160],
                                line_width_min_pixels=2,
                            ))

                        layers.append(pdk.Layer(
                            "ScatterplotLayer",
                            data=df_st,
                            get_position=["lng", "lat"],
                            get_fill_color="color",
                            get_radius="radius",
                            radius_min_pixels=6,
                            radius_max_pixels=18,
                            pickable=True,
                            auto_highlight=True,
                            opacity=0.9,
                        ))

                        center_lat = df_st["lat"].mean()
                        center_lng = df_st["lng"].mean()

                        deck = pdk.Deck(
                            layers=layers,
                            initial_view_state=pdk.ViewState(
                                latitude=center_lat,
                                longitude=center_lng,
                                zoom=13,
                                pitch=0,
                            ),
                            tooltip={
                                "html": "<b>{station_name}</b><br/>"
                                        "Capacity: <b>{capacity_tier}</b><br/>"
                                        "Docks: <b>{total_docks}</b>",
                                "style": {
                                    "backgroundColor": "#1e293b",
                                    "color":           "#e2e8f0",
                                    "fontSize":        "12px",
                                    "borderRadius":    "8px",
                                    "padding":         "8px",
                                },
                            },
                            map_style="mapbox://styles/mapbox/dark-v10",
                        )
                        st.pydeck_chart(deck, use_container_width=True, height=400)

                        n_large  = len(df_st[df_st["capacity_tier"] == "LARGE"])
                        n_medium = len(df_st[df_st["capacity_tier"] == "MEDIUM"])
                        n_small  = len(df_st[df_st["capacity_tier"] == "SMALL"])
                        st.caption(
                            f"**{stations_data.get('total_stations',0)}** stations · "
                            f"🟢 {n_large} large · "
                            f"🔵 {n_medium} medium · "
                            f"🟡 {n_small} small · "
                            f"**{stations_data.get('total_docks',0)}** total docks"
                        )
                    else:
                        st.info("No station data available.")
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="section-title">Station List — {hood_filter}</div>'
                        f'<div class="section-subtitle">Sorted by dock capacity · largest first</div>',
                        unsafe_allow_html=True,
                    )

                    if stations:
                        TIER_COLORS_HEX = {
                            "LARGE":  ("#1E8449", "#fff"),
                            "MEDIUM": ("#60a5fa", "#fff"),
                            "SMALL":  ("#F1C40F", "#000"),
                        }
                        max_docks = max(s["total_docks"] for s in stations) or 1

                        html = '<div style="overflow-y:auto;max-height:420px;padding-right:4px;">'
                        for s in sorted(stations, key=lambda x: x["total_docks"], reverse=True):
                            bg, fg   = TIER_COLORS_HEX.get(s["capacity_tier"], ("#475569","#fff"))
                            pct      = max(8, int(s["total_docks"] / max_docks * 100))
                            name     = s["station_name"]
                            short    = name if len(name) <= 40 else name[:38] + "…"

                            html += (
                                f'<div style="margin-bottom:8px;">'

                                # Station name + tier pill
                                f'<div style="display:flex;justify-content:space-between;'
                                f'align-items:center;margin-bottom:3px;">'
                                f'<span style="color:#e2e8f0;font-size:11px;" title="{name}">'
                                f'{short}</span>'
                                f'<span style="background:{bg};color:{fg};padding:1px 8px;'
                                f'border-radius:999px;font-size:9px;font-weight:700;'
                                f'white-space:nowrap;margin-left:6px;">'
                                f'{s["capacity_tier"]}</span>'
                                f'</div>'

                                # Dock bar
                                f'<div style="background:rgba(255,255,255,0.04);'
                                f'border-radius:4px;height:16px;position:relative;overflow:hidden;">'
                                f'<div style="position:absolute;left:0;top:0;height:100%;'
                                f'width:{pct}%;background:{bg};opacity:0.6;"></div>'
                                f'<div style="position:absolute;left:6px;top:50%;'
                                f'transform:translateY(-50%);color:#fff;'
                                f'font-size:9px;font-weight:700;">'
                                f'{s["total_docks"]} docks</div>'
                                f'</div>'

                                f'</div>'
                            )
                        html += '</div>'
                        st.markdown(html, unsafe_allow_html=True)
                    else:
                        st.info("No station data available.")
                    st.markdown('</div>', unsafe_allow_html=True)

                # Row 2: Capacity donut + stats
                col3, col4 = st.columns(2, gap="medium")

                with col3:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">Station Capacity Mix</div>', unsafe_allow_html=True)
                    if neighborhoods_data:
                        large  = nbhd_bb.get("large_stations", 0) or 0
                        medium = nbhd_bb.get("medium_stations", 0) or 0
                        small  = nbhd_bb.get("small_stations", 0) or 0
                        df_cap = pd.DataFrame([
                            {"Tier": f"Large ({large})",  "Count": large},
                            {"Tier": f"Medium ({medium})", "Count": medium},
                            {"Tier": f"Small ({small})",   "Count": small},
                        ])
                        df_cap = df_cap[df_cap["Count"] > 0]
                        total_cap = df_cap["Count"].sum()
                        df_cap["Pct"] = (df_cap["Count"] / total_cap * 100).round(1)

                        if not df_cap.empty:
                            donut = alt.Chart(df_cap).mark_arc(
                                innerRadius=55, outerRadius=95,
                                stroke="#1a1a2e", strokeWidth=2,
                            ).encode(
                                theta=alt.Theta("Count:Q"),
                                color=alt.Color("Tier:N",
                                    scale=alt.Scale(
                                        domain=df_cap["Tier"].tolist(),
                                        range=["#1E8449","#60a5fa","#F1C40F"][:len(df_cap)]),
                                    legend=alt.Legend(title=None, orient="bottom",
                                                      direction="horizontal")),
                                tooltip=[
                                    alt.Tooltip("Tier:N",  title="Tier"),
                                    alt.Tooltip("Count:Q", title="Stations"),
                                    alt.Tooltip("Pct:Q",   title="%", format=".1f"),
                                ],
                            )
                            labels = alt.Chart(df_cap).mark_text(
                                radius=112, fontSize=11,
                                fontWeight="bold", color="#e2e8f0",
                            ).encode(
                                theta=alt.Theta("Count:Q", stack=True),
                                text=alt.Text("Pct:Q", format=".0f"),
                            )
                            st.altair_chart(
                                alt.layer(donut, labels).properties(height=320),
                                use_container_width=True,
                            )
                    st.markdown('</div>', unsafe_allow_html=True)

                with col4:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">Station Stats</div>', unsafe_allow_html=True)
                    if neighborhoods_data:
                        total_s   = nbhd_bb.get("total_stations", 0) or 0
                        total_d   = nbhd_bb.get("total_docks", 0) or 0
                        avg_d     = nbhd_bb.get("avg_docks_per_station", 0) or 0
                        per_sqmi  = nbhd_bb.get("stations_per_sqmile", 0) or 0

                        for label, val, sub in [
                            ("Total Stations",        f'{total_s}',          "In this neighborhood"),
                            ("Total Docks",           f'{total_d}',          "Bike capacity"),
                            ("Avg Docks / Station",   f'{avg_d:.1f}',        "Station size average"),
                            ("Stations / Sq Mile",    f'{per_sqmi:.2f}',     "Coverage density"),
                            ("Large Stations",        f'{nbhd_bb.get("large_stations",0) or 0}',  "≥23 docks"),
                            ("Medium Stations",       f'{nbhd_bb.get("medium_stations",0) or 0}', "15–22 docks"),
                            ("Small Stations",        f'{nbhd_bb.get("small_stations",0) or 0}',  "<15 docks"),
                        ]:
                            st.markdown(
                                f'<div style="display:flex;justify-content:space-between;'
                                f'align-items:center;padding:6px 0;'
                                f'border-bottom:1px solid rgba(255,255,255,0.06);">'
                                f'<div>'
                                f'<div style="color:#e2e8f0;font-size:12px;">{label}</div>'
                                f'<div style="color:rgba(255,255,255,0.3);font-size:10px;">{sub}</div>'
                                f'</div>'
                                f'<span style="color:#34d399;font-size:14px;font-weight:700;">{val}</span>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )
                    st.markdown('</div>', unsafe_allow_html=True)

        # ── Generic fallback ──────────────────────────────────────────────────
        else:
            st.info(f"Domain deep-dive for **{domain_filter}** — data loaded. Custom charts coming soon.")
            if domain_data:
                first_key = next((k for k in domain_data if isinstance(domain_data[k], list)
                                  and domain_data[k]), None)
                if first_key:
                    st.dataframe(pd.DataFrame(domain_data[first_key]),
                                 use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════════════════════════
    # MODE 1: No neighborhood, no domain → Home Page
    # ══════════════════════════════════════════════════════════════════════════
    else:
        render_metric_cards([
            ("Neighborhoods",   "51",          "Boston · Cambridge · Suburbs"),
            ("Safest",          safest_name,   f"Grade: {safest_list[0].get('grade', '—')} · Lowest crime rate" if safest_list else "—"),
            ("Most Affordable", afford_name,   afford_sub),
            ("Best Transit",    best_transit_name, transit_sub[:60] if transit_sub else "Excellent coverage"),
            ("Crime Trend",     f"{safer_pct}% holding steady", f"📉 {n_dec} improving · ➡️ {n_stable} stable · 📈 {n_inc} rising"),
        ])
        # Row 1: Map + Safety donut
        col_map, col_safe = st.columns([1.2, 1], gap="medium")

        with col_map:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown(
                '<div class="section-title">Neighborhood Safety Score — Boston & Cambridge</div>'
                '<div class="section-subtitle">Green = safer · Red = higher concern · Hover for details</div>',
                unsafe_allow_html=True,
            )
            map_data = load_map()
            features = map_data.get("features", [])
            if features:
                import pydeck as pdk

                for f in features:
                    grade = f["properties"].get("safety_grade", "")
                    f["properties"]["fill_color"] = SAFETY_COLORS.get(
                        str(grade).strip().upper(), [160, 160, 160, 140]
                    )

                geojson = {"type": "FeatureCollection", "features": features}
                lats = [f["properties"]["latitude"] for f in features if f["properties"].get("latitude")]
                lngs = [f["properties"]["longitude"] for f in features if f["properties"].get("longitude")]

                layer = pdk.Layer(
                    "GeoJsonLayer", data=geojson,
                    filled=True, stroked=True, pickable=True, auto_highlight=True,
                    get_fill_color="properties.fill_color",
                    get_line_color=[255, 255, 255, 60],   # softer white borders
                    line_width_min_pixels=1,
                    line_width_max_pixels=2,
                )

                view = pdk.ViewState(
                    latitude=42.35,
                    longitude=-71.08,
                    zoom=10.2,      # zoomed out more to show all 51
                    pitch=0,
                    bearing=0,
                )
                deck = pdk.Deck(
                    layers=[layer],
                    initial_view_state=view,
                    tooltip={
                        "html": "<b style='font-size:13px'>{neighborhood}</b><br/>"
                                "<span style='color:#52b788'>Safety: <b>{safety_score}</b>/100 · {safety_grade}</span><br/>"
                                "Overall: <b>{master_score}</b>/100 · {master_grade}<br/>"
                                "<span style='opacity:0.7'>↑ {top_strength} · ↓ {top_weakness}</span>",
                        "style": {
                            "backgroundColor": "#1e293b",
                            "color": "#e2e8f0",
                            "fontSize": "12px",
                            "borderRadius": "10px",
                            "padding": "10px 14px",
                            "boxShadow": "0 4px 12px rgba(0,0,0,0.4)",
                        },
                    },
                    map_style="mapbox://styles/mapbox/dark-v10",
                )
                st.pydeck_chart(deck, use_container_width=True, height=520)
                l1, l2, l3, l4 = st.columns(4)
                l1.markdown('<span style="color:#1E8449;">■</span> **Excellent**', unsafe_allow_html=True)
                l2.markdown('<span style="color:#82E0AA;">■</span> **Good**',      unsafe_allow_html=True)
                l3.markdown('<span style="color:#F1C40F;">■</span> **Moderate**',  unsafe_allow_html=True)
                l4.markdown('<span style="color:#C0392B;">■</span> **High Concern**', unsafe_allow_html=True)
            else:
                st.info("Map data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        with col_safe:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown(
                '<div class="section-title">Safety Grade Distribution</div>'
                '<div class="section-subtitle">All 50 neighborhoods · How Boston stacks up on safety</div>',
                unsafe_allow_html=True,
            )
            # FIX: always load city-wide safety (no hood_filter)
            domain_safety = load_domain("safety")
            all_scores    = domain_safety.get("scores", [])
            if all_scores:
                df_all      = pd.DataFrame(all_scores)
                grade_counts = df_all["safety_grade"].value_counts().reset_index()
                grade_counts.columns = ["Grade", "Count"]
                grade_order  = ["EXCELLENT", "GOOD", "MODERATE", "HIGH CONCERN"]
                grade_colors = ["#1E8449", "#82E0AA", "#F1C40F", "#C0392B"]
                donut = alt.Chart(grade_counts).mark_arc(
                    innerRadius=70, outerRadius=130, stroke="#1a1a2e", strokeWidth=2,
                ).encode(
                    theta=alt.Theta("Count:Q", stack=True),
                    color=alt.Color("Grade:N",
                                    scale=alt.Scale(domain=grade_order, range=grade_colors),
                                    legend=alt.Legend(title=None, orient="bottom",
                                                      direction="horizontal", labelFontSize=12)),
                    order=alt.Order("Count:Q", sort="descending"),
                    tooltip=[alt.Tooltip("Grade:N", title="Grade"),
                             alt.Tooltip("Count:Q", title="Neighborhoods")],
                )
                grade_counts["Pct"] = (
                    grade_counts["Count"] / grade_counts["Count"].sum() * 100
                ).round(0).astype(int).astype(str) + "%"
                labels = alt.Chart(grade_counts).mark_text(
                    radius=155, fontSize=14, fontWeight="bold", color="#e2e8f0",
                ).encode(
                    theta=alt.Theta("Count:Q", stack=True),
                    order=alt.Order("Count:Q", sort="descending"),
                    text=alt.Text("Pct:N"),
                )
                st.altair_chart(
                    alt.layer(donut, labels).properties(height=380, width=380),
                    use_container_width=True,
                )
                top3 = safest_list[:3]
                st.markdown('<div style="margin-top:8px;">', unsafe_allow_html=True)
                for i, n in enumerate(top3):
                    medal = ["🥇", "🥈", "🥉"][i]
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;'
                        f'padding:5px 0;border-bottom:1px solid rgba(255,255,255,0.06);">'
                        f'<span style="color:#e2e8f0;font-size:13px;">{medal} {n["neighborhood"]}</span>'
                        f'</div>', unsafe_allow_html=True,
                    )
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("Safety data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        # Row 2: Most affordable + Best transit
        col_afford, col_transit = st.columns(2, gap="medium")

        with col_afford:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown(
                '<div class="section-title">Most Affordable — Top 10</div>'
                '<div class="section-subtitle">Higher score = more affordable relative to Boston market</div>',
                unsafe_allow_html=True,
            )
            if affordable_list:
                df_afford = pd.DataFrame(affordable_list)
                bars = alt.Chart(df_afford).mark_bar(
                    cornerRadiusTopRight=5, cornerRadiusBottomRight=5,
                ).encode(
                    y=alt.Y("neighborhood:N", sort=None,
                            axis=alt.Axis(title=None, labelFontSize=11,
                                          labelLimit=160, labelFontWeight="bold")),
                    x=alt.X("score:Q", scale=alt.Scale(domain=[0, 100]),
                            axis=alt.Axis(title="Affordability Score", grid=True)),
                    color=alt.Color("score:Q",
                                    scale=alt.Scale(domain=[60, 80], range=["#95d5b2", "#1b4332"]),
                                    legend=None),
                    tooltip=["neighborhood:N",
                             alt.Tooltip("score:Q", format=".1f"), "grade:N",
                             alt.Tooltip("avg_monthly_rent:Q", title="Avg Rent $", format=",.0f"),
                             alt.Tooltip("price_per_sqft:Q", title="$/sqft", format=".2f")],
                )
                labels = alt.Chart(df_afford).mark_text(
                    align="left", dx=4, fontSize=11, fontWeight="bold", color="#e2e8f0",
                ).encode(
                    y=alt.Y("neighborhood:N", sort=None),
                    x=alt.X("score:Q"),
                    text=alt.Text("score:Q", format=".0f"),
                )
                st.altair_chart(alt.layer(bars, labels).properties(height=380), use_container_width=True)
            else:
                st.info("Housing data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        with col_transit:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.markdown(
                '<div class="section-title">Best Transit — Top 5</div>'
                '<div class="section-subtitle">Rapid transit lines · Stop coverage</div>',
                unsafe_allow_html=True,
            )
            if transit_list:
                LINE_PILL_COLORS = {
                    "red":      ("#ef4444", "#fff"), "green":    ("#22c55e", "#fff"),
                    "orange":   ("#f97316", "#fff"), "blue":     ("#3b82f6", "#fff"),
                    "mattapan": ("#ef4444", "#fff"), "silver":   ("#94a3b8", "#fff"),
                }
                def line_pills(lines_str):
                    if not lines_str:
                        return '<span style="color:rgba(255,255,255,0.3);font-size:11px;">Bus only</span>'
                    pills = ""
                    for line in lines_str.split(","):
                        line = line.strip()
                        color, text_color = "#64748b", "#fff"
                        for key, (bg, fg) in LINE_PILL_COLORS.items():
                            if key in line.lower(): color, text_color = bg, fg; break
                        pills += (f'<span style="background:{color};color:{text_color};'
                                  f'padding:2px 8px;border-radius:999px;font-size:10px;'
                                  f'font-weight:700;margin-right:4px;">{line}</span>')
                    return pills
                transit_sorted = sorted(
                    transit_list,
                    key=lambda x: (x.get("score", 0) or 0, x.get("total_routes", 0) or 0),
                    reverse=True,
                )[:5]
                for n in transit_sorted:
                    lines  = n.get("rapid_transit_lines")
                    routes = n.get("total_routes", "—")
                    st.markdown(
                        f'<div style="padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.06);">'
                        f'<div style="font-weight:600;font-size:13px;color:#e2e8f0;margin-bottom:4px;">{n["neighborhood"]}</div>'
                        f'<div>{line_pills(lines)}</div>'
                        f'<div style="color:rgba(255,255,255,0.3);font-size:10px;margin-top:3px;">{routes} routes</div>'
                        f'</div>', unsafe_allow_html=True,
                    )
            else:
                st.info("Transit data not available.")
            st.markdown('</div>', unsafe_allow_html=True)

        # Row 3: Best overall livability heatmap
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Best Overall Livability — Top 10</div>'
            '<div class="section-subtitle">Weighted composite across 9 domains</div>',
            unsafe_allow_html=True,
        )
        if overall_list:
            matrix = load_domain_matrix(limit=15)
            nbhds  = matrix.get("neighborhoods", [])
            if nbhds:
                domain_cols = ["Safety","Housing","Transit","Grocery","Healthcare",
                               "Schools","Restaurants","Universities","Bluebikes"]
                rows = []
                for n in nbhds:
                    for d in domain_cols:
                        rows.append({
                            "neighborhood": n["neighborhood"], "Domain": d,
                            "Score": n.get(d) or 0, "master_score": n.get("master_score", 0),
                            "strength": n.get("top_strength", ""), "weakness": n.get("top_weakness", ""),
                        })
                df_heat = pd.DataFrame(rows)
                heatmap = alt.Chart(df_heat).mark_rect(stroke="#1a1a2e", strokeWidth=1).encode(
                    x=alt.X("Domain:N", sort=domain_cols,
                            axis=alt.Axis(title=None, labelAngle=-30,
                                          labelFontSize=11, labelFontWeight="bold")),
                    y=alt.Y("neighborhood:N",
                            sort=alt.EncodingSortField("master_score", order="descending"),
                            axis=alt.Axis(title=None, labelFontSize=11, labelFontWeight="bold")),
                    color=alt.Color("Score:Q",
                                    scale=alt.Scale(domain=[0, 100], range=["#1e3a5f", "#52b788"]),
                                    legend=alt.Legend(title="Score", orient="right")),
                    tooltip=["neighborhood:N","Domain:N",
                             alt.Tooltip("Score:Q", format=".1f", title="Score"),
                             alt.Tooltip("strength:N", title="Strength"),
                             alt.Tooltip("weakness:N", title="Weakness")],
                )
                text = alt.Chart(df_heat).mark_text(fontSize=10, fontWeight="bold").encode(
                    x=alt.X("Domain:N", sort=domain_cols),
                    y=alt.Y("neighborhood:N",
                            sort=alt.EncodingSortField("master_score", order="descending")),
                    text=alt.Text("Score:Q", format=".0f"),
                    color=alt.condition(
                        alt.datum.Score > 50,
                        alt.value("#0f172a"),
                        alt.value("#e2e8f0"),
                    ),
                )
                st.altair_chart(
                    alt.layer(heatmap, text).properties(height=420),
                    use_container_width=True,
                )
        st.markdown('</div>', unsafe_allow_html=True)
# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — CHATBOT
# ══════════════════════════════════════════════════════════════════════════════
with tab_chat:
    st.markdown(
        '<p style="color:rgba(255,255,255,0.4);font-size:0.72rem;font-weight:600;'
        'letter-spacing:0.05em;text-transform:uppercase;margin-bottom:6px;">'
        'Quick Examples</p>',
        unsafe_allow_html=True,
    )

    for row_ex in [EXAMPLES[:3], EXAMPLES[3:]]:
        cols = st.columns(3)
        for col, (icon, text) in zip(cols, row_ex):
            with col:
                st.markdown('<div class="ex-btn">', unsafe_allow_html=True)
                if st.button(f"{icon} {text}", key=f"ex_{text}", use_container_width=True):
                    st.session_state.prefill = text
                st.markdown('</div>', unsafe_allow_html=True)

    st.divider()

    if "messages" not in st.session_state:
        st.session_state.messages = [{
            "role": "assistant",
            "content": (
                "👋 Hi! I'm **NeighborWise AI** — your Boston neighborhood intelligence assistant.\n\n"
                "Ask me anything: *Which neighborhood is safest for families?*, "
                "*Compare Back Bay and Roxbury*, or *Generate a report for Fenway*."
            ),
            "type": "data_query",
        }]

    for idx, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                render_assistant_message(msg, key_prefix=f"hist_{idx}")
            else:
                st.markdown(msg["content"])

    prefill = st.session_state.pop("prefill", None)
    user_input = st.chat_input("Ask about any Boston neighborhood...")
    if prefill and not user_input:
        user_input = prefill

    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                t_start = time.time()
                payload = {
                    "query": user_input,
                    "domain_filter": domain_filter,
                }
                result = api_post("/query", payload=payload, timeout=300)
                elapsed = time.time() - t_start

            if not result:
                answer_text = "❌ Failed to get a response from the API."
                new_msg = {"role": "assistant", "content": answer_text, "type": "error"}
            else:
                rtype = result.get("type", "data_query")
                new_msg = {
                    "role":        "assistant",
                    "content":     result.get("answer", ""),
                    "type":        rtype,
                    "chart_path":  result.get("chart_path"),
                    "image_paths": result.get("image_paths"),
                    "sql":         result.get("sql"),
                    "results":     result.get("results"),
                    "rag_chunks":  result.get("rag_chunks"),
                    "routing":     result.get("routing"),
                    "elapsed":     result.get("elapsed", elapsed),
                }

                # If report was triggered via chat, stash it for the report tab
                if rtype == "report" and result.get("pdf_path"):
                    st.session_state["last_report"] = result
                    new_msg["content"] += (
                        "\n\n✅ Switch to the **Neighborhood Report** tab to download it."
                    )

                # Log token/cost data — runs for ALL query types
                _log_query_cost(user_input, result, elapsed)

            render_assistant_message(new_msg, key_prefix=f"new_{len(st.session_state.messages)}")
            # Show output guardrail warnings if any issues were detected
            guardrails = result.get("guardrails") if result else None
            if guardrails and isinstance(guardrails, dict):
                issues = guardrails.get("output_issues", [])
                pii    = guardrails.get("pii_detected", False)
                if pii:
                    st.caption("⚠️ PII detected and redacted from this response.")
                if issues and not pii:
                    st.caption(f"⚠️ Output flagged: {', '.join(issues[:2])}")

        st.session_state.messages.append(new_msg)
        _render_cost_tracker()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — NEIGHBORHOOD REPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_report:
    col_left, col_right = st.columns([1, 1], gap="large")

    with col_left:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div style="font-family:DM Serif Display,serif;font-size:1.25rem;'
            'color:#e2e8f0;margin-bottom:5px;">Generate Neighborhood Report</div>'
            '<p style="color:rgba(255,255,255,0.4);font-size:0.85rem;margin-top:0;'
            'margin-bottom:12px;">Full PDF — domain scores, charts, DALL-E visuals, '
            'SARIMAX forecast &amp; lifestyle analysis.</p>',
            unsafe_allow_html=True,
        )

        report_neighborhoods = [n["name"] for n in neighborhoods_raw] if neighborhoods_raw else ["Fenway"]
        default_idx = report_neighborhoods.index("Fenway") if "Fenway" in report_neighborhoods else 0
        selected_report_hood = st.selectbox(
            "Choose a neighborhood",
            options=report_neighborhoods,
            index=default_idx,
            key="report_hood_select",
        )

        st.markdown("<div style='height:5px'></div>", unsafe_allow_html=True)
        for icon, title, desc in REPORT_ITEMS:
            st.markdown(
                f"<div style='display:flex;align-items:center;gap:10px;"
                f"padding:5px 0;border-bottom:1px solid rgba(255,255,255,0.06);'>"
                f"<span style='font-size:15px;width:20px;'>{icon}</span>"
                f"<div><span style='font-weight:600;font-size:11px;color:#e2e8f0;'>{title}</span> "
                f"<span style='font-size:10px;color:rgba(255,255,255,0.35);'>— {desc}</span>"
                f"</div></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="gen-btn">', unsafe_allow_html=True)
        generate = st.button("🚀  Generate Report", use_container_width=True, key="gen_btn")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown(
            "<p style='color:rgba(255,255,255,0.3);font-size:10px;margin-top:5px;"
            "text-align:center;'>⏱ ~3–5 minutes · includes DALL-E image generation</p></div>",
            unsafe_allow_html=True,
        )

    with col_right:
        if generate:
            if "last_report" in st.session_state:
                del st.session_state["last_report"]
            if "report_poll_id" in st.session_state:
                del st.session_state["report_poll_id"]

            # ── Check session cache first ──────────────────────────────────
            cache_key = f"report_cache_{selected_report_hood.lower().replace(' ', '_')}"
            if cache_key in st.session_state:
                # Already generated this session — fake fast progress
                st.markdown(
                    f'<div class="narrative-box-blue">'
                    f'<div class="narrative-title">⚡ Loading cached report for {selected_report_hood}</div>'
                    f'Found from earlier this session — loading instantly.</div>',
                    unsafe_allow_html=True,
                )
                progress = st.progress(0)
                for pct in [30, 60, 90, 100]:
                    time.sleep(0.3)
                    progress.progress(pct, text="✅ Done!" if pct == 100 else "📄 Loading report...")
                st.session_state["last_report"] = st.session_state[cache_key]

            else:
                # Fresh generation
                st.markdown(
                    f'<div class="narrative-box-blue">'
                    f'<div class="narrative-title">⏳ Generating report for {selected_report_hood}</div>'
                    f'This takes 3–5 minutes. Do not close this tab.</div>',
                    unsafe_allow_html=True,
                )

                resp = api_post("/report/generate",
                                payload={"neighborhood": selected_report_hood},
                                timeout=15)
                if resp and resp.get("report_id"):
                    report_id = resp["report_id"]
                    st.session_state["report_poll_id"] = report_id
                    st.session_state["report_poll_hood"] = selected_report_hood

                    progress = st.progress(0)
                    status_ph = st.empty()
                    step_idx = 0
                    max_wait = 400
                    poll_interval = 8
                    elapsed_poll = 0

                    while elapsed_poll < max_wait:
                        time.sleep(poll_interval)
                        elapsed_poll += poll_interval

                        poll = api_get(f"/report/{report_id}")
                        status = poll.get("status", "processing")

                        pct = min(int(elapsed_poll / max_wait * 90), 90)
                        step_label = REPORT_STEPS[min(step_idx, len(REPORT_STEPS) - 1)]
                        progress.progress(pct, text=step_label)
                        step_idx = min(step_idx + 1, len(REPORT_STEPS) - 1)

                        if status == "completed":
                            progress.progress(100, text="✅ Done!")
                            status_ph.empty()
                            st.session_state["last_report"] = poll
                            # ── Save to session cache ──────────────────────
                            st.session_state[cache_key] = poll
                            break
                        elif status == "failed":
                            progress.empty()
                            st.error(f"❌ Report failed: {poll.get('message', 'Unknown error')}")
                            break
                    else:
                        st.warning("⏰ Report is taking longer than expected.")
                else:
                    st.error("❌ Failed to start report generation. Check that the API is running.")

        # Show download if report is ready
        if "last_report" in st.session_state:
            report = st.session_state["last_report"]
            nbhd = report.get("neighborhood", selected_report_hood)

            if report.get("status") == "completed":
                st.markdown(
                    f'<div class="narrative-box">'
                    f'<div class="narrative-title">✅ Report ready — {nbhd}</div>'
                    f'9 domains · 4 charts · 4 DALL-E images · SARIMAX forecast</div>',
                    unsafe_allow_html=True,
                )

                # ── Try direct from disk first (survives FastAPI restarts) ──
                cached_pdf_path = report.get("pdf_path")
                pdf_data = None
                file_name = f"{nbhd.lower().replace(' ', '_')}_report.pdf"

                if cached_pdf_path and Path(cached_pdf_path).exists():
                    with open(cached_pdf_path, "rb") as f:
                        pdf_data = f.read()

                else:
                    # Fallback — try FastAPI download endpoint
                    download_url = f"{API_BASE_URL}{report.get('url', '')}"
                    try:
                        pdf_resp = requests.get(download_url, timeout=30)
                        if pdf_resp.status_code == 200:
                            pdf_data = pdf_resp.content
                        else:
                            # Stale cache — clear it and ask user to regenerate
                            st.warning(
                                "⚠️ Cached report session expired — "
                                "please click Generate Report again."
                            )
                            cache_key = f"report_cache_{nbhd.lower().replace(' ', '_')}"
                            if cache_key in st.session_state:
                                del st.session_state[cache_key]
                            if "last_report" in st.session_state:
                                del st.session_state["last_report"]
                    except Exception as e:
                        st.error(f"Download error: {e}")

                if pdf_data:
                    st.markdown('<div class="dl-btn">', unsafe_allow_html=True)
                    st.download_button(
                        "⬇️  Download PDF Report",
                        data=pdf_data,
                        file_name=file_name,
                        mime="application/pdf",
                        use_container_width=True,
                    )
                    st.markdown('</div>', unsafe_allow_html=True)

        elif not generate:
            st.markdown(
                '<div class="section-card" style="text-align:center;padding:50px 28px;">'
                '<div style="font-size:40px;margin-bottom:8px;">📄</div>'
                '<div style="font-family:DM Serif Display,serif;font-size:1.1rem;'
                'color:#e2e8f0;">Your report will appear here</div>'
                '<div style="color:rgba(255,255,255,0.3);font-size:11px;margin-top:5px;">'
                'Select a neighborhood and click Generate Report</div></div>',
                unsafe_allow_html=True,
            )