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
    page_title="NeighbourWise AI",
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
    border-radius: 14px; border: 1px solid rgba(255,255,255,0.1);
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
.metric-sub { font-size: 0.7rem; color: rgba(255,255,255,0.35); margin-top: 0.15rem; }

.section-card {
    background: rgba(255,255,255,0.04); padding: 1.1rem 1.2rem 0.9rem;
    border-radius: 14px; border: 1px solid rgba(255,255,255,0.08);
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
    <div class="hero-title">NeighbourWise AI — Neighborhood Intelligence</div>
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
        'color:#e2e8f0;margin-bottom:3px;">NeighbourWise AI</div>'
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
    "🏠  Overview", "💬  Ask NeighbourWise", "📄  Neighborhood Report",
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

    # KPI cards always city-wide regardless of sidebar selection
    if not hood_filter:
        render_metric_cards([
            ("Neighborhoods",   "51",          "Boston · Cambridge · Suburbs"),
            ("Safest",          safest_name,   f"Grade: {safest_list[0].get('grade', '—')} · Lowest crime rate" if safest_list else "—"),
            ("Most Affordable", afford_name,   afford_sub),
            ("Best Transit",    best_transit_name, transit_sub[:60] if transit_sub else "Excellent coverage"),
            ("Crime Trend",     f"{safer_pct}% holding steady", f"📉 {n_dec} improving · ➡️ {n_stable} stable · 📈 {n_inc} rising"),
        ])

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
                color=alt.Color("color:N", scale=None, legend=None),
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

            st.markdown(
                f'<div class="narrative-box" style="margin-top:8px;">'
                f'<b>{hood_filter}</b> scores better than <b>{better_count} of its {len(neighbors)} '
                f'nearest neighbors</b>. '
                f'Top performer nearby: <b>{best_nb["neighborhood"]}</b> '
                f'({int(best_nb["master_score"] or 0)}) · '
                f'Lowest: <b>{worst_nb["neighborhood"]}</b> '
                f'({int(worst_nb["master_score"] or 0)}).'
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

            col1, col2 = st.columns(2, gap="medium")
            with col1:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Safety Scores</div>', unsafe_allow_html=True)
                if scores:
                    df_s = pd.DataFrame(scores[:20])
                    bars = alt.Chart(df_s).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4,
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("safety_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="Safety Score")),
                        color=alt.Color("safety_grade:N",
                                        scale=alt.Scale(
                                            domain=["EXCELLENT","GOOD","MODERATE","HIGH CONCERN"],
                                            range=["#1E8449","#82E0AA","#F1C40F","#C0392B"]),
                                        legend=None),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("safety_score:Q", format=".1f"),
                                 "safety_grade:N",
                                 alt.Tooltip("yoy_change_pct:Q", title="YoY Change %", format=".1f"),
                                 "most_common_offense:N"],
                    )
                    st.altair_chart(bars.properties(height=500), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            with col2:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">DBSCAN Hotspot Clusters</div>', unsafe_allow_html=True)
                if hotspots:
                    df_h = pd.DataFrame(hotspots[:20])
                    bars = alt.Chart(df_h).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#E45756",
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("hotspot_crime_share_pct:Q",
                                axis=alt.Axis(title="% Crimes in Hotspots")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("hotspot_clusters:Q", title="Clusters"),
                                 alt.Tooltip("hotspot_crime_share_pct:Q", format=".1f", title="Crime Share %"),
                                 alt.Tooltip("total_crimes:Q", title="Total Crimes")],
                    )
                    st.altair_chart(bars.properties(height=500), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            if forecasts:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">SARIMAX Forecasts — Next 6 Months</div>',
                            unsafe_allow_html=True)
                df_f = pd.DataFrame(forecasts)
                high_hoods = {n["neighborhood"] for n in narrative if n.get("reliability") == "HIGH"}
                if high_hoods:
                    df_f = df_f[df_f["neighborhood"].isin(high_hoods)]
                line = alt.Chart(df_f).mark_line(point=True).encode(
                    x=alt.X("forecast_month:N", axis=alt.Axis(title="Month", labelAngle=-30)),
                    y=alt.Y("forecasted_count:Q", axis=alt.Axis(title="Forecasted Crimes")),
                    color=alt.Color("neighborhood:N", legend=alt.Legend(title="Neighborhood")),
                    tooltip=["neighborhood:N","forecast_month:N",
                             alt.Tooltip("forecasted_count:Q", title="Forecast"),
                             alt.Tooltip("lower_ci:Q", title="Lower CI"),
                             alt.Tooltip("upper_ci:Q", title="Upper CI")],
                )
                st.altair_chart(line.properties(height=350), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            if narrative:
                with st.expander("📋 Full Safety Narrative Table", expanded=False):
                    df_n = pd.DataFrame(narrative)[
                        ["neighborhood","recent_trend","recent_avg_monthly",
                         "forecasted_count","hotspot_clusters","reliability"]
                    ]
                    df_n.columns = ["Neighborhood","Trend","Avg Monthly",
                                    "Forecast","Hotspot Clusters","Reliability"]
                    st.dataframe(df_n, use_container_width=True, hide_index=True)

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
            mbta     = domain_data.get("mbta", [])
            bikes    = domain_data.get("bluebikes", [])
            summary  = domain_data.get("summary", {})
            map_data = load_map()
            features = map_data.get("features", [])

            st.markdown(
                f'<div class="narrative-box">'
                f'{summary.get("neighborhoods_with_rapid_transit", 0)} neighborhoods have rapid transit · '
                f'{summary.get("neighborhoods_with_commuter_rail", 0)} have commuter rail · '
                f'Avg MBTA score: {summary.get("avg_transit_score", "—")} · '
                f'Avg BlueBikes score: {summary.get("avg_bikeshare_score", "—")}'
                f'</div>',
                unsafe_allow_html=True,
            )

            if mbta and features:
                import pydeck as pdk
                TRANSIT_FILL = {
                    "EXCELLENT": [29,  78,  216, 200],
                    "GOOD":      [96,  165, 250, 180],
                    "MODERATE":  [186, 230, 253, 160],
                    "LIMITED":   [71,  85,  105, 140],
                }
                centroid_lookup = {
                    f["properties"]["neighborhood"].upper(): {
                        "lat": f["properties"]["latitude"],
                        "lng": f["properties"]["longitude"],
                    }
                    for f in features if f["properties"].get("latitude")
                }
                transit_features = []
                for f in features:
                    nbhd = f["properties"]["neighborhood"].upper()
                    mbta_row = next((r for r in mbta if r["neighborhood"].upper() == nbhd), None)
                    if not mbta_row: continue
                    grade = str(mbta_row.get("transit_grade", "MODERATE")).upper()
                    lines = mbta_row.get("rapid_transit_lines") or "Bus only"
                    transit_features.append({
                        "type": "Feature", "geometry": f["geometry"],
                        "properties": {
                            "neighborhood":  f["properties"]["neighborhood"],
                            "transit_score": mbta_row.get("transit_score", 0),
                            "transit_grade": grade, "lines": lines,
                            "total_stops":   mbta_row.get("total_stops", 0),
                            "rapid_stops":   mbta_row.get("rapid_transit_stops", 0),
                            "total_routes":  mbta_row.get("total_routes", 0),
                            "accessible_pct":mbta_row.get("pct_accessible_stops", 0),
                            "fill_color":    TRANSIT_FILL.get(grade, [100, 100, 100, 140]),
                        }
                    })
                LINE_COLORS = {
                    "Red Line":   [239, 68,  68,  230], "Green":      [34,  197, 94,  230],
                    "Blue Line":  [59,  130, 246, 230], "Orange Line":[249, 115, 22,  230],
                    "Mattapan":   [239, 68,  68,  200], "Silver":     [148, 163, 184, 200],
                }
                scatter_data = []
                for r in mbta:
                    coords = centroid_lookup.get(r["neighborhood"].upper())
                    if not coords: continue
                    lines = r.get("rapid_transit_lines") or ""
                    circle_color = [148, 163, 184, 200]
                    for line_key, color in LINE_COLORS.items():
                        if line_key.lower() in lines.lower(): circle_color = color; break
                    scatter_data.append({
                        "name": r["neighborhood"], "lat": coords["lat"], "lng": coords["lng"],
                        "transit_score": r.get("transit_score", 0) or 0,
                        "transit_grade": r.get("transit_grade", ""),
                        "lines": lines if lines else "Bus only",
                        "total_stops": r.get("total_stops", 0) or 0,
                        "rapid_stops": r.get("rapid_transit_stops", 0) or 0,
                        "total_routes": r.get("total_routes", 0) or 0,
                        "accessible_pct": r.get("pct_accessible_stops", 0) or 0,
                        "color": circle_color,
                        "radius": max(200, min(600, (r.get("rapid_transit_stops", 0) or 0) * 80 + 200)),
                    })
                geojson_layer = pdk.Layer(
                    "GeoJsonLayer",
                    data={"type": "FeatureCollection", "features": transit_features},
                    filled=True, stroked=True, pickable=True, auto_highlight=True,
                    get_fill_color="properties.fill_color",
                    get_line_color=[255, 255, 255, 60], line_width_min_pixels=1,
                )
                scatter_layer = pdk.Layer(
                    "ScatterplotLayer", data=pd.DataFrame(scatter_data),
                    get_position=["lng", "lat"], get_fill_color="color",
                    get_radius="radius", pickable=True, auto_highlight=True, opacity=0.9,
                )
                lats = [d["lat"] for d in scatter_data]
                lngs = [d["lng"] for d in scatter_data]
                view = pdk.ViewState(
                    latitude=sum(lats)/len(lats) if lats else 42.36,
                    longitude=sum(lngs)/len(lngs) if lngs else -71.06,
                    zoom=10.5, pitch=0,
                )
                deck = pdk.Deck(
                    layers=[geojson_layer, scatter_layer], initial_view_state=view,
                    tooltip={
                        "html": "<b>{name}</b><br/>Score: <b>{transit_score}</b>/100 · <b>{transit_grade}</b><br/>"
                                "Lines: <b>{lines}</b><br/>Stops: {total_stops} total · {rapid_stops} rapid<br/>"
                                "Routes: {total_routes} · Accessible: {accessible_pct}%",
                        "style": {"backgroundColor":"#1e293b","color":"#e2e8f0",
                                  "fontSize":"12px","borderRadius":"8px","padding":"8px"},
                    },
                    map_style="mapbox://styles/mapbox/dark-v10",
                )
                st.pydeck_chart(deck, use_container_width=True, height=540)
                st.markdown(
                    '<div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:8px;">'
                    '<span style="color:#1d4ed8;font-weight:600;">■ Excellent</span>'
                    '<span style="color:#60a5fa;font-weight:600;">■ Good</span>'
                    '<span style="color:#bae6fd;font-weight:600;">■ Moderate</span>'
                    '<span style="color:#475569;font-weight:600;">■ Limited</span>'
                    '&nbsp;&nbsp;|&nbsp;&nbsp;'
                    '<span style="color:#ef4444;">● Red Line</span>'
                    '<span style="color:#22c55e;">● Green Line</span>'
                    '<span style="color:#3b82f6;">● Blue Line</span>'
                    '<span style="color:#f97316;">● Orange Line</span>'
                    '<span style="color:#94a3b8;">● Bus/Other</span>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.info("Transit map data not available.")

            col1, col2 = st.columns(2, gap="medium")
            with col1:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">MBTA Transit Scores</div>', unsafe_allow_html=True)
                if mbta:
                    df_t = pd.DataFrame(mbta[:20])
                    bars = alt.Chart(df_t).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#60a5fa",
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("transit_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="Transit Score")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("transit_score:Q", format=".1f"),
                                 "transit_grade:N", "rapid_transit_lines:N",
                                 alt.Tooltip("total_routes:Q", title="Routes"),
                                 alt.Tooltip("pct_accessible_stops:Q", title="% Accessible", format=".1f")],
                    )
                    st.altair_chart(bars.properties(height=480), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            with col2:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Stop Type Breakdown — Top 15</div>', unsafe_allow_html=True)
                if mbta:
                    df_t = pd.DataFrame(mbta[:15])
                    df_melt = df_t[["neighborhood","rapid_transit_stops","commuter_rail_stops","bus_stops"]].melt(
                        id_vars="neighborhood", var_name="Stop Type", value_name="Count"
                    )
                    stacked = alt.Chart(df_melt).mark_bar().encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=140)),
                        x=alt.X("Count:Q", axis=alt.Axis(title="Stop Count")),
                        color=alt.Color("Stop Type:N",
                                        scale=alt.Scale(
                                            domain=["rapid_transit_stops","commuter_rail_stops","bus_stops"],
                                            range=["#60a5fa","#a78bfa","#34d399"]),
                                        legend=alt.Legend(title=None, orient="bottom")),
                        tooltip=["neighborhood:N","Stop Type:N","Count:Q"],
                    )
                    st.altair_chart(stacked.properties(height=480), use_container_width=True)
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

            st.markdown(
                f'<div class="narrative-box">'
                f'Total stations: <b>{summary.get("total_stations_citywide", "—")}</b> · '
                f'Total docks: <b>{summary.get("total_docks_citywide", "—")}</b>'
                f'</div>', unsafe_allow_html=True,
            )
            if neighborhoods_data:
                df_bb = pd.DataFrame(neighborhoods_data[:20])
                col1, col2 = st.columns(2, gap="medium")
                with col1:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">BlueBikes Scores</div>', unsafe_allow_html=True)
                    bars = alt.Chart(df_bb).mark_bar(
                        cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#34d399",
                    ).encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=160)),
                        x=alt.X("bikeshare_score:Q", scale=alt.Scale(domain=[0, 100]),
                                axis=alt.Axis(title="BlueBikes Score")),
                        tooltip=["neighborhood:N",
                                 alt.Tooltip("bikeshare_score:Q", format=".1f"),
                                 alt.Tooltip("total_stations:Q", title="Stations"),
                                 alt.Tooltip("total_docks:Q", title="Docks"),
                                 alt.Tooltip("stations_per_sqmile:Q", format=".2f", title="Stations/sqmi")],
                    )
                    st.altair_chart(bars.properties(height=480), use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="section-card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">Station Size Mix</div>', unsafe_allow_html=True)
                    df_melt = df_bb[["neighborhood","large_stations","medium_stations","small_stations"]].melt(
                        id_vars="neighborhood", var_name="Size", value_name="Count"
                    )
                    stacked = alt.Chart(df_melt).mark_bar().encode(
                        y=alt.Y("neighborhood:N", sort=None,
                                axis=alt.Axis(title=None, labelFontSize=10, labelLimit=140)),
                        x=alt.X("Count:Q", axis=alt.Axis(title="Station Count")),
                        color=alt.Color("Size:N",
                                        scale=alt.Scale(
                                            domain=["large_stations","medium_stations","small_stations"],
                                            range=["#34d399","#60a5fa","#a78bfa"]),
                                        legend=alt.Legend(title=None, orient="bottom")),
                        tooltip=["neighborhood:N","Size:N","Count:Q"],
                    )
                    st.altair_chart(stacked.properties(height=480), use_container_width=True)
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
                    get_line_color=[255, 255, 255, 120], line_width_min_pixels=1,
                )
                view = pdk.ViewState(
                    latitude=sum(lats)/len(lats) if lats else 42.36,
                    longitude=sum(lngs)/len(lngs) if lngs else -71.06,
                    zoom=10.8, pitch=0,
                )
                deck = pdk.Deck(
                    layers=[layer], initial_view_state=view,
                    tooltip={
                        "html": "<b>{neighborhood}</b><br/>"
                                "Safety: <b>{safety_score}</b>/100 ({safety_grade})<br/>"
                                "Overall: <b>{master_score}</b>/100 ({master_grade})<br/>"
                                "Strength: {top_strength} · Weakness: {top_weakness}",
                        "style": {"backgroundColor":"#1e293b","color":"#e2e8f0",
                                  "fontSize":"12px","borderRadius":"8px","padding":"8px"},
                    },
                    map_style="mapbox://styles/mapbox/dark-v10",
                )
                st.pydeck_chart(deck, use_container_width=True, height=500)
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
                "👋 Hi! I'm **NeighbourWise AI** — your Boston neighborhood intelligence assistant.\n\n"
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

            render_assistant_message(new_msg, key_prefix=f"new_{len(st.session_state.messages)}")

        st.session_state.messages.append(new_msg)


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
            # Clear any previous report
            if "last_report" in st.session_state:
                del st.session_state["last_report"]
            if "report_poll_id" in st.session_state:
                del st.session_state["report_poll_id"]

            st.markdown(
                f'<div class="narrative-box-blue">'
                f'<div class="narrative-title">⏳ Generating report for {selected_report_hood}</div>'
                f'This takes 3–5 minutes. Do not close this tab.</div>',
                unsafe_allow_html=True,
            )

            # Kick off async generation
            resp = api_post("/report/generate",
                            payload={"neighborhood": selected_report_hood},
                            timeout=15)
            if resp and resp.get("report_id"):
                report_id = resp["report_id"]
                st.session_state["report_poll_id"] = report_id
                st.session_state["report_poll_hood"] = selected_report_hood

                # Poll with progress bar
                progress = st.progress(0)
                status_ph = st.empty()
                step_idx = 0
                max_wait = 400  # seconds
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
                        break
                    elif status == "failed":
                        progress.empty()
                        st.error(f"❌ Report failed: {poll.get('message', 'Unknown error')}")
                        break
                else:
                    st.warning("⏰ Report is taking longer than expected. "
                               "Check back in a moment — it may still complete.")
            else:
                st.error("❌ Failed to start report generation. Check that the API is running.")

        # Show download if report is ready
        if "last_report" in st.session_state:
            report = st.session_state["last_report"]
            nbhd = report.get("neighborhood", selected_report_hood)

            if report.get("status") == "completed" and report.get("url"):
                st.markdown(
                    f'<div class="narrative-box">'
                    f'<div class="narrative-title">✅ Report ready — {nbhd}</div>'
                    f'9 domains · 4 charts · 4 DALL-E images · SARIMAX forecast</div>',
                    unsafe_allow_html=True,
                )

                # Download via FastAPI endpoint
                download_url = f"{API_BASE_URL}{report['url']}"
                try:
                    pdf_resp = requests.get(download_url, timeout=30)
                    if pdf_resp.status_code == 200:
                        st.markdown('<div class="dl-btn">', unsafe_allow_html=True)
                        st.download_button(
                            "⬇️  Download PDF Report",
                            data=pdf_resp.content,
                            file_name=f"{nbhd.lower().replace(' ', '_')}_report.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )
                        st.markdown('</div>', unsafe_allow_html=True)
                    else:
                        st.error("PDF download failed. Try refreshing.")
                except Exception as e:
                    st.error(f"Download error: {e}")

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