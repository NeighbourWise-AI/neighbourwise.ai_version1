"""
neighbourwise_app.py — NeighbourWise AI
Three tabs:
  🏠 Overview   — curated stats, safety map, crime forecasts, leaderboard
  💬 Ask        — chatbot (SQL + RAG + validator)
  📄 Report     — PDF report generator

Run:
    streamlit run neighbourwise_app.py
"""

import streamlit as st
import sys
import time
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).resolve().parent))

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NeighbourWise AI",
    page_icon="🏘️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# CSS — works WITH dark theme instead of fighting it
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.block-container { padding: 1rem 2rem 2rem 2rem; }
#MainMenu, footer, header { visibility: hidden; }

/* Hero */
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

/* Metric cards — theme-aware */
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
    font-family: 'DM Serif Display', serif; font-size: 1.5rem;
    color: #e2e8f0; line-height: 1.15;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.metric-sub { font-size: 0.7rem; color: rgba(255,255,255,0.35); margin-top: 0.15rem; }

/* Section card — semi-transparent on dark */
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

/* Narrative boxes */
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

/* Tabs */
.stTabs [data-baseweb="tab-list"] { gap: 6px; }
.stTabs [data-baseweb="tab"] {
    height: 42px; padding: 0 18px; border-radius: 10px;
    font-family: 'DM Sans', sans-serif; font-size: 13px; font-weight: 500;
}
.stTabs [aria-selected="true"] {
    background-color: rgba(96,165,250,0.15) !important;
    font-weight: 700 !important;
}

/* Buttons */
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

/* Score badges */
.score-badge {
    display: inline-block; padding: 3px 10px;
    border-radius: 999px; font-size: 0.72rem; font-weight: 600;
}
.badge-data   { background: rgba(59,130,246,0.2); color: #93c5fd; }
.badge-chart  { background: rgba(16,185,129,0.2); color: #6ee7b7; }
.badge-web    { background: rgba(245,158,11,0.2); color: #fcd34d; }
.badge-report { background: rgba(139,92,246,0.2); color: #c4b5fd; }
.badge-image  { background: rgba(236,72,153,0.2); color: #f9a8d4; }

/* Chart preview grid */
.chart-grid-card {
    background: rgba(255,255,255,0.04); border-radius: 14px;
    border: 1px solid rgba(255,255,255,0.08);
    padding: 10px; margin-bottom: 10px; overflow: hidden;
}
.chart-grid-label {
    font-size: 0.72rem; font-weight: 600; color: rgba(255,255,255,0.45);
    text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 6px;
}

/* Bottom 5 mini-stat rows */
.mini-stat {
    display: flex; align-items: center; justify-content: space-between;
    padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.06);
}
.mini-stat:last-child { border-bottom: none; }
.mini-stat-name { font-size: 0.85rem; font-weight: 500; color: #cbd5e1; }
.mini-stat-value {
    font-family: 'DM Serif Display', serif; font-size: 0.95rem; color: #e2e8f0;
}
.mini-stat-badge {
    display: inline-block; padding: 2px 8px; border-radius: 999px;
    font-size: 0.65rem; font-weight: 600; margin-left: 6px;
}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# CACHED RESOURCES
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_cached_conn():
    from shared.snowflake_conn import get_conn
    return get_conn()

@st.cache_data(ttl=3600)
def get_neighborhood_list():
    from shared.snowflake_conn import get_conn, get_all_neighborhoods
    conn = get_conn()
    try:
        return sorted(get_all_neighborhoods(conn))
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def load_overview_stats():
    from shared.snowflake_conn import get_conn, run_query
    conn = get_conn()
    try:
        df = run_query("""
            SELECT COUNT(*) AS TOTAL_NEIGHBORHOODS,
                   ROUND(AVG(MASTER_SCORE),1) AS AVG_MASTER_SCORE,
                   MAX(MASTER_SCORE) AS TOP_SCORE
            FROM NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
            WHERE MASTER_SCORE IS NOT NULL
        """, conn)
        result = df.iloc[0].to_dict() if not df.empty else {}
        # Safest neighborhood
        df2 = run_query("""
            SELECT NEIGHBORHOOD_NAME, SAFETY_SCORE
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
            WHERE SAFETY_SCORE IS NOT NULL
            ORDER BY SAFETY_SCORE DESC LIMIT 1
        """, conn)
        if not df2.empty:
            result["SAFEST_NAME"] = df2.iloc[0]["NEIGHBORHOOD_NAME"]
            result["SAFEST_SCORE"] = df2.iloc[0]["SAFETY_SCORE"]
        # Most affordable — use HOUSING_SCORE (higher = more affordable)
        df3 = run_query("""
            SELECT NEIGHBORHOOD_NAME, HOUSING_SCORE, HOUSING_GRADE, AVG_ESTIMATED_RENT
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING
            WHERE HOUSING_SCORE IS NOT NULL
            ORDER BY HOUSING_SCORE DESC
            LIMIT 1
        """, conn)
        if not df3.empty:
            result["CHEAPEST_NAME"] = df3.iloc[0]["NEIGHBORHOOD_NAME"]
            result["CHEAPEST_RENT"] = df3.iloc[0].get("AVG_ESTIMATED_RENT")
            result["CHEAPEST_GRADE"] = df3.iloc[0].get("HOUSING_GRADE", "")
        return result
    except Exception as e:
        print(f"[Overview] Stats error: {e}")
        return {}
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def load_safety_overview():
    from shared.snowflake_conn import get_conn, run_query
    conn = get_conn()
    try:
        return run_query("""
            SELECT NEIGHBORHOOD_NAME, SAFETY_SCORE, SAFETY_GRADE,
                   TOTAL_INCIDENTS, VIOLENT_CRIME_COUNT
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
            WHERE SAFETY_SCORE IS NOT NULL
            ORDER BY SAFETY_SCORE DESC
        """, conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def load_crime_hotspots():
    from shared.snowflake_conn import get_conn, run_query
    conn = get_conn()
    try:
        return run_query("""
            SELECT NEIGHBORHOOD_NAME, N_HOTSPOT_CLUSTERS,
                   HOTSPOT_CRIME_SHARE_PCT, TOTAL_CRIMES
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_HOTSPOT_CLUSTERS
            ORDER BY HOTSPOT_CRIME_SHARE_PCT DESC
        """, conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def load_master_scores():
    from shared.snowflake_conn import get_conn, run_query
    conn = get_conn()
    try:
        return run_query("""
            SELECT NEIGHBORHOOD_NAME, MASTER_SCORE, MASTER_GRADE,
                   TOP_STRENGTH, TOP_WEAKNESS, CITY
            FROM NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
            WHERE MASTER_SCORE IS NOT NULL
            ORDER BY MASTER_SCORE DESC
        """, conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def load_crime_narratives():
    from shared.snowflake_conn import get_conn, run_query
    conn = get_conn()
    try:
        return run_query("""
            SELECT NEIGHBORHOOD_NAME, RECENT_TREND, RECENT_AVG_MONTHLY,
                   FORECAST_MONTH, FORECASTED_COUNT, TRAIN_MAPE,
                   N_HOTSPOT_CLUSTERS, SAFETY_NARRATIVE, RELIABILITY_FLAG
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE
            ORDER BY FORECASTED_COUNT DESC
        """, conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def load_safety_choropleth():
    from shared.snowflake_conn import get_conn, run_query
    conn = get_conn()
    try:
        return run_query("""
            SELECT ml.NAME AS NEIGHBORHOOD_NAME,
                   ml.CENTROID_LAT, ml.CENTROID_LONG,
                   ST_ASGEOJSON(TO_GEOGRAPHY(ml.GEOMETRY_WKT))::VARCHAR AS GEOJSON,
                   ns.SAFETY_SCORE, ns.SAFETY_GRADE
            FROM NEIGHBOURWISE_DOMAINS.STAGE.STG_MASTER_LOCATION ml
            INNER JOIN NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY ns
                ON UPPER(ml.NAME) = UPPER(ns.NEIGHBORHOOD_NAME)
            WHERE ml.CITY IN ('BOSTON','CAMBRIDGE')
              AND ml.GRANULARITY = 'NEIGHBORHOOD'
              AND ns.SAFETY_SCORE IS NOT NULL
              AND ns.SAFETY_GRADE != 'INSUFFICIENT DATA'
              AND ml.GEOMETRY_WKT IS NOT NULL
              AND ml.CENTROID_LAT IS NOT NULL
        """, conn)
    except Exception as e:
        print(f"[Overview] Choropleth load failed: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
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

# Altair dark theme config — shared by all charts
def dark_altair_config():
    return {
        "background": "transparent",
        "font": "DM Sans",
        "title": {"color": "#e2e8f0", "fontSize": 14, "fontWeight": "bold",
                  "anchor": "start", "subtitleColor": "rgba(255,255,255,0.4)",
                  "subtitleFontSize": 11},
        "axis": {"labelColor": "rgba(255,255,255,0.55)", "labelFontSize": 11,
                 "titleColor": "rgba(255,255,255,0.4)", "titleFontSize": 11,
                 "gridColor": "rgba(255,255,255,0.06)", "domainColor": "rgba(255,255,255,0.1)",
                 "tickColor": "rgba(255,255,255,0.1)"},
        "legend": {"labelColor": "#cbd5e1", "labelFontSize": 11,
                   "titleColor": "rgba(255,255,255,0.5)", "titleFontSize": 11},
        "view": {"stroke": "transparent"},
    }

# Grade color maps
GRADE_COLORS = {"TOP PICK":"#1E8449","SOLID CHOICE":"#82E0AA",
                "MODERATE PICK":"#F1C40F","LIMITED APPEAL":"#C0392B"}
SAFETY_COLORS = {"EXCELLENT":"#1E8449","GOOD":"#82E0AA",
                 "MODERATE":"#F1C40F","HIGH CONCERN":"#C0392B"}
TREND_COLORS = {"increasing":"#E45756","stable":"#F58518","decreasing":"#54A24B"}

INTENT_BADGES = {
    "data_query": ("🔍", "Data Query", "badge-data"),
    "chart":      ("📊", "Chart",      "badge-chart"),
    "web_search": ("🌐", "Web Search", "badge-web"),
    "report":     ("📄", "Report",     "badge-report"),
    "image":      ("🏙️", "Images",     "badge-image"),
}
EXAMPLES = [
    ("🏥", "How many hospitals in Dorchester?"),
    ("🛡️", "Top 5 safest neighborhoods in Boston"),
    ("🔄", "Compare Back Bay and Roxbury across all domains"),
    ("🎓", "Moving to Roxbury as a student — good idea?"),
    ("🚇", "Which neighborhoods have no subway access?"),
    ("🌐", "Latest MBTA service delays"),
]
DOMAIN_OPTIONS = ["ALL","HEALTHCARE","SAFETY","TRANSIT","HOUSING",
                  "RESTAURANTS","GROCERY","SCHOOLS","UNIVERSITIES","BLUEBIKES"]
REPORT_ITEMS = [
    ("📊", "Domain Scorecard",  "9 domains, 0–100"),
    ("📈", "4 Charts",          "Radar · bar · trend · comparison"),
    ("🏙️", "DALL-E Images",    "4 AI-generated visuals"),
    ("🔮", "SARIMAX Forecast",  "6-month crime prediction"),
    ("📝", "AI Narratives",     "Cortex Mistral + Claude"),
    ("📚", "RAG Context",       "Lifestyle & character"),
]
REPORT_STEPS = [
    "📡 Fetching domain data...", "🔮 Running SARIMAX forecast...",
    "📚 RAG lifestyle context...", "✍️  Cortex narratives...",
    "📊 Generating charts...", "🏙️  DALL-E images...", "📄 Assembling PDF...",
]
CHART_LABELS = {
    "chart_radar": "Domain Radar", "chart_grouped_bar": "Domain Scorecard",
    "chart_bar_neighbors": "Neighborhood Comparison",
    "chart_crime_trend": "Crime Trend & Forecast",
}


def render_assistant_message(msg: dict, key_prefix: str):
    st.markdown(msg["content"])
    if msg.get("chart_path") and Path(msg["chart_path"]).exists():
        st.image(msg["chart_path"], use_container_width=True)
    if msg.get("image_paths"):
        valid = [p for p in msg["image_paths"] if Path(p).exists()]
        for i in range(0, len(valid), 2):
            c1, c2 = st.columns(2)
            if i < len(valid):   c1.image(valid[i],   use_container_width=True)
            if i+1 < len(valid): c2.image(valid[i+1], use_container_width=True)
    if msg.get("sql"):
        with st.expander(f"📊 SQL [{key_prefix}]", expanded=False):
            st.code(msg["sql"], language="sql")
    if msg.get("results") and isinstance(msg["results"], list) and msg["results"]:
        with st.expander(f"📋 {len(msg['results'])} rows [{key_prefix}]", expanded=False):
            st.dataframe(pd.DataFrame(msg["results"][:50]), use_container_width=True)
    if msg.get("rag_chunks"):
        with st.expander(f"📄 Sources [{key_prefix}]", expanded=False):
            for i, c in enumerate(msg["rag_chunks"][:3]):
                d = c.get("DOMAIN", c.get("domain", "?"))
                t = c.get("CHUNK_TEXT", c.get("chunk_text", ""))[:250]
                st.caption(f"**[{d}]** {t}...")
                if i < len(msg["rag_chunks"]) - 1: st.divider()
    rtype = msg.get("type", "")
    if rtype in INTENT_BADGES:
        icon, label, cls = INTENT_BADGES[rtype]
        improved = msg.get("improved")
        val_str = " · 🔍 Improved" if improved is True else (" · ✅ Validated" if improved is False else "")
        elapsed = msg.get("elapsed")
        time_str = (f'<span style="color:rgba(255,255,255,0.3);font-size:11px;'
                    f'margin-left:8px;">{elapsed:.1f}s</span>') if elapsed else ""
        st.markdown(f'<span class="score-badge {cls}">{icon} {label}{val_str}</span>{time_str}',
                    unsafe_allow_html=True)


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
        unsafe_allow_html=True)
    st.divider()
    domain_filter = st.selectbox("Domain filter", DOMAIN_OPTIONS,
                                  help="Narrows RAG search to a specific domain")
    st.divider()
    st.markdown(
        '<div style="font-size:0.72rem;color:rgba(255,255,255,0.3);line-height:1.5;">'
        '51 neighborhoods · 9 domains<br>'
        'SQL (Mistral) + RAG (e5-base-v2)<br>'
        'Validator (Claude Sonnet)</div>',
        unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab_overview, tab_chat, tab_report = st.tabs([
    "🏠  Overview", "💬  Ask NeighbourWise", "📄  Neighborhood Report",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    import altair as alt

    stats   = load_overview_stats()
    hs_df   = load_crime_hotspots()
    ms_df   = load_master_scores()
    safe_df = load_safety_overview()
    nar_df  = load_crime_narratives()

    total_n = int(stats.get("TOTAL_NEIGHBORHOODS", 51))
    safest  = str(stats.get("SAFEST_NAME", "—")).title()
    safest_s = stats.get("SAFEST_SCORE", "—")
    cheapest = str(stats.get("CHEAPEST_NAME", "—")).title()
    cheapest_g = str(stats.get("CHEAPEST_GRADE", "")).replace("_", " ").title()
    afford_sub = f"Housing grade: {cheapest_g}" if cheapest_g else "Highest housing score"

    n_inc = int((nar_df["RECENT_TREND"] == "increasing").sum()) if not nar_df.empty else 0
    n_dec = int((nar_df["RECENT_TREND"] == "decreasing").sum()) if not nar_df.empty else 0
    n_hotspot = int(hs_df["N_HOTSPOT_CLUSTERS"].sum()) if not hs_df.empty else "—"

    # ── Metric cards ──────────────────────────────────────────────────────────
    render_metric_cards([
        ("Neighborhoods",     total_n,    "Boston · Cambridge · Suburbs"),
        ("Safest",            safest,     f"Safety score: {safest_s}"),
        ("Most Affordable",   cheapest,   afford_sub),
        ("Crime Hotspot Zones", n_hotspot, f"📈{n_inc} worsening · 📉{n_dec} improving"),
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 1 — Safety choropleth map + Safety top 10 bar
    # ══════════════════════════════════════════════════════════════════════════
    col_map, col_safe = st.columns([1.2, 1], gap="medium")

    with col_map:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Neighborhood Safety Score — Boston & Cambridge</div>'
            '<div class="section-subtitle">Green = safer · Red = higher concern</div>',
            unsafe_allow_html=True)

        choro_df = load_safety_choropleth()
        if not choro_df.empty and "GEOJSON" in choro_df.columns:
            import pydeck as pdk
            import json as _json

            SAFETY_FILL = {
                "EXCELLENT":    [30, 132, 73, 180],
                "GOOD":         [130, 224, 170, 180],
                "MODERATE":     [241, 196, 15, 180],
                "HIGH CONCERN": [192, 57, 43, 180],
            }

            features = []
            for _, r in choro_df.iterrows():
                try:
                    geom = _json.loads(r["GEOJSON"])
                except Exception:
                    continue
                grade = r["SAFETY_GRADE"] if pd.notna(r["SAFETY_GRADE"]) else "N/A"
                score = round(float(r["SAFETY_SCORE"]), 1) if pd.notna(r["SAFETY_SCORE"]) else "N/A"
                features.append({
                    "type": "Feature", "geometry": geom,
                    "properties": {
                        "NEIGHBORHOOD_NAME": r["NEIGHBORHOOD_NAME"],
                        "SAFETY_SCORE": score,
                        "SAFETY_GRADE": grade,
                        "fill_color": SAFETY_FILL.get(str(grade).strip().upper(),
                                                       [160, 160, 160, 140]),
                    }
                })

            geojson = {"type": "FeatureCollection", "features": features}

            layer = pdk.Layer(
                "GeoJsonLayer", data=geojson,
                filled=True, stroked=True, pickable=True, auto_highlight=True,
                get_fill_color="properties.fill_color",
                get_line_color=[255, 255, 255, 120],
                line_width_min_pixels=1,
            )
            view = pdk.ViewState(
                latitude=float(choro_df["CENTROID_LAT"].mean()),
                longitude=float(choro_df["CENTROID_LONG"].mean()),
                zoom=10.8, pitch=0,
            )
            deck = pdk.Deck(
                layers=[layer], initial_view_state=view,
                tooltip={
                    "html": "<b>{NEIGHBORHOOD_NAME}</b><br/>"
                            "Score: <b>{SAFETY_SCORE}</b>/100<br/>"
                            "Grade: <b>{SAFETY_GRADE}</b>",
                    "style": {"backgroundColor": "#1e293b", "color": "#e2e8f0",
                              "fontSize": "12px", "borderRadius": "8px", "padding": "8px"},
                },
                map_style="mapbox://styles/mapbox/dark-v10",
            )
            st.pydeck_chart(deck, use_container_width=True, height=520)
            l1, l2, l3, l4 = st.columns(4)
            l1.markdown('<span style="color:#1E8449;">■</span> **Excellent (≥75)**', unsafe_allow_html=True)
            l2.markdown('<span style="color:#82E0AA;">■</span> **Good (50–74)**', unsafe_allow_html=True)
            l3.markdown('<span style="color:#F1C40F;">■</span> **Moderate (25–49)**', unsafe_allow_html=True)
            l4.markdown('<span style="color:#C0392B;">■</span> **High Concern (<25)**', unsafe_allow_html=True)
        else:
            st.info("Safety map data not available. Ensure STG_MASTER_LOCATION has GEOMETRY_WKT.")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_safe:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Safest Neighborhoods — Top 10</div>'
            '<div class="section-subtitle">Score 0–100 · Crime density + violent crime % + trends</div>',
            unsafe_allow_html=True)
        if not safe_df.empty:
            top10 = safe_df.head(10).copy()
            bars = alt.Chart(top10).mark_bar(
                cornerRadiusTopRight=5, cornerRadiusBottomRight=5,
            ).encode(
                y=alt.Y("NEIGHBORHOOD_NAME:N", sort=None,
                        axis=alt.Axis(title=None, labelFontSize=11,
                                      labelLimit=180, labelFontWeight="bold")),
                x=alt.X("SAFETY_SCORE:Q",
                        scale=alt.Scale(domain=[0, 100]),
                        axis=alt.Axis(title="Safety Score", grid=True, tickCount=5)),
                color=alt.Color("SAFETY_GRADE:N",
                                scale=alt.Scale(domain=list(SAFETY_COLORS.keys()),
                                                range=list(SAFETY_COLORS.values())),
                                legend=alt.Legend(title="Grade", orient="bottom",
                                                  direction="horizontal")),
                tooltip=["NEIGHBORHOOD_NAME:N",
                         alt.Tooltip("SAFETY_SCORE:Q", format=".1f"),
                         "SAFETY_GRADE:N",
                         alt.Tooltip("TOTAL_INCIDENTS:Q", title="Incidents"),
                         alt.Tooltip("VIOLENT_CRIME_COUNT:Q", title="Violent")],
            )
            labels = alt.Chart(top10).mark_text(
                align="left", dx=4, fontSize=11, fontWeight="bold", color="#e2e8f0",
            ).encode(
                y=alt.Y("NEIGHBORHOOD_NAME:N", sort=None),
                x=alt.X("SAFETY_SCORE:Q"),
                text=alt.Text("SAFETY_SCORE:Q", format=".0f"),
            )
            chart = alt.layer(bars, labels).properties(height=520)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Safety data not available.")
        st.markdown('</div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 2 — Crime Forecast (top 15)
    # ══════════════════════════════════════════════════════════════════════════
    if not nar_df.empty:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Crime Forecast — Top 15 Neighborhoods</div>'
            '<div class="section-subtitle">'
            'Forecasted monthly crime count · Color = recent trend direction</div>',
            unsafe_allow_html=True)

        fc_top15 = nar_df.head(15).copy()
        fc_bars = alt.Chart(fc_top15).mark_bar(
            cornerRadiusTopRight=5, cornerRadiusBottomRight=5,
        ).encode(
            y=alt.Y("NEIGHBORHOOD_NAME:N", sort=None,
                    axis=alt.Axis(title=None, labelFontSize=11,
                                  labelLimit=170, labelFontWeight="bold")),
            x=alt.X("FORECASTED_COUNT:Q",
                    axis=alt.Axis(title="Forecasted Crimes", grid=True, tickCount=6)),
            color=alt.Color("RECENT_TREND:N",
                            scale=alt.Scale(domain=list(TREND_COLORS.keys()),
                                            range=list(TREND_COLORS.values())),
                            legend=alt.Legend(title="Trend", orient="top-right",
                                              direction="vertical")),
            tooltip=["NEIGHBORHOOD_NAME:N",
                     alt.Tooltip("FORECASTED_COUNT:Q", title="Forecast"),
                     "RECENT_TREND:N",
                     alt.Tooltip("RECENT_AVG_MONTHLY:Q", format=".0f", title="Avg/mo"),
                     alt.Tooltip("TRAIN_MAPE:Q", format=".1f", title="MAPE %"),
                     "RELIABILITY_FLAG:N"],
        )
        fc_labels = alt.Chart(fc_top15).mark_text(
            align="left", dx=4, fontSize=10, fontWeight="bold", color="#e2e8f0",
        ).encode(
            y=alt.Y("NEIGHBORHOOD_NAME:N", sort=None),
            x=alt.X("FORECASTED_COUNT:Q"),
            text=alt.Text("FORECASTED_COUNT:Q", format=".0f"),
        )
        st.altair_chart(
            alt.layer(fc_bars, fc_labels).properties(height=480),
            use_container_width=True)

        fc_month = str(nar_df["FORECAST_MONTH"].iloc[0]) if not nar_df.empty else ""
        n_stable = int((nar_df["RECENT_TREND"] == "stable").sum())
        st.markdown(
            f'<div class="narrative-box">'
            f'<div class="narrative-title">Forecast Summary — {fc_month}</div>'
            f'Of {len(nar_df)} neighborhoods with forecasts: '
            f'<b style="color:#E45756;">{n_inc} worsening</b>, '
            f'<b style="color:#F58518;">{n_stable} stable</b>, '
            f'<b style="color:#54A24B;">{n_dec} improving</b>.</div>',
            unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # ROW 3 — Safety Grade Distribution + Master Score Top 10
    # ══════════════════════════════════════════════════════════════════════════
    col_dist, col_ms = st.columns(2, gap="medium")

    with col_dist:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Safety Grade Distribution</div>'
            '<div class="section-subtitle">'
            'How 51 neighborhoods are graded on safety</div>',
            unsafe_allow_html=True)
        if not safe_df.empty:
            grade_dist = safe_df.groupby("SAFETY_GRADE").size().reset_index(name="COUNT")
            grade_order = ["EXCELLENT", "GOOD", "MODERATE", "HIGH CONCERN"]

            # Build label text for the pie: "GOOD — 27"
            grade_dist["LABEL"] = grade_dist.apply(
                lambda r: f"{r['SAFETY_GRADE']} — {r['COUNT']}", axis=1)

            donut = alt.Chart(grade_dist).mark_arc(
                innerRadius=60, outerRadius=120, stroke="#1a1a2e", strokeWidth=2,
            ).encode(
                theta=alt.Theta("COUNT:Q", stack=True),
                color=alt.Color("SAFETY_GRADE:N",
                                scale=alt.Scale(domain=grade_order,
                                                range=[SAFETY_COLORS[g] for g in grade_order]),
                                legend=alt.Legend(title=None, orient="bottom",
                                                  direction="horizontal",
                                                  labelFontSize=11, columns=2)),
                order=alt.Order("COUNT:Q", sort="descending"),
                tooltip=[alt.Tooltip("SAFETY_GRADE:N", title="Grade"),
                         alt.Tooltip("COUNT:Q", title="Neighborhoods")],
            )
            donut_labels = alt.Chart(grade_dist).mark_text(
                radius=145, fontSize=13, fontWeight="bold", color="#e2e8f0",
            ).encode(
                theta=alt.Theta("COUNT:Q", stack=True),
                order=alt.Order("COUNT:Q", sort="descending"),
                text=alt.Text("COUNT:Q"),
            )
            st.altair_chart(
                alt.layer(donut, donut_labels).properties(height=340, width=340),
                use_container_width=True)
        else:
            st.info("Safety data not available.")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_ms:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-title">Master Score — Top 10</div>'
            '<div class="section-subtitle">'
            'Weighted composite across 9 domains</div>',
            unsafe_allow_html=True)
        if not ms_df.empty:
            ms_top = ms_df.head(10).copy()

            # Color bars by score range for visual differentiation
            ms_bars = alt.Chart(ms_top).mark_bar(
                cornerRadiusTopRight=6, cornerRadiusBottomRight=6,
            ).encode(
                y=alt.Y("NEIGHBORHOOD_NAME:N", sort=None,
                        axis=alt.Axis(title=None, labelFontSize=11,
                                      labelLimit=180, labelFontWeight="bold")),
                x=alt.X("MASTER_SCORE:Q", scale=alt.Scale(domain=[0, 100]),
                        axis=alt.Axis(title="Master Score", grid=True, tickCount=5)),
                color=alt.Color("MASTER_SCORE:Q",
                                scale=alt.Scale(domain=[40, 75],
                                                range=["#F59E0B", "#1E8449"]),
                                legend=None),
                tooltip=["NEIGHBORHOOD_NAME:N",
                         alt.Tooltip("MASTER_SCORE:Q", format=".1f"),
                         "MASTER_GRADE:N", "TOP_STRENGTH:N", "TOP_WEAKNESS:N"],
            )
            ms_labels = alt.Chart(ms_top).mark_text(
                align="left", dx=4, fontSize=12, fontWeight="bold", color="#e2e8f0",
            ).encode(
                y=alt.Y("NEIGHBORHOOD_NAME:N", sort=None),
                x=alt.X("MASTER_SCORE:Q"),
                text=alt.Text("MASTER_SCORE:Q", format=".0f"),
            )
            st.altair_chart(
                alt.layer(ms_bars, ms_labels).properties(height=340),
                use_container_width=True)
        else:
            st.info("Master score data not available.")
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Full leaderboard (collapsed) ──────────────────────────────────────────
    with st.expander("📋 Full Neighborhood Leaderboard — All 51", expanded=False):
        if not ms_df.empty:
            display = ms_df[["NEIGHBORHOOD_NAME","MASTER_SCORE","MASTER_GRADE",
                             "TOP_STRENGTH","TOP_WEAKNESS","CITY"]].copy()
            display.columns = ["Neighborhood","Score","Grade","Strength","Weakness","City"]
            for c in ["Neighborhood","Strength","Weakness","City"]:
                display[c] = display[c].str.title()
            st.dataframe(display, use_container_width=True, hide_index=True, height=600)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — CHATBOT
# ══════════════════════════════════════════════════════════════════════════════
with tab_chat:
    st.markdown(
        '<p style="color:rgba(255,255,255,0.4);font-size:0.72rem;font-weight:600;'
        'letter-spacing:0.05em;text-transform:uppercase;margin-bottom:6px;">'
        'Quick Examples</p>', unsafe_allow_html=True)
    for row_ex in [EXAMPLES[:3], EXAMPLES[3:]]:
        cols = st.columns(3)
        for col, (icon, text) in zip(cols, row_ex):
            with col:
                st.markdown('<div class="ex-btn">', unsafe_allow_html=True)
                if st.button(f"{icon} {text}", key=f"ex_{text}", use_container_width=True):
                    st.session_state.prefill = text
                st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    st.divider()

    if "messages" not in st.session_state:
        st.session_state.messages = [{
            "role": "assistant",
            "content": ("👋 Hi! I'm **NeighbourWise AI** — your Boston neighborhood "
                        "intelligence assistant.\n\nAsk me anything: *Which neighborhood "
                        "is safest for families?*, *Compare Back Bay and Roxbury*, or "
                        "*Generate a report for Fenway*."),
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
            with st.spinner(""):
                t_start = time.time()
                conn = get_cached_conn()
                from router_agent import route
                result = route(user_input, conn,
                               domain_filter=(domain_filter if domain_filter != "ALL" else None))
                elapsed = time.time() - t_start

            rtype = result.get("type", "data_query")
            chart_path = image_paths = sql_query = sql_results = rag_chunks = improved = None

            if rtype == "data_query":
                answer_text = result.get("answer", "")
                sql_query = result.get("sql"); sql_results = result.get("results")
                rag_chunks = result.get("rag_chunks"); improved = result.get("improved")
            elif rtype == "chart":
                chart_path = result.get("path")
                answer_text = "Here's your chart." if chart_path else f"❌ {result.get('error','Failed')}"
            elif rtype == "image":
                image_paths = result.get("paths", [])
                nbhd = result.get("neighborhood", "")
                answer_text = (f"Here are the visuals for **{nbhd.title()}**." if image_paths
                               else f"❌ {result.get('error','Failed')}")
            elif rtype == "web_search":
                answer_text = result.get("answer", "")
                val = result.get("validation") or {}
                improved = not val.get("passed", True) if "passed" in val else None
            elif rtype == "report":
                if result.get("pdf_path"):
                    answer_text = (f"✅ Report ready for **{result.get('neighborhood','').title()}**! "
                                   f"Switch to the **Neighborhood Report** tab to download it.")
                    st.session_state["last_report"] = result
                else:
                    answer_text = f"❌ {result.get('error','Failed')}"
            else:
                answer_text = result.get("error", "Something went wrong.")

            new_msg = {"role": "assistant", "content": answer_text, "type": rtype,
                       "chart_path": chart_path, "image_paths": image_paths,
                       "sql": sql_query, "results": sql_results,
                       "rag_chunks": rag_chunks, "improved": improved, "elapsed": elapsed}
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
            unsafe_allow_html=True)

        try:
            neighborhoods = get_neighborhood_list()
        except Exception:
            neighborhoods = ["Fenway","Back Bay","Roxbury","Dorchester",
                             "South End","Beacon Hill","Jamaica Plain"]

        selected = st.selectbox("Choose a neighborhood", options=neighborhoods,
                                index=neighborhoods.index("Fenway") if "Fenway" in neighborhoods else 0)

        st.markdown("<div style='height:5px'></div>", unsafe_allow_html=True)
        for icon, title, desc in REPORT_ITEMS:
            st.markdown(
                f"<div style='display:flex;align-items:center;gap:10px;"
                f"padding:5px 0;border-bottom:1px solid rgba(255,255,255,0.06);'>"
                f"<span style='font-size:15px;width:20px;'>{icon}</span>"
                f"<div><span style='font-weight:600;font-size:11px;color:#e2e8f0;'>"
                f"{title}</span> "
                f"<span style='font-size:10px;color:rgba(255,255,255,0.35);'>— {desc}</span>"
                f"</div></div>", unsafe_allow_html=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="gen-btn">', unsafe_allow_html=True)
        generate = st.button("🚀  Generate Report", use_container_width=True, key="gen_btn")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown(
            "<p style='color:rgba(255,255,255,0.3);font-size:10px;margin-top:5px;"
            "text-align:center;'>⏱ ~3–5 minutes · includes DALL-E image generation</p></div>",
            unsafe_allow_html=True)

    with col_right:
        if generate:
            if "last_report" in st.session_state: del st.session_state["last_report"]
            st.markdown(
                f'<div class="narrative-box-blue">'
                f'<div class="narrative-title">⏳ Generating report for {selected}</div>'
                f'This takes 3–5 minutes. Do not close this tab.</div>',
                unsafe_allow_html=True)
            progress = st.progress(0); status = st.empty()
            import threading
            result_holder = {}
            def _run_report():
                from shared.snowflake_conn import get_conn
                from router_agent import route
                conn = get_conn()
                try:
                    result_holder["result"] = route(f"generate a report for {selected}", conn)
                except Exception as e:
                    result_holder["result"] = {"type": "report", "error": str(e)}
                finally: conn.close()
            t = threading.Thread(target=_run_report, daemon=True); t.start()
            step_idx = 0
            while t.is_alive():
                if step_idx < len(REPORT_STEPS):
                    pct = int((step_idx + 1) / len(REPORT_STEPS) * 90)
                    progress.progress(pct, text=REPORT_STEPS[step_idx])
                    step_idx += 1
                time.sleep(9)
            t.join(); progress.progress(100, text="✅ Done!"); status.empty()
            result = result_holder.get("result", {})
            if result.get("pdf_path") and Path(result["pdf_path"]).exists():
                st.session_state["last_report"] = result
                st.success(f"✅ Report ready for **{selected}**!")
            else:
                st.error(f"❌ Failed: {result.get('error','Unknown error')}")

        if "last_report" in st.session_state:
            report = st.session_state["last_report"]
            pdf_path = report.get("pdf_path", "")
            nbhd = report.get("neighborhood", selected)
            if pdf_path and Path(pdf_path).exists():
                st.markdown(
                    f'<div class="narrative-box">'
                    f'<div class="narrative-title">✅ Report ready — {nbhd.title()}</div>'
                    f'9 domains · 4 charts · 4 images</div>',
                    unsafe_allow_html=True)
                with open(pdf_path, "rb") as f: pdf_bytes = f.read()
                st.markdown('<div class="dl-btn">', unsafe_allow_html=True)
                st.download_button("⬇️  Download PDF Report", data=pdf_bytes,
                                   file_name=f"{nbhd.lower().replace(' ','_')}_report.pdf",
                                   mime="application/pdf", use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
        elif not generate:
            st.markdown(
                '<div class="section-card" style="text-align:center;padding:50px 28px;">'
                '<div style="font-size:40px;margin-bottom:8px;">📄</div>'
                '<div style="font-family:DM Serif Display,serif;font-size:1.1rem;'
                'color:#e2e8f0;">Your report will appear here</div>'
                '<div style="color:rgba(255,255,255,0.3);font-size:11px;margin-top:5px;">'
                'Select a neighborhood and click Generate Report</div></div>',
                unsafe_allow_html=True)