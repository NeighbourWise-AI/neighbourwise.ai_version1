"""
NeighbourWise AI — FastAPI Backend
═══════════════════════════════════════════════════════════════════════════════
REST API for NeighbourWise AI neighborhood intelligence platform.

Endpoints:
  - /overview/*          — Dashboard data (stats, safety map, leaderboards)
  - /query               — Route user queries (SQL + RAG + graph + web search)
  - /neighborhoods       — List all neighborhoods
  - /report/*            — Generate and retrieve neighborhood reports
  - /health             — Health check

Run:
    uvicorn main:app --reload
    
Then navigate to: http://localhost:8000/docs
"""

import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging

from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
import pandas as pd
from dotenv import load_dotenv
import uuid

# Load environment
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# DOMAIN & ROUTING CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

# Keyword → canonical domain tag (mirrors router_agent._DOMAIN_KEYWORDS)
_DOMAIN_KEYWORDS: Dict[str, List[str]] = {
    "SAFETY":     ["crime", "safe", "safety", "incident", "violent", "theft", "assault", "police", "shooting", "robbery"],
    "HOUSING":    ["housing", "rent", "apartment", "price", "afford", "sqft", "bedroom", "buy", "home", "mortgage", "condo"],
    "RESTAURANT": ["restaurant", "food", "eat", "dining", "cafe", "bar", "coffee", "cuisine", "brunch", "lunch", "dinner"],
    "HEALTHCARE": ["hospital", "clinic", "doctor", "health", "medical", "pharmacy", "urgent care", "dentist", "healthcare"],
    "SCHOOLS":    ["school", "education", "college", "university", "k-12", "rating", "academic", "district", "student"],
    "GROCERY":    ["grocery", "supermarket", "market", "whole foods", "trader joe", "star market", "store", "provisions"],
    "TRANSIT":    ["mbta", "transit", "bus", "subway", "train", "commute", "t-stop", "green line", "red line", "orange line", "blue line"],
    "BLUEBIKES":  ["bluebike", "bike", "cycling", "bicycle", "bikeshare", "docking station"],
    "WEATHER":    ["weather", "temperature", "rain", "snow", "climate", "cold", "warm", "season", "humidity"],
}

# Neighborhoods known to the platform (used for fast name extraction)
_KNOWN_NEIGHBORHOODS = [
    "allston", "back bay", "beacon hill", "brighton", "charlestown", "chinatown",
    "dorchester", "east boston", "fenway", "hyde park", "jamaica plain", "kenmore",
    "mattapan", "mission hill", "north end", "roslindale", "roxbury", "south boston",
    "south end", "west end", "west roxbury", "downtown", "cambridge", "somerville",
    "brookline", "quincy", "newton", "waltham", "medford", "malden", "everett",
    "revere", "chelsea", "watertown",
]

# Intent routing map
_INTENT_LABELS = {
    "data_query":   "Single-domain SQL + RAG lookup",
    "graph_query":  "Multi-domain / ranking / comparison query (Neo4j + Snowflake)",
    "web_search":   "Outside platform coverage — live web search",
    "report":       "Full neighborhood PDF report",
    "chart":        "Data visualization request",
}

# ── Livability / comparison intent signals ────────────────────────────────────
# Queries with these words + Boston geography → graph_query against internal data
# (master scores, domain rankings) rather than web_search.
_LIVABILITY_INTENT: List[str] = [
    "best", "worst", "top", "safest", "most affordable", "cheapest",
    "most walkable", "rank", "ranking", "compare", "comparison",
    "versus", " vs ", "which is better", "which is safer", "which is cheaper",
    "where should i live", "where to live", "recommend", "suggest", "ideal",
    "good for families", "good for students", "good for young",
    "livability", "livable", "move to", "moving to", "relocat",
    "which neighborhood", "what neighborhood", "best neighborhood",
    "worst neighborhood", "top neighborhood", "tell me about",
]

# Boston geography signals — confirm a query is about the platform's coverage area
_BOSTON_GEOGRAPHY: List[str] = [
    "boston", "cambridge", "somerville", "quincy", "brookline", "greater boston",
    "allston", "back bay", "beacon hill", "brighton", "charlestown", "chinatown",
    "dorchester", "east boston", "fenway", "hyde park", "jamaica plain", "mattapan",
    "mission hill", "north end", "roslindale", "roxbury", "south boston", "south end",
    "west roxbury", "west end", "neighbourwise",
    "neighborhood in boston", "neighbourhoods in boston",
    "area in boston", "district in boston", "live in boston",
]

# Non-Boston geography — if present, domain keywords should NOT imply internal data
_NON_BOSTON_CITIES: List[str] = [
    "new york", "nyc", "chicago", "los angeles", " la ", "seattle", "austin",
    "denver", "miami", "atlanta", "washington dc", "philadelphia", "san francisco",
    "portland", "houston", "dallas", "detroit", "minneapolis", "phoenix",
    "san diego", "las vegas", "new orleans", "nashville", "charlotte",
]

# Signals that the query wants current/external info regardless of domain words
_WEB_ONLY_SIGNALS: List[str] = [
    "latest news", "breaking news", "news today", "this week's", "current events",
    "tonight", "this weekend", "schedule", "ticket", "event ",
    "apply for", "how do i apply", "library card", "permit", "license",
    "weather today", "weather forecast", "temperature today",
    "red sox", "celtics", "bruins", "patriots", "game tonight",
]


# ══════════════════════════════════════════════════════════════════════════════
# INITIALIZE FASTAPI
# ══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="NeighbourWise AI API",
    description="Boston neighborhood intelligence via SQL + RAG + Graph + Web Search",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ══════════════════════════════════════════════════════════════════════════════

class QueryRequest(BaseModel):
    """User query request."""
    query: str = Field(..., description="Natural language query about Boston neighborhoods")
    domain_filter: Optional[str] = Field(
        None,
        description="Force a specific domain tag (SAFETY, HOUSING, RESTAURANT, HEALTHCARE, "
                    "SCHOOLS, GROCERY, TRANSIT, BLUEBIKES, WEATHER). Overrides auto-detection."
    )

    class Config:
        json_schema_extra = {
            "example": {
                "query": "Is Allston safe and affordable?",
                "domain_filter": None,
            }
        }


class RoutingMeta(BaseModel):
    """Routing decision metadata returned alongside every query."""
    detected_domains: List[str] = Field(..., description="All domains detected in the query")
    detected_neighborhoods: List[str] = Field(..., description="Neighborhood names found in the query")
    intent: str = Field(..., description="Routing intent: data_query | graph_query | web_search | report | chart")
    intent_description: str = Field(..., description="Human-readable reason for this routing decision")
    domain_override: bool = Field(False, description="True when domain_filter was supplied by caller")
    fallback_used: Optional[str] = Field(None, description="Set when primary agent failed and a fallback was triggered, e.g. \'graph→data_query\'"  )


class QueryResponse(BaseModel):
    """Query response from router agent."""
    type: str = Field(..., description="Intent type: data_query, chart, image, web_search, report, graph_query")
    answer: str = Field(..., description="Synthesized answer")
    neighborhood: Optional[str] = Field(None, description="Primary detected neighborhood (if any)")
    domain: Optional[str] = Field(None, description="Primary detected domain")
    domains: List[str] = Field(default_factory=list, description="All detected domains")
    confidence: float = Field(0.0, description="Classification confidence (0–1)")
    elapsed: float = Field(0.0, description="Query execution time in seconds")
    routing: Optional[RoutingMeta] = Field(None, description="Routing decision metadata")
    sql: Optional[str] = Field(None, description="Executed SQL (if data_query)")
    results: Optional[List[Dict]] = Field(None, description="SQL results (if data_query)")
    rag_chunks: Optional[List[Dict]] = Field(None, description="RAG sources (if applicable)")
    validation: Optional[Dict] = Field(None, description="Validation feedback")
    chart_path: Optional[str] = Field(None, description="Path to generated chart (if chart)")
    image_paths: Optional[List[str]] = Field(None, description="Paths to generated images (if image)")
    error: Optional[str] = Field(None, description="Error message (if failed)")


class OverviewStats(BaseModel):
    total_neighborhoods: int
    avg_master_score: float
    top_score: float
    safest_neighborhood: Optional[str]
    safest_score: Optional[float]
    most_affordable: Optional[str]
    affordable_rent: Optional[float]


class SafetyRecord(BaseModel):
    neighborhood_name: str
    safety_score: float
    safety_grade: str
    total_incidents: int
    violent_crime_count: int


class NeighborhoodSummary(BaseModel):
    name: str
    city: str
    master_score: float
    master_grade: str
    top_strength: str
    top_weakness: str


class ReportRequest(BaseModel):
    neighborhood: str = Field(..., description="Neighborhood name")

    class Config:
        json_schema_extra = {"example": {"neighborhood": "Dorchester"}}


class ReportResponse(BaseModel):
    report_id: str
    neighborhood: str
    status: str = Field("pending")
    pdf_path: Optional[str] = None
    url: Optional[str] = None
    created_at: str
    completed_at: Optional[str] = None
    message: str


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    snowflake_connected: bool


# ══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY TASK STORE
# ══════════════════════════════════════════════════════════════════════════════

reports_db: Dict[str, Dict[str, Any]] = {}
REPORTS_DIR = Path(__file__).resolve().parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_snowflake_conn():
    try:
        from shared.snowflake_conn import get_conn
        return get_conn()
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")


def run_query(query: str, conn):
    try:
        from shared.snowflake_conn import run_query as sf_run_query
        return sf_run_query(query, conn)
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


# ══════════════════════════════════════════════════════════════════════════════
# SMART ROUTING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def detect_domains(text: str, domain_filter: Optional[str] = None) -> List[str]:
    """
    Return canonical domain tags present in `text`.
    If domain_filter is supplied it is returned as the sole domain (caller override).
    """
    if domain_filter:
        tag = domain_filter.upper()
        if tag in _DOMAIN_KEYWORDS:
            return [tag]
        logger.warning(f"Unknown domain_filter '{domain_filter}' — ignoring override.")

    lower = text.lower()
    found = []
    for domain, keywords in _DOMAIN_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            found.append(domain)
    return found


def detect_neighborhoods(text: str) -> List[str]:
    """Return normalized neighborhood names found in `text`."""
    lower = text.lower()
    return [n.title() for n in _KNOWN_NEIGHBORHOODS if n in lower]


def _is_chart_request(text: str) -> bool:
    chart_words = ["chart", "graph", "plot", "visualiz", "histogram", "bar chart", "pie chart", "map"]
    lower = text.lower()
    return any(w in lower for w in chart_words)


def _is_report_request(text: str) -> bool:
    report_words = ["full report", "generate report", "pdf report", "neighborhood report", "comprehensive report"]
    lower = text.lower()
    return any(w in lower for w in report_words)


def _is_boston_scoped(query: str) -> bool:
    """True when the query is clearly about the Greater Boston / NeighbourWise coverage area."""
    q = query.lower()
    return any(k in q for k in _BOSTON_GEOGRAPHY)


def _is_non_boston(query: str) -> bool:
    """True when the query explicitly names a city outside our coverage area."""
    q = query.lower()
    return any(c in q for c in _NON_BOSTON_CITIES)


def _has_livability_intent(query: str) -> bool:
    """True when the query is asking to compare, rank, or choose between neighborhoods."""
    q = query.lower()
    return any(k in q for k in _LIVABILITY_INTENT)


def _has_web_only_signal(query: str, detected_domains: List[str] = None) -> bool:
    """
    True when the query genuinely needs live external data that internal DB cannot serve.

    Rules (in order):
      1. Hard phrases always → True  (news, apply-for, sports events, weather today)
      2. WEATHER domain + time-sensitive word → True  (live forecast, not historical climate)
      3. Event/schedule signal → True
    """
    q = query.lower()
    domains = detected_domains or []

    if any(k in q for k in _WEB_ONLY_HARD):
        return True
    if "WEATHER" in domains and any(t in q for t in _WEATHER_TIME_WORDS):
        return True
    if any(k in q for k in _EVENT_SIGNALS):
        return True
    return False


def determine_intent(
    query: str,
    detected_domains: List[str],
    domain_filter: Optional[str],
) -> str:
    """
    Routing rules (evaluated in priority order):

    1. report keywords                               → report
    2. chart/visualization keywords                  → chart
    3. Web-only signals (news, weather today, etc.)  → web_search
       (overrides domain matches — these need live data)
    4. Non-Boston city explicitly named + no
       Boston geography present                      → web_search
    5. Livability/comparison intent + Boston scope   → graph_query
       (e.g. "best neighborhood", "compare Allston
        and Brighton", "where should I live")
    6. 2+ domains detected                           → graph_query
    7. 1 domain detected                             → data_query
    8. Boston geography present (no domain/intent)   → graph_query
       (e.g. "tell me about Dorchester" — use all-
        domain profile from master scores)
    9. Nothing matches                               → web_search

    Philosophy: web_search is the last resort, not the default for
    zero-domain queries. The platform has rich data across 51 neighborhoods
    and 9 domains — anything plausibly about Greater Boston livability
    should be answered from internal data first.
    """
    if _is_report_request(query):
        return "report"
    if _is_chart_request(query):
        return "chart"

    # Web-only signals always win — live data needed regardless of domain words
    if _has_web_only_signal(query, detected_domains):
        return "web_search"

    # Query is about a non-Boston city with no local geography mentioned
    if _is_non_boston(query) and not _is_boston_scoped(query):
        return "web_search"

    # Livability/comparison intent → always use internal data
    # (platform is Boston-specific; "best neighborhood" = Boston by default)
    if _has_livability_intent(query):
        return "graph_query"

    # Multi-domain explicit signals
    if len(detected_domains) >= 2:
        return "graph_query"

    # Single domain
    if len(detected_domains) == 1:
        return "data_query"

    # Boston geography present but no specific domain/intent
    # → open-ended profile query, use graph with all domains
    if _is_boston_scoped(query):
        return "graph_query"

    # Genuinely nothing to go on → web search
    return "web_search"


def build_routing_meta(
    query: str,
    detected_domains: List[str],
    detected_neighborhoods: List[str],
    intent: str,
    domain_filter: Optional[str],
) -> RoutingMeta:
    return RoutingMeta(
        detected_domains=detected_domains,
        detected_neighborhoods=detected_neighborhoods,
        intent=intent,
        intent_description=_INTENT_LABELS.get(intent, intent),
        domain_override=bool(domain_filter and domain_filter.upper() in _DOMAIN_KEYWORDS),
    )


# ══════════════════════════════════════════════════════════════════════════════
# AGENT DISPATCH
# ══════════════════════════════════════════════════════════════════════════════

def _dispatch_data_query(query: str, domains: List[str], conn) -> Dict[str, Any]:
    """Single-domain path: invoke router_agent.route() with detected domain hint."""
    from router_agent import route
    domain_hint = domains[0] if domains else None
    return route(query, conn, domain_filter=domain_hint)


def _is_graph_answer_empty(result: Dict[str, Any]) -> bool:
    """
    Return True if the graph agent returned but produced no useful data —
    e.g. Neo4j DNS failure, empty nodes, or the agent explicitly signals
    it could not retrieve data.
    """
    if result.get("error"):
        return True
    answer = result.get("answer", "")
    # Phrases the graph agent emits when Neo4j is unreachable
    _failure_signals = [
        "getaddrinfo failed",
        "dns resolution error",
        "connectivity error",
        "graph data: ❌",
        "graph unavailable",
        "unable to retrieve",
        "connection failed",
        "database returned a dns",
    ]
    answer_lower = answer.lower()
    return any(sig in answer_lower for sig in _failure_signals)


def _dispatch_graph_query(query: str, domains: List[str], conn) -> Dict[str, Any]:
    """
    Multi-domain path: try Graph_agent (Neo4j fan-out) first.

    Fallback chain on failure:
      1. ImportError or any runtime exception          → per-domain data_query calls
      2. Graph agent returns but answer signals Neo4j  → per-domain data_query calls
         DNS / connection failure
      3. Only one domain survives fallback             → single data_query
    """
    result: Dict[str, Any] = {}

    # ── Try Graph_agent ──────────────────────────────────────────────────────
    try:
        from Graph_agent import run_graph_agent
        result = run_graph_agent(query, domains=domains, conn=conn)
    except ImportError:
        logger.warning("[graph] Graph_agent not importable — falling back to data_query")
        result = {}
    except Exception as exc:
        logger.warning(f"[graph] Graph_agent raised {type(exc).__name__}: {exc} — falling back to data_query")
        result = {}

    # ── Check for silent Neo4j failure in the returned answer ───────────────
    if result and not _is_graph_answer_empty(result):
        return result  # ✅ graph succeeded

    logger.warning(
        "[graph] Graph agent returned empty/failure answer — "
        "falling back to parallel data_query across domains: %s", domains
    )

    # ── Fallback: run one data_query per domain and merge answers ────────────
    from router_agent import route

    sub_results: List[Dict[str, Any]] = []
    for domain in domains:
        try:
            sub = route(query, conn, domain_filter=domain)
            if sub.get("answer"):
                sub_results.append(sub)
        except Exception as exc:
            logger.error(f"[graph-fallback] data_query for domain={domain} failed: {exc}")

    if not sub_results:
        # Nothing worked at all — return whatever the graph agent gave us
        return result or {"type": "error", "answer": "", "error": "All agents failed"}

    if len(sub_results) == 1:
        merged = sub_results[0]
        merged["type"] = "data_query"
        merged["routing_fallback"] = "graph→data_query (single domain)"
        return merged

    # Merge multiple domain answers into one coherent response
    combined_answer = "\n\n---\n\n".join(
        f"**{domains[i] if i < len(domains) else 'Domain'} Analysis**\n\n{r['answer']}"
        for i, r in enumerate(sub_results)
    )
    merged_results = []
    for r in sub_results:
        if r.get("results"):
            merged_results.extend(r["results"])
    merged_rag = []
    for r in sub_results:
        if r.get("rag_chunks"):
            merged_rag.extend(r["rag_chunks"])

    return {
        "type": "data_query",
        "answer": combined_answer,
        "neighborhood": sub_results[0].get("neighborhood"),
        "domain": ", ".join(domains),
        "results": merged_results or None,
        "rag_chunks": merged_rag or None,
        "sql": None,
        "validation": sub_results[0].get("validation"),
        "confidence": min(r.get("confidence", 0.0) for r in sub_results),
        "routing_fallback": "graph→data_query (multi-domain merge)",
    }


def _dispatch_web_search(query: str) -> Dict[str, Any]:
    """Zero-domain path: invoke web_search_agent."""
    try:
        from web_search_agent import search
        return search(query)
    except ImportError:
        from router_agent import route
        conn = get_snowflake_conn()
        result = route(query, conn)
        conn.close()
        return result


def _dispatch_chart(query: str, domains: List[str], conn) -> Dict[str, Any]:
    """Chart intent: delegate to router_agent (it handles chart intent internally)."""
    from router_agent import route
    domain_hint = domains[0] if domains else None
    return route(query, conn, domain_filter=domain_hint)


def _dispatch_report(query: str, neighborhoods: List[str], conn) -> Dict[str, Any]:
    """Report intent: delegate to report_agent for a single neighborhood."""
    from report_agent import ask_report_agent
    neighborhood = neighborhoods[0] if neighborhoods else "Boston"
    return ask_report_agent(neighborhood, conn)


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Check API and database health."""
    try:
        conn = get_snowflake_conn()
        conn.close()
        snowflake_ok = True
    except Exception:
        snowflake_ok = False

    return HealthResponse(
        status="healthy" if snowflake_ok else "degraded",
        timestamp=datetime.utcnow().isoformat(),
        snowflake_connected=snowflake_ok,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ROOT
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/", tags=["Info"])
async def root():
    return {
        "name": "NeighbourWise AI API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "endpoints": {
            "overview": {
                "stats": "GET /overview/stats",
                "safety": "GET /overview/safety",
                "hotspots": "GET /overview/hotspots",
                "master_scores": "GET /overview/master-scores",
                "crime_narratives": "GET /overview/crime-narratives",
                "choropleth": "GET /overview/choropleth",
            },
            "query": "POST /query",
            "neighborhoods": "GET /neighborhoods",
            "report": {
                "generate": "POST /report/generate",
                "status": "GET /report/{report_id}",
                "download": "GET /report/{report_id}/download",
                "list": "GET /report",
            },
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# OVERVIEW ENDPOINTS  (unchanged from original)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/overview/stats", response_model=OverviewStats, tags=["Overview"])
async def get_overview_stats():
    """Get overview statistics (total neighborhoods, scores, safest, cheapest)."""
    conn = get_snowflake_conn()
    try:
        df = run_query("""
            SELECT COUNT(*) AS TOTAL_NEIGHBORHOODS,
                   ROUND(AVG(MASTER_SCORE),1) AS AVG_MASTER_SCORE,
                   MAX(MASTER_SCORE) AS TOP_SCORE
            FROM NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
            WHERE MASTER_SCORE IS NOT NULL
        """, conn)
        result = df.iloc[0].to_dict() if not df.empty else {}

        df_safe = run_query("""
            SELECT NEIGHBORHOOD_NAME, SAFETY_SCORE
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
            WHERE SAFETY_SCORE IS NOT NULL
            ORDER BY SAFETY_SCORE DESC LIMIT 1
        """, conn)
        safest_name  = df_safe.iloc[0]["NEIGHBORHOOD_NAME"].title() if not df_safe.empty else None
        safest_score = float(df_safe.iloc[0]["SAFETY_SCORE"])       if not df_safe.empty else None

        df_afford = run_query("""
            SELECT NEIGHBORHOOD_NAME, HOUSING_SCORE, AVG_ESTIMATED_RENT
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING
            WHERE HOUSING_SCORE IS NOT NULL
            ORDER BY HOUSING_SCORE DESC LIMIT 1
        """, conn)
        cheapest_name = df_afford.iloc[0]["NEIGHBORHOOD_NAME"].title() if not df_afford.empty else None
        cheapest_rent = (
            float(df_afford.iloc[0]["AVG_ESTIMATED_RENT"])
            if not df_afford.empty and pd.notna(df_afford.iloc[0]["AVG_ESTIMATED_RENT"])
            else None
        )

        return OverviewStats(
            total_neighborhoods=int(result.get("TOTAL_NEIGHBORHOODS", 51)),
            avg_master_score=float(result.get("AVG_MASTER_SCORE", 0)),
            top_score=float(result.get("TOP_SCORE", 0)),
            safest_neighborhood=safest_name,
            safest_score=safest_score,
            most_affordable=cheapest_name,
            affordable_rent=cheapest_rent,
        )
    finally:
        conn.close()


@app.get("/overview/safety", tags=["Overview"])
async def get_safety_overview():
    conn = get_snowflake_conn()
    try:
        df = run_query("""
            SELECT NEIGHBORHOOD_NAME, SAFETY_SCORE, SAFETY_GRADE,
                   TOTAL_INCIDENTS, VIOLENT_CRIME_COUNT
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
            WHERE SAFETY_SCORE IS NOT NULL
            ORDER BY SAFETY_SCORE DESC
        """, conn)
        return {
            "count": len(df),
            "data": [
                {
                    "neighborhood": row["NEIGHBORHOOD_NAME"].title(),
                    "score": float(row["SAFETY_SCORE"]),
                    "grade": row["SAFETY_GRADE"],
                    "incidents": int(row["TOTAL_INCIDENTS"]),
                    "violent_crimes": int(row["VIOLENT_CRIME_COUNT"]),
                }
                for _, row in df.iterrows()
            ],
        }
    finally:
        conn.close()


@app.get("/overview/hotspots", tags=["Overview"])
async def get_crime_hotspots():
    conn = get_snowflake_conn()
    try:
        df = run_query("""
            SELECT NEIGHBORHOOD_NAME, N_HOTSPOT_CLUSTERS,
                   HOTSPOT_CRIME_SHARE_PCT, TOTAL_CRIMES
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_HOTSPOT_CLUSTERS
            ORDER BY HOTSPOT_CRIME_SHARE_PCT DESC
        """, conn)
        return {
            "count": len(df),
            "data": [
                {
                    "neighborhood": row["NEIGHBORHOOD_NAME"].title(),
                    "clusters": int(row["N_HOTSPOT_CLUSTERS"]),
                    "crime_share_pct": float(row["HOTSPOT_CRIME_SHARE_PCT"]),
                    "total_crimes": int(row["TOTAL_CRIMES"]),
                }
                for _, row in df.iterrows()
            ],
        }
    finally:
        conn.close()


@app.get("/overview/master-scores", tags=["Overview"])
async def get_master_scores(limit: int = Query(51, ge=1, le=100)):
    conn = get_snowflake_conn()
    try:
        df = run_query(f"""
            SELECT NEIGHBORHOOD_NAME, MASTER_SCORE, MASTER_GRADE,
                   TOP_STRENGTH, TOP_WEAKNESS, CITY
            FROM NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
            WHERE MASTER_SCORE IS NOT NULL
            ORDER BY MASTER_SCORE DESC
            LIMIT {limit}
        """, conn)
        return {
            "count": len(df),
            "data": [
                {
                    "neighborhood": row["NEIGHBORHOOD_NAME"].title(),
                    "score": float(row["MASTER_SCORE"]),
                    "grade": row["MASTER_GRADE"],
                    "strength": row["TOP_STRENGTH"],
                    "weakness": row["TOP_WEAKNESS"],
                    "city": row["CITY"].title(),
                }
                for _, row in df.iterrows()
            ],
        }
    finally:
        conn.close()


@app.get("/overview/crime-narratives", tags=["Overview"])
async def get_crime_narratives():
    conn = get_snowflake_conn()
    try:
        df = run_query("""
            SELECT NEIGHBORHOOD_NAME, RECENT_TREND, RECENT_AVG_MONTHLY,
                   FORECAST_MONTH, FORECASTED_COUNT, TRAIN_MAPE,
                   N_HOTSPOT_CLUSTERS, SAFETY_NARRATIVE, RELIABILITY_FLAG
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE
            ORDER BY FORECASTED_COUNT DESC
        """, conn)
        trend_counts = {
            "increasing": int((df["RECENT_TREND"] == "increasing").sum()) if len(df) > 0 else 0,
            "decreasing": int((df["RECENT_TREND"] == "decreasing").sum()) if len(df) > 0 else 0,
            "stable":     int((df["RECENT_TREND"] == "stable").sum())     if len(df) > 0 else 0,
        }
        return {
            "count": len(df),
            "trend_summary": trend_counts,
            "data": [
                {
                    "neighborhood": row["NEIGHBORHOOD_NAME"].title(),
                    "recent_trend": row["RECENT_TREND"],
                    "recent_avg_monthly": float(row["RECENT_AVG_MONTHLY"]),
                    "forecast_month": str(row["FORECAST_MONTH"]),
                    "forecasted_count": float(row["FORECASTED_COUNT"]),
                    "mape": float(row["TRAIN_MAPE"]) if pd.notna(row["TRAIN_MAPE"]) else None,
                    "hotspot_clusters": int(row["N_HOTSPOT_CLUSTERS"]),
                }
                for _, row in df.iterrows()
            ],
        }
    finally:
        conn.close()


@app.get("/overview/choropleth", tags=["Overview"])
async def get_safety_choropleth():
    conn = get_snowflake_conn()
    try:
        df = run_query("""
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
        features = []
        for _, row in df.iterrows():
            try:
                geom = json.loads(row["GEOJSON"])
                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "neighborhood": row["NEIGHBORHOOD_NAME"].title(),
                        "safety_score": float(row["SAFETY_SCORE"]),
                        "safety_grade": row["SAFETY_GRADE"],
                        "latitude": float(row["CENTROID_LAT"]),
                        "longitude": float(row["CENTROID_LONG"]),
                    },
                })
            except Exception as e:
                logger.warning(f"Failed to parse geometry for {row['NEIGHBORHOOD_NAME']}: {e}")
                continue
        return {"type": "FeatureCollection", "features": features}
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# NEIGHBORHOODS ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/neighborhoods", tags=["Neighborhoods"])
async def list_neighborhoods():
    conn = get_snowflake_conn()
    try:
        from shared.snowflake_conn import get_all_neighborhoods
        neighborhoods = sorted(get_all_neighborhoods(conn))
        return {"count": len(neighborhoods), "neighborhoods": neighborhoods}
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# QUERY ENDPOINT  ← smart routing rebuilt here
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/query", response_model=QueryResponse, tags=["Query"])
async def process_query(request: QueryRequest):
    """
    Route and process a natural language query about Boston neighborhoods.

    **Routing logic (applied in this order):**

    | Condition                              | Intent       | Agent invoked           |
    |----------------------------------------|--------------|-------------------------|
    | Query contains report keywords         | report       | report_agent            |
    | Query contains chart/graph/plot keywords | chart      | router_agent (chart)    |
    | 2+ domains detected in query           | graph_query  | Graph_agent (Neo4j fan-out) |
    | Exactly 1 domain detected              | data_query   | router_agent (SQL + RAG)|
    | No domain detected                     | web_search   | web_search_agent        |

    Pass `domain_filter` to force a specific domain and skip auto-detection.
    The `routing` field in the response describes the decision taken.
    """
    logger.info(f"[/query] Received: {request.query!r}  domain_filter={request.domain_filter!r}")
    t_start = time.time()

    # ── 1. Pre-classify (no DB needed) ────────────────────────────────────────
    detected_domains       = detect_domains(request.query, request.domain_filter)
    detected_neighborhoods = detect_neighborhoods(request.query)
    intent                 = determine_intent(request.query, detected_domains, request.domain_filter)

    routing_meta = build_routing_meta(
        query=request.query,
        detected_domains=detected_domains,
        detected_neighborhoods=detected_neighborhoods,
        intent=intent,
        domain_filter=request.domain_filter,
    )

    logger.info(
        f"[/query] Routing decision → intent={intent!r}  "
        f"domains={detected_domains}  neighborhoods={detected_neighborhoods}"
    )

    # ── 2. Dispatch ───────────────────────────────────────────────────────────
    conn = None
    try:
        result: Dict[str, Any] = {}

        if intent == "web_search":
            # Web search agent does NOT need a Snowflake connection
            result = _dispatch_web_search(request.query)

        else:
            conn = get_snowflake_conn()

            if intent == "report":
                result = _dispatch_report(request.query, detected_neighborhoods, conn)

            elif intent == "chart":
                result = _dispatch_chart(request.query, detected_domains, conn)

            elif intent == "graph_query":
                result = _dispatch_graph_query(request.query, detected_domains, conn)

            else:  # data_query (default)
                result = _dispatch_data_query(request.query, detected_domains, conn)

    except Exception as e:
        logger.error(f"[/query] Dispatch failed for intent={intent!r}: {e}", exc_info=True)
        return QueryResponse(
            type="error",
            answer="",
            domains=detected_domains,
            routing=routing_meta,
            error=str(e),
            elapsed=round(time.time() - t_start, 2),
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    elapsed = round(time.time() - t_start, 2)

    # ── 3. Propagate any fallback info from agent result into routing_meta ─────
    fallback = result.get("routing_fallback")
    if fallback:
        routing_meta.fallback_used = fallback
        logger.info(f"[/query] Fallback used: {fallback}")

    effective_type = result.get("type", intent)
    logger.info(f"[/query] Completed in {elapsed}s  effective_type={effective_type!r}  fallback={fallback!r}")

    # ── 4. Normalise agent result into QueryResponse ───────────────────────────
    return QueryResponse(
        type=effective_type,
        answer=result.get("answer", ""),
        neighborhood=result.get("neighborhood") or (detected_neighborhoods[0] if detected_neighborhoods else None),
        domain=result.get("domain") or (detected_domains[0] if detected_domains else None),
        domains=detected_domains,
        confidence=float(result.get("confidence", 0.0)),
        elapsed=elapsed,
        routing=routing_meta,
        sql=result.get("sql"),
        results=result.get("results"),
        rag_chunks=result.get("rag_chunks"),
        validation=result.get("validation"),
        chart_path=result.get("path"),
        image_paths=result.get("paths"),
        error=result.get("error"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# REPORT ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

def _generate_report_background(report_id: str, neighborhood: str, conn):
    try:
        reports_db[report_id]["status"] = "processing"
        from report_agent import ask_report_agent
        logger.info(f"[Report {report_id}] Generating report for {neighborhood}")
        result = ask_report_agent(neighborhood, conn)
        if result and result.get("pdf_path"):
            pdf_path = result["pdf_path"]
            reports_db[report_id].update({
                "status": "completed",
                "pdf_path": pdf_path,
                "url": f"/report/{report_id}/download",
                "completed_at": datetime.utcnow().isoformat(),
                "message": f"Report ready for {neighborhood}",
            })
            logger.info(f"[Report {report_id}] Completed: {pdf_path}")
        else:
            reports_db[report_id].update({
                "status": "failed",
                "message": result.get("error", "Report generation failed"),
            })
    except Exception as e:
        logger.error(f"[Report {report_id}] Generation failed: {e}", exc_info=True)
        reports_db[report_id].update({"status": "failed", "message": str(e)})


@app.post("/report/generate", response_model=ReportResponse, tags=["Report"])
async def generate_report(request: ReportRequest, background_tasks: BackgroundTasks):
    """Generate a comprehensive neighborhood report (async)."""
    report_id = str(uuid.uuid4())[:8]
    reports_db[report_id] = {
        "report_id": report_id,
        "neighborhood": request.neighborhood.title(),
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "pdf_path": None,
        "url": None,
        "message": f"Report generation started for {request.neighborhood}",
    }
    conn = get_snowflake_conn()
    background_tasks.add_task(_generate_report_background, report_id, request.neighborhood, conn)
    return ReportResponse(**reports_db[report_id])


@app.get("/report/{report_id}", response_model=ReportResponse, tags=["Report"])
async def get_report_status(report_id: str):
    if report_id not in reports_db:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")
    return ReportResponse(**reports_db[report_id])


@app.get("/report", tags=["Report"])
async def list_reports():
    return {
        "count": len(reports_db),
        "reports": [
            {
                "report_id": rid,
                "neighborhood": data["neighborhood"],
                "status": data["status"],
                "created_at": data["created_at"],
                "completed_at": data.get("completed_at"),
            }
            for rid, data in reports_db.items()
        ],
    }


@app.get("/report/{report_id}/download", tags=["Report"])
async def download_report(report_id: str):
    if report_id not in reports_db:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")
    report_data = reports_db[report_id]
    if report_data["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Report still {report_data['status']}. Check /report/{report_id} for status.",
        )
    pdf_path = report_data.get("pdf_path")
    if not pdf_path or not Path(pdf_path).exists():
        raise HTTPException(status_code=404, detail="PDF file not found")
    return FileResponse(
        path=pdf_path,
        filename=f"{report_data['neighborhood']}_report.pdf",
        media_type="application/pdf",
    )


# ══════════════════════════════════════════════════════════════════════════════
# 404 HANDLER
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/404", tags=["Info"])
async def not_found():
    raise HTTPException(status_code=404, detail="Endpoint not found. Check /docs for valid endpoints.")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )