"""
NeighbourWise AI — FastAPI Backend
═══════════════════════════════════════════════════════════════════════════════
REST API for NeighbourWise AI neighborhood intelligence platform.

Endpoints:
  - /overview/*          — Dashboard data (via overview_endpoints.py router)
  - /query               — Route user queries (SQL + RAG + Graph + Web Search)
  - /neighborhoods       — List all neighborhoods (legacy)
  - /report/*            — Generate and retrieve neighborhood reports
  - /health              — Health check

Run:
    python3 -m uvicorn neighbourwise_fastapi:app --reload --port 8001
"""

import json
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse
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
# INITIALIZE FASTAPI
# ══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="NeighbourWise AI API",
    description="Boston neighborhood intelligence via SQL + RAG + Graph + Web Search",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Mount overview router — all /overview/* endpoints live in overview_endpoints.py
from overview_endpoints import router as overview_router
app.include_router(overview_router)

# CORS middleware
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
        description="Force a specific domain tag (SAFETY, HOUSING, RESTAURANTS, HEALTHCARE, "
                    "SCHOOLS, GROCERY, TRANSIT, BLUEBIKES). Overrides auto-detection."
    )

    class Config:
        json_schema_extra = {
            "example": {
                "query": "Is Allston safe and affordable?",
                "domain_filter": None,
            }
        }


class RoutingMeta(BaseModel):
    """Routing decision metadata returned alongside every query response."""
    detected_domains: List[str] = Field(..., description="All domains detected in the query")
    detected_neighborhoods: List[str] = Field(..., description="Neighborhood names found in the query")
    intent: str = Field(..., description="Routing intent: data_query | graph_query | web_search | report | chart | image")
    intent_description: str = Field(..., description="Human-readable reason for this routing decision")
    domain_override: bool = Field(False, description="True when domain_filter was supplied by caller")
    fallback_used: Optional[str] = Field(None, description="Set when primary agent failed and a fallback was triggered")


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


class ReportRequest(BaseModel):
    """Report generation request."""
    neighborhood: str = Field(..., description="Neighborhood name")

    class Config:
        json_schema_extra = {"example": {"neighborhood": "Dorchester"}}


class ReportResponse(BaseModel):
    """Report generation response."""
    report_id: str = Field(..., description="Unique report identifier")
    neighborhood: str
    status: str = Field("pending", description="Status: pending, processing, completed, failed")
    pdf_path: Optional[str] = Field(None, description="Path to generated PDF")
    url: Optional[str] = Field(None, description="Download URL")
    created_at: str
    completed_at: Optional[str] = None
    message: str


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: str
    snowflake_connected: bool


# ══════════════════════════════════════════════════════════════════════════════
# REPORT STORAGE
# ══════════════════════════════════════════════════════════════════════════════

reports_db: Dict[str, Dict[str, Any]] = {}
REPORTS_DIR = Path(__file__).resolve().parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_snowflake_conn():
    """Get Snowflake connection."""
    try:
        from shared.snowflake_conn import get_conn
        return get_conn()
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")


def run_query(query: str, conn):
    """Execute query on Snowflake."""
    try:
        from shared.snowflake_conn import run_query as sf_run_query
        return sf_run_query(query, conn)
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


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
    """Root endpoint — API documentation."""
    return {
        "name": "NeighbourWise AI API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "endpoints": {
            "overview": {
                "neighborhoods":      "GET /overview/neighborhoods",
                "kpis":               "GET /overview/kpis",
                "map":                "GET /overview/map",
                "crime_summary":      "GET /overview/crime-summary",
                "domain_safety":      "GET /overview/domain/safety",
                "domain_housing":     "GET /overview/domain/housing",
                "domain_transit":     "GET /overview/domain/transit",
                "domain_grocery":     "GET /overview/domain/grocery",
                "domain_healthcare":  "GET /overview/domain/healthcare",
                "domain_schools":     "GET /overview/domain/schools",
                "domain_restaurants": "GET /overview/domain/restaurants",
                "domain_universities":"GET /overview/domain/universities",
                "domain_bluebikes":   "GET /overview/domain/bluebikes",
            },
            "query":         "POST /query",
            "neighborhoods": "GET /neighborhoods",
            "report": {
                "generate": "POST /report/generate",
                "status":   "GET /report/{report_id}",
                "download": "GET /report/{report_id}/download",
                "list":     "GET /report",
            },
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# NEIGHBORHOODS ENDPOINT (legacy — kept for backwards compatibility)
# Use /overview/neighborhoods for the sidebar dropdown
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/neighborhoods", tags=["Neighborhoods"])
async def list_neighborhoods():
    """Get list of all neighborhoods (raw). Use /overview/neighborhoods for the sidebar dropdown."""
    conn = get_snowflake_conn()
    try:
        from shared.snowflake_conn import get_all_neighborhoods
        neighborhoods = sorted(get_all_neighborhoods(conn))
        return {"count": len(neighborhoods), "neighborhoods": neighborhoods}
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# QUERY ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/query", response_model=QueryResponse, tags=["Query"])
async def process_query(request: QueryRequest):
    """
    Route and process a natural language query about Boston neighborhoods.

    Routing logic (handled internally by router_agent):
    - report keywords                          → report       (report_agent)
    - chart/visualization keywords             → chart        (graphic_agent)
    - web-only signals (news, weather today)   → web_search   (web_search_agent)
    - livability/comparison intent             → graph_query  (Graph_agent + Neo4j)
    - 2+ domains detected                      → graph_query  (Graph_agent + Neo4j)
    - 1 domain detected                        → data_query   (SQL + RAG)
    - nothing matches                          → web_search

    Pass domain_filter to force a specific domain and skip auto-detection.
    The routing field in the response describes the decision taken.
    """
    logger.info(f"[/query] Received: {request.query!r}  domain_filter={request.domain_filter!r}")
    t_start = time.time()

    conn = None
    try:
        from router_agent import route
        from shared.snowflake_conn import get_conn

        conn = get_conn()
        result = route(request.query, conn, domain_filter=request.domain_filter)
        elapsed = round(time.time() - t_start, 2)

        # Propagate fallback info if present
        routing_data = result.get("routing")
        fallback = result.get("routing_fallback")
        if routing_data and fallback:
            if isinstance(routing_data, dict):
                routing_data["fallback_used"] = fallback
            elif hasattr(routing_data, "fallback_used"):
                routing_data.fallback_used = fallback

        logger.info(
            f"[/query] Completed in {elapsed}s  "
            f"type={result.get('type', 'data_query')!r}  "
            f"fallback={fallback!r}"
        )

        return QueryResponse(
            type=result.get("type", "data_query"),
            answer=result.get("answer", ""),
            neighborhood=result.get("neighborhood"),
            domain=result.get("domain"),
            domains=result.get("domains", []),
            confidence=float(result.get("confidence", 0.0)),
            elapsed=elapsed,
            routing=routing_data,
            sql=result.get("sql"),
            results=result.get("results"),
            rag_chunks=result.get("rag_chunks"),
            validation=result.get("validation"),
            chart_path=result.get("path"),
            image_paths=result.get("paths"),
            error=result.get("error"),
        )

    except Exception as e:
        logger.error(f"[/query] Failed: {e}", exc_info=True)
        return QueryResponse(
            type="error",
            answer="",
            error=str(e),
            elapsed=round(time.time() - t_start, 2),
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# REPORT ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

def _generate_report_background(report_id: str, neighborhood: str):
    """
    Background task to generate a neighborhood PDF report.
    Calls report_agent.generate_report() which manages its own Snowflake connection.
    """
    try:
        reports_db[report_id]["status"] = "processing"
        from report_agent import generate_report

        logger.info(f"[Report {report_id}] Generating report for {neighborhood}")
        pdf_path = generate_report(neighborhood)

        if pdf_path:
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
                "message": "Report generation returned no PDF path",
            })

    except Exception as e:
        logger.error(f"[Report {report_id}] Generation failed: {e}", exc_info=True)
        reports_db[report_id].update({
            "status": "failed",
            "message": str(e),
        })


@app.post("/report/generate", response_model=ReportResponse, tags=["Report"])
async def generate_report_endpoint(request: ReportRequest, background_tasks: BackgroundTasks):
    """
    Start async neighborhood report generation.

    Returns immediately with a report_id.
    Poll GET /report/{report_id} for status.
    Download via GET /report/{report_id}/download when status == 'completed'.
    Typical generation time: 3–5 minutes.
    """
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

    # generate_report() creates its own Snowflake connection — no conn passed
    background_tasks.add_task(_generate_report_background, report_id, request.neighborhood)

    return ReportResponse(**reports_db[report_id])


@app.get("/report/{report_id}", response_model=ReportResponse, tags=["Report"])
async def get_report_status(report_id: str):
    """Get the status of a report generation task."""
    if report_id not in reports_db:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")
    return ReportResponse(**reports_db[report_id])


@app.get("/report", tags=["Report"])
async def list_reports():
    """List all generated reports in this session."""
    return {
        "count": len(reports_db),
        "reports": [
            {
                "report_id": rid,
                "neighborhood": data["neighborhood"],
                "status": data["status"],
                "created_at": data["created_at"],
                "completed_at": data.get("completed_at"),
                "url": data.get("url"),
            }
            for rid, data in reports_db.items()
        ]
    }


@app.get("/report/{report_id}/download", tags=["Report"])
async def download_report(report_id: str):
    """Download a completed neighborhood report PDF."""
    if report_id not in reports_db:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")

    report_data = reports_db[report_id]

    if report_data["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Report is still {report_data['status']}. Poll /report/{report_id} for status."
        )

    pdf_path = report_data.get("pdf_path")
    if not pdf_path or not Path(pdf_path).exists():
        raise HTTPException(status_code=404, detail="PDF file not found on disk")

    return FileResponse(
        path=pdf_path,
        filename=f"{report_data['neighborhood'].lower().replace(' ', '_')}_report.pdf",
        media_type="application/pdf",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        "neighbourwise_fastapi:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info",
    )