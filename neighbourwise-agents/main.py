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
# INITIALIZE FASTAPI
# ══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="NeighbourWise AI API",
    description="Boston neighborhood intelligence via SQL + RAG + Graph + Web Search",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure based on your frontend URL
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
    domain_filter: Optional[str] = Field(None, description="Filter to specific domain (e.g., SAFETY, HOUSING)")
    
    class Config:
        example = {
            "query": "Is Allston safe and affordable?",
            "domain_filter": None
        }


class QueryResponse(BaseModel):
    """Query response from router agent."""
    type: str = Field(..., description="Intent type: data_query, chart, image, web_search, report, graph_query")
    answer: str = Field(..., description="Synthesized answer")
    neighborhood: Optional[str] = Field(None, description="Detected neighborhood (if any)")
    domain: Optional[str] = Field(None, description="Detected domain")
    confidence: float = Field(0.0, description="Classification confidence (0-1)")
    elapsed: float = Field(0.0, description="Query execution time in seconds")
    sql: Optional[str] = Field(None, description="Executed SQL (if data_query)")
    results: Optional[List[Dict]] = Field(None, description="SQL results (if data_query)")
    rag_chunks: Optional[List[Dict]] = Field(None, description="RAG sources (if applicable)")
    validation: Optional[Dict] = Field(None, description="Validation feedback")
    chart_path: Optional[str] = Field(None, description="Path to generated chart (if chart)")
    image_paths: Optional[List[str]] = Field(None, description="Paths to generated images (if image)")
    error: Optional[str] = Field(None, description="Error message (if failed)")


class OverviewStats(BaseModel):
    """Overview statistics."""
    total_neighborhoods: int
    avg_master_score: float
    top_score: float
    safest_neighborhood: Optional[str]
    safest_score: Optional[float]
    most_affordable: Optional[str]
    affordable_rent: Optional[float]


class SafetyRecord(BaseModel):
    """Safety record for a neighborhood."""
    neighborhood_name: str
    safety_score: float
    safety_grade: str
    total_incidents: int
    violent_crime_count: int


class NeighborhoodSummary(BaseModel):
    """Summary for a neighborhood."""
    name: str
    city: str
    master_score: float
    master_grade: str
    top_strength: str
    top_weakness: str


class ReportRequest(BaseModel):
    """Report generation request."""
    neighborhood: str = Field(..., description="Neighborhood name")
    
    class Config:
        example = {"neighborhood": "Dorchester"}


class ReportResponse(BaseModel):
    """Report generation response."""
    report_id: str = Field(..., description="Unique report identifier")
    neighborhood: str
    status: str = Field("pending", description="Status: pending, processing, completed, failed")
    pdf_path: Optional[str] = Field(None, description="Path to generated PDF")
    url: Optional[str] = Field(None, description="Download URL")
    created_at: str
    completed_at: Optional[str]
    message: str


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: str
    snowflake_connected: bool


# ══════════════════════════════════════════════════════════════════════════════
# STORAGE FOR LONG-RUNNING TASKS
# ══════════════════════════════════════════════════════════════════════════════

# Store report generation tasks and their status
reports_db: Dict[str, Dict[str, Any]] = {}
REPORTS_DIR = Path(__file__).resolve().parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE & AGENT CONNECTIONS
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
# ROOT ENDPOINT
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
# OVERVIEW ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/overview/stats", response_model=OverviewStats, tags=["Overview"])
async def get_overview_stats():
    """Get overview statistics (total neighborhoods, scores, safest, cheapest)."""
    conn = get_snowflake_conn()
    try:
        # Total neighborhoods and scores
        df = run_query("""
            SELECT COUNT(*) AS TOTAL_NEIGHBORHOODS,
                   ROUND(AVG(MASTER_SCORE),1) AS AVG_MASTER_SCORE,
                   MAX(MASTER_SCORE) AS TOP_SCORE
            FROM NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
            WHERE MASTER_SCORE IS NOT NULL
        """, conn)
        result = df.iloc[0].to_dict() if not df.empty else {}

        # Safest neighborhood
        df_safe = run_query("""
            SELECT NEIGHBORHOOD_NAME, SAFETY_SCORE
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
            WHERE SAFETY_SCORE IS NOT NULL
            ORDER BY SAFETY_SCORE DESC LIMIT 1
        """, conn)
        safest_name = df_safe.iloc[0]["NEIGHBORHOOD_NAME"].title() if not df_safe.empty else None
        safest_score = float(df_safe.iloc[0]["SAFETY_SCORE"]) if not df_safe.empty else None

        # Most affordable
        df_afford = run_query("""
            SELECT NEIGHBORHOOD_NAME, HOUSING_SCORE, AVG_ESTIMATED_RENT
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING
            WHERE HOUSING_SCORE IS NOT NULL
            ORDER BY HOUSING_SCORE DESC LIMIT 1
        """, conn)
        cheapest_name = df_afford.iloc[0]["NEIGHBORHOOD_NAME"].title() if not df_afford.empty else None
        cheapest_rent = float(df_afford.iloc[0]["AVG_ESTIMATED_RENT"]) if not df_afford.empty and pd.notna(df_afford.iloc[0]["AVG_ESTIMATED_RENT"]) else None

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
    """Get safety scores for all neighborhoods (top 10 by default)."""
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
            ]
        }
    finally:
        conn.close()


@app.get("/overview/hotspots", tags=["Overview"])
async def get_crime_hotspots():
    """Get crime hotspot clusters."""
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
            ]
        }
    finally:
        conn.close()


@app.get("/overview/master-scores", tags=["Overview"])
async def get_master_scores(limit: int = Query(51, ge=1, le=100)):
    """Get master scores (livability) for neighborhoods."""
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
            ]
        }
    finally:
        conn.close()


@app.get("/overview/crime-narratives", tags=["Overview"])
async def get_crime_narratives():
    """Get crime forecasts and trends."""
    conn = get_snowflake_conn()
    try:
        df = run_query("""
            SELECT NEIGHBORHOOD_NAME, RECENT_TREND, RECENT_AVG_MONTHLY,
                   FORECAST_MONTH, FORECASTED_COUNT, TRAIN_MAPE,
                   N_HOTSPOT_CLUSTERS, SAFETY_NARRATIVE, RELIABILITY_FLAG
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE
            ORDER BY FORECASTED_COUNT DESC
        """, conn)
        
        # Count by trend
        trend_counts = {
            "increasing": int((df["RECENT_TREND"] == "increasing").sum()) if len(df) > 0 else 0,
            "decreasing": int((df["RECENT_TREND"] == "decreasing").sum()) if len(df) > 0 else 0,
            "stable": int((df["RECENT_TREND"] == "stable").sum()) if len(df) > 0 else 0,
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
            ]
        }
    finally:
        conn.close()


@app.get("/overview/choropleth", tags=["Overview"])
async def get_safety_choropleth():
    """Get GeoJSON data for safety map visualization."""
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
                    }
                })
            except Exception as e:
                logger.warning(f"Failed to parse geometry for {row['NEIGHBORHOOD_NAME']}: {e}")
                continue
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        return geojson
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# NEIGHBORHOODS ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/neighborhoods", tags=["Neighborhoods"])
async def list_neighborhoods():
    """Get list of all Boston neighborhoods."""
    conn = get_snowflake_conn()
    try:
        from shared.snowflake_conn import get_all_neighborhoods
        neighborhoods = sorted(get_all_neighborhoods(conn))
        return {
            "count": len(neighborhoods),
            "neighborhoods": neighborhoods
        }
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# QUERY ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/query", response_model=QueryResponse, tags=["Query"])
async def process_query(request: QueryRequest):
    """
    Route and process a natural language query about Boston neighborhoods.
    
    Returns different response types based on query intent:
    - data_query: SQL + RAG synthesis
    - chart: Generated visualization
    - image: Generated images (DALL-E)
    - web_search: Web search results
    - graph_query: Cross-domain Neo4j analysis
    - report: PDF report generation
    """
    logger.info(f"Processing query: {request.query}")
    conn = get_snowflake_conn()
    t_start = time.time()
    
    try:
        from router_agent import route
        
        result = route(
            request.query,
            conn,
            domain_filter=request.domain_filter
        )
        
        elapsed = time.time() - t_start
        
        # Transform result for API response
        response_data = {
            "type": result.get("type", "data_query"),
            "answer": result.get("answer", ""),
            "neighborhood": result.get("neighborhood"),
            "domain": result.get("domain"),
            "confidence": result.get("confidence", 0.0),
            "elapsed": elapsed,
            "sql": result.get("sql"),
            "results": result.get("results"),
            "rag_chunks": result.get("rag_chunks"),
            "validation": result.get("validation"),
            "chart_path": result.get("path"),  # for chart type
            "image_paths": result.get("paths"),  # for image type
            "error": result.get("error"),
        }
        
        return QueryResponse(**response_data)
        
    except Exception as e:
        logger.error(f"Query processing failed: {e}", exc_info=True)
        return QueryResponse(
            type="error",
            answer="",
            error=str(e),
            elapsed=time.time() - t_start,
        )
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# REPORT ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

def _generate_report_background(report_id: str, neighborhood: str, conn):
    """Background task to generate neighborhood report."""
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
        reports_db[report_id].update({
            "status": "failed",
            "message": str(e),
        })


@app.post("/report/generate", response_model=ReportResponse, tags=["Report"])
async def generate_report(request: ReportRequest, background_tasks: BackgroundTasks):
    """
    Generate a comprehensive neighborhood report.
    
    Returns immediately with a report_id. Poll /report/{report_id} for status.
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
    
    # Queue report generation as background task
    conn = get_snowflake_conn()
    background_tasks.add_task(_generate_report_background, report_id, request.neighborhood, conn)
    
    return ReportResponse(**reports_db[report_id])


@app.get("/report/{report_id}", response_model=ReportResponse, tags=["Report"])
async def get_report_status(report_id: str):
    """Get the status of a report generation task."""
    if report_id not in reports_db:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")
    
    return ReportResponse(**reports_db[report_id])


@app.get("/report", tags=["Report"])
async def list_reports():
    """List all generated reports."""
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
        ]
    }


@app.get("/report/{report_id}/download", tags=["Report"])
async def download_report(report_id: str):
    """Download a generated report PDF."""
    if report_id not in reports_db:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")
    
    report_data = reports_db[report_id]
    
    if report_data["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Report still {report_data['status']}. Check /report/{report_id} for status."
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
    """Not found response."""
    raise HTTPException(status_code=404, detail="Endpoint not found. Check /docs for valid endpoints.")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
