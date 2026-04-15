"""
NeighbourWise AI — Overview Endpoints
══════════════════════════════════════════════════════════════════════════════
All column names verified against Table_schemas.tsv (April 10, 2026).

Mount into neighbourwise_fastapi.py with:
    from overview_endpoints import router as overview_router
    app.include_router(overview_router)

Endpoints:
  GET /overview/neighborhoods          — neighborhood list for sidebar dropdown
  GET /overview/kpis                   — top-10 KPI cards (home page hero)
  GET /overview/map                    — GeoJSON choropleth
  GET /overview/crime-summary          — high-level trend summary widget

  GET /overview/domain/safety          — scores + SARIMAX forecasts + DBSCAN hotspots
  GET /overview/domain/housing         — affordability breakdown
  GET /overview/domain/transit         — MBTA + BlueBikes
  GET /overview/domain/grocery         — store types + food access
  GET /overview/domain/healthcare      — facility profiles + hotspots
  GET /overview/domain/schools         — level coverage + type mix
  GET /overview/domain/restaurants     — density + cuisine diversity
  GET /overview/domain/universities    — presence + research + housing
  GET /overview/domain/bluebikes       — stations + docks + density

All domain endpoints accept optional ?neighborhood= query param.

Table name reference (all single-u NEIGHBORHOOD confirmed):
  MARTS.MRT_NEIGHBORHOOD_SAFETY
  MARTS.MRT_NEIGHBORHOOD_HOUSING
  MARTS.MRT_NEIGHBORHOOD_MBTA
  MARTS.MRT_NEIGHBORHOOD_BLUEBIKES
  MARTS.MRT_NEIGHBORHOOD_SCHOOLS
  MARTS.MRT_NEIGHBORHOOD_RESTAURANTS
  MARTS.MRT_NEIGHBORHOOD_GROCERY_STORES
  MARTS.MRT_NEIGHBORHOOD_HEALTHCARE
  MARTS.MRT_NEIGHBORHOOD_UNIVERSITIES
  MARTS.MASTER_LOCATION
  ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
  CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE
  CRIME_ANALYSIS.CA_CRIME_FORECAST
  CRIME_ANALYSIS.CA_CRIME_HOTSPOT_CLUSTERS
  CRIME_ANALYSIS.GA_GROCERY_HOTSPOT_CLUSTERS
  HEALTHCARE_ANALYSIS.HA_HEALTHCARE_ACCESS_PROFILE
  HEALTHCARE_ANALYSIS.HA_HEALTHCARE_HOTSPOT_CLUSTERS
"""

import logging
from typing import Optional
import pandas as pd

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/overview", tags=["Overview"])

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

HARBOR_FILTER = "AND UPPER(NEIGHBORHOOD_NAME) != 'HARBOR ISLANDS'"


def _get_conn():
    try:
        from shared.snowflake_conn import get_conn
        return get_conn()
    except Exception as e:
        logger.error(f"Snowflake connection failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")


def _run(sql: str, conn) -> pd.DataFrame:
    try:
        from shared.snowflake_conn import run_query
        return run_query(sql, conn)
    except Exception as e:
        logger.error(f"Query failed: {e}\nSQL: {sql}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


def _hood(neighborhood: Optional[str], col: str = "NEIGHBORHOOD_NAME") -> str:
    """Return an AND clause for neighborhood filtering, or empty string."""
    if not neighborhood or neighborhood.strip().upper() == "ALL":
        return ""
    safe = neighborhood.replace("'", "''").upper()
    return f"AND UPPER({col}) = '{safe}'"


def _title(val) -> str:
    if not val:
        return ""
    result = str(val).title()
    result = result.replace("2/Mit", "2/MIT")
    return result


def _f(val) -> Optional[float]:
    try:
        return float(val) if pd.notna(val) else None
    except (TypeError, ValueError):
        return None


def _i(val) -> Optional[int]:
    try:
        return int(val) if pd.notna(val) else None
    except (TypeError, ValueError):
        return None


def _s(row, col) -> Optional[str]:
    """Safe string from row — returns None if null."""
    v = row.get(col)
    return str(v) if pd.notna(v) else None


# ─────────────────────────────────────────────────────────────────────────────
# NEIGHBORHOOD LIST
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/neighborhoods")
async def list_neighborhoods():
    """All neighborhood names for the sidebar dropdown. Harbor Islands excluded."""
    conn = _get_conn()
    try:
        df = _run(f"""
            SELECT DISTINCT NEIGHBORHOOD_NAME, CITY
            FROM NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
            WHERE MASTER_SCORE IS NOT NULL
              {HARBOR_FILTER}
            ORDER BY NEIGHBORHOOD_NAME
        """, conn)
        return {
            "count": len(df),
            "neighborhoods": [
                {"name": _title(r["NEIGHBORHOOD_NAME"]), "city": _title(r["CITY"])}
                for _, r in df.iterrows()
            ]
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# KPI CARDS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/kpis")
async def get_kpis(neighborhood: Optional[str] = Query(None)):
    """
    Top-10 KPI lists: safest, most affordable, best transit, best overall livability.
    Pass ?neighborhood= to scope all lists to a single neighborhood.
    """
    conn = _get_conn()
    hc = _hood(neighborhood)
    limit = "" if hc else "LIMIT 10"

    try:
        # Safest
        df_safety = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, SAFETY_SCORE, SAFETY_GRADE
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
            WHERE SAFETY_SCORE IS NOT NULL
              AND SAFETY_GRADE != 'INSUFFICIENT DATA'
              {HARBOR_FILTER} {hc}
            ORDER BY SAFETY_SCORE DESC {limit}
        """, conn)

        # Most affordable (higher HOUSING_SCORE = more affordable)
        df_housing = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, HOUSING_SCORE, HOUSING_GRADE,
                   AVG_ESTIMATED_RENT, AVG_PRICE_PER_SQFT
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING
            WHERE HOUSING_SCORE IS NOT NULL
              AND HOUSING_GRADE NOT IN ('INSUFFICIENT DATA')
              AND INSUFFICIENT_DATA = FALSE
              {HARBOR_FILTER} {hc}
            ORDER BY HOUSING_SCORE DESC {limit}
        """, conn)

        # Best transit
        df_transit = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, TRANSIT_SCORE, TRANSIT_GRADE,
                   HAS_RAPID_TRANSIT, RAPID_TRANSIT_ROUTE_NAMES, TOTAL_ROUTES
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_MBTA
            WHERE TRANSIT_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY TRANSIT_SCORE DESC {limit}
        """, conn)

        # Best overall livability
        df_master = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, MASTER_SCORE, MASTER_GRADE,
                   TOP_STRENGTH, TOP_WEAKNESS
            FROM NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
            WHERE MASTER_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY MASTER_SCORE DESC {limit}
        """, conn)

        def fmt_safety(r):
            return {"neighborhood": _title(r["NEIGHBORHOOD_NAME"]), "city": _title(r["CITY"]),
                    "score": _f(r["SAFETY_SCORE"]), "grade": r["SAFETY_GRADE"]}

        def fmt_housing(r):
            return {"neighborhood": _title(r["NEIGHBORHOOD_NAME"]), "city": _title(r["CITY"]),
                    "score": _f(r["HOUSING_SCORE"]), "grade": r["HOUSING_GRADE"],
                    "avg_monthly_rent": _f(r["AVG_ESTIMATED_RENT"]),
                    "price_per_sqft": _f(r["AVG_PRICE_PER_SQFT"])}

        def fmt_transit(r):
            lines = _s(r, "RAPID_TRANSIT_ROUTE_NAMES")
            return {"neighborhood": _title(r["NEIGHBORHOOD_NAME"]), "city": _title(r["CITY"]),
                    "score": _f(r["TRANSIT_SCORE"]), "grade": r["TRANSIT_GRADE"],
                    "has_rapid_transit": bool(r["HAS_RAPID_TRANSIT"]),
                    "rapid_transit_lines": lines if lines and lines != "NO RAPID TRANSIT" else None,
                    "total_routes": _i(r["TOTAL_ROUTES"])}

        def fmt_master(r):
            return {"neighborhood": _title(r["NEIGHBORHOOD_NAME"]), "city": _title(r["CITY"]),
                    "score": _f(r["MASTER_SCORE"]), "grade": r["MASTER_GRADE"],
                    "top_strength": _s(r, "TOP_STRENGTH"), "top_weakness": _s(r, "TOP_WEAKNESS")}

        return {
            "filter": {"neighborhood": neighborhood or "ALL"},
            "safest": [fmt_safety(r) for _, r in df_safety.iterrows()],
            "most_affordable": [fmt_housing(r) for _, r in df_housing.iterrows()],
            "best_transit": [fmt_transit(r) for _, r in df_transit.iterrows()],
            "best_overall": [fmt_master(r) for _, r in df_master.iterrows()],
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# MAP — GeoJSON choropleth
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/map")
async def get_map():
    """GeoJSON FeatureCollection for the safety choropleth map."""
    conn = _get_conn()
    try:
        import json
        df = _run("""
            SELECT
                ml.NEIGHBORHOOD_NAME,
                ml.CITY,
                ml.CENTROID_LAT,
                ml.CENTROID_LONG,
                ST_ASGEOJSON(ml.GEOMETRY)::VARCHAR  AS GEOJSON,
                ns.SAFETY_SCORE,
                ns.SAFETY_GRADE,
                ms.MASTER_SCORE,
                ms.MASTER_GRADE,
                ms.TOP_STRENGTH,
                ms.TOP_WEAKNESS
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MASTER_LOCATION ml
            INNER JOIN NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY ns
                ON UPPER(ml.NEIGHBORHOOD_NAME) = UPPER(ns.NEIGHBORHOOD_NAME)
            INNER JOIN NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE ms
                ON UPPER(ml.NEIGHBORHOOD_NAME) = UPPER(ms.NEIGHBORHOOD_NAME)
            WHERE ml.CITY IN ('BOSTON', 'CAMBRIDGE')
              AND ml.GRANULARITY = 'NEIGHBORHOOD'
              AND ns.SAFETY_SCORE IS NOT NULL
              AND ns.SAFETY_GRADE != 'INSUFFICIENT DATA'
              AND ml.HAS_GEOMETRY = TRUE
              AND ml.CENTROID_LAT IS NOT NULL
              AND UPPER(ml.NEIGHBORHOOD_NAME) != 'HARBOR ISLANDS'
        """, conn)

        features = []
        for _, row in df.iterrows():
            try:
                geom = json.loads(row["GEOJSON"])
                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "neighborhood": _title(row["NEIGHBORHOOD_NAME"]),
                        "city": _title(row["CITY"]),
                        "latitude": _f(row["CENTROID_LAT"]),
                        "longitude": _f(row["CENTROID_LONG"]),
                        "safety_score": _f(row["SAFETY_SCORE"]),
                        "safety_grade": row["SAFETY_GRADE"],
                        "master_score": _f(row["MASTER_SCORE"]),
                        "master_grade": row["MASTER_GRADE"],
                        "top_strength": _s(row, "TOP_STRENGTH"),
                        "top_weakness": _s(row, "TOP_WEAKNESS"),
                    }
                })
            except Exception as e:
                logger.warning(f"Skipping geometry for {row['NEIGHBORHOOD_NAME']}: {e}")

        return {"type": "FeatureCollection", "count": len(features), "features": features}
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CRIME SUMMARY WIDGET
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/crime-summary")
async def get_crime_summary():
    """High-level city-wide crime trend summary for the home page widget."""
    conn = _get_conn()
    try:
        df_trend = _run(f"""
            SELECT RECENT_TREND,
                   COUNT(*) AS NEIGHBORHOOD_COUNT,
                   ROUND(AVG(RECENT_AVG_MONTHLY), 1) AS AVG_MONTHLY_INCIDENTS,
                   ROUND(AVG(FORECASTED_COUNT), 1)   AS AVG_FORECAST_NEXT_MONTH
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE
            WHERE RELIABILITY_FLAG = 'HIGH'
              {HARBOR_FILTER}
            GROUP BY RECENT_TREND
            ORDER BY NEIGHBORHOOD_COUNT DESC
        """, conn)

        df_top = _run(f"""
            SELECT NEIGHBORHOOD_NAME, FORECASTED_COUNT, RECENT_TREND
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE
            WHERE RELIABILITY_FLAG = 'HIGH'
              {HARBOR_FILTER}
            ORDER BY FORECASTED_COUNT DESC
            LIMIT 5
        """, conn)

        df_improving = _run(f"""
            SELECT NEIGHBORHOOD_NAME, RECENT_AVG_MONTHLY
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE
            WHERE RECENT_TREND = 'decreasing'
              AND RELIABILITY_FLAG = 'HIGH'
              {HARBOR_FILTER}
            ORDER BY RECENT_AVG_MONTHLY DESC
            LIMIT 5
        """, conn)

        return {
            "trend_summary": {
                r["RECENT_TREND"]: {
                    "neighborhood_count": _i(r["NEIGHBORHOOD_COUNT"]),
                    "avg_monthly_incidents": _f(r["AVG_MONTHLY_INCIDENTS"]),
                    "avg_forecast_next_month": _f(r["AVG_FORECAST_NEXT_MONTH"]),
                }
                for _, r in df_trend.iterrows()
            },
            "highest_volume_next_month": [
                {"neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                 "forecasted_count": _f(r["FORECASTED_COUNT"]),
                 "trend": r["RECENT_TREND"]}
                for _, r in df_top.iterrows()
            ],
            "most_improved": [
                {"neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                 "avg_monthly_incidents": _f(r["RECENT_AVG_MONTHLY"])}
                for _, r in df_improving.iterrows()
            ],
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: SAFETY
# Verified: MRT_NEIGHBORHOOD_SAFETY, CA_CRIME_SAFETY_NARRATIVE,
#           CA_CRIME_FORECAST, CA_CRIME_HOTSPOT_CLUSTERS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/safety")
async def domain_safety(neighborhood: Optional[str] = Query(None)):
    """Safety domain deep dive: scores + SARIMAX forecasts + DBSCAN hotspot clusters."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df_scores = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, SAFETY_SCORE, SAFETY_GRADE,
                   TOTAL_INCIDENTS, VIOLENT_CRIME_COUNT, PROPERTY_CRIME_COUNT,
                   DRUG_RELATED_COUNT, PCT_VIOLENT, PCT_PROPERTY, PCT_DRUG_RELATED,
                   YOY_CHANGE_PCT, NIGHT_CRIME_COUNT, PCT_NIGHT_CRIMES,
                   MOST_COMMON_OFFENSE, AVG_MONTHLY_INCIDENTS,
                   INCIDENTS_LAST_12M, INCIDENTS_PRIOR_12M, SAFETY_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY
            WHERE SAFETY_SCORE IS NOT NULL
              AND SAFETY_GRADE != 'INSUFFICIENT DATA'
              {HARBOR_FILTER} {hc}
            ORDER BY SAFETY_SCORE DESC
        """, conn)

        df_forecast = _run(f"""
            SELECT NEIGHBORHOOD_NAME, FORECAST_MONTH, FORECASTED_COUNT,
                   LOWER_CI, UPPER_CI, TRAIN_MAPE
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_FORECAST
            WHERE 1=1 {hc}
            ORDER BY NEIGHBORHOOD_NAME, FORECAST_MONTH
        """, conn)

        df_hotspots = _run(f"""
            SELECT NEIGHBORHOOD_NAME, TOTAL_CRIMES, N_HOTSPOT_CLUSTERS,
                   HOTSPOT_CRIME_SHARE_PCT, NOISE_CRIME_PCT
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_HOTSPOT_CLUSTERS
            WHERE 1=1 {hc}
            ORDER BY HOTSPOT_CRIME_SHARE_PCT DESC
        """, conn)

        df_narrative = _run(f"""
            SELECT NEIGHBORHOOD_NAME, RECENT_TREND, RECENT_AVG_MONTHLY,
                   FORECAST_MONTH, FORECASTED_COUNT, TRAIN_MAPE,
                   N_HOTSPOT_CLUSTERS, SAFETY_NARRATIVE, RELIABILITY_FLAG
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE
            WHERE 1=1 {hc}
            ORDER BY FORECASTED_COUNT DESC
        """, conn)

        return {
            "domain": "safety",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "scores": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "safety_score": _f(r["SAFETY_SCORE"]),
                    "safety_grade": r["SAFETY_GRADE"],
                    "total_incidents": _i(r["TOTAL_INCIDENTS"]),
                    "violent_crimes": _i(r["VIOLENT_CRIME_COUNT"]),
                    "property_crimes": _i(r["PROPERTY_CRIME_COUNT"]),
                    "drug_related": _i(r["DRUG_RELATED_COUNT"]),
                    "pct_violent": _f(r["PCT_VIOLENT"]),
                    "pct_property": _f(r["PCT_PROPERTY"]),
                    "pct_drug_related": _f(r["PCT_DRUG_RELATED"]),
                    "yoy_change_pct": _f(r["YOY_CHANGE_PCT"]),
                    "night_crimes": _i(r["NIGHT_CRIME_COUNT"]),
                    "pct_night_crimes": _f(r["PCT_NIGHT_CRIMES"]),
                    "most_common_offense": _s(r, "MOST_COMMON_OFFENSE"),
                    "avg_monthly_incidents": _i(r["AVG_MONTHLY_INCIDENTS"]),
                    "incidents_last_12m": _i(r["INCIDENTS_LAST_12M"]),
                    "incidents_prior_12m": _i(r["INCIDENTS_PRIOR_12M"]),
                    "description": _s(r, "SAFETY_DESCRIPTION"),
                }
                for _, r in df_scores.iterrows()
            ],
            "forecasts": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "forecast_month": str(r["FORECAST_MONTH"]),
                    "forecasted_count": _f(r["FORECASTED_COUNT"]),
                    "lower_ci": _f(r["LOWER_CI"]),
                    "upper_ci": _f(r["UPPER_CI"]),
                    "train_mape": _f(r["TRAIN_MAPE"]),
                }
                for _, r in df_forecast.iterrows()
            ],
            "hotspots": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "total_crimes": _i(r["TOTAL_CRIMES"]),
                    "hotspot_clusters": _i(r["N_HOTSPOT_CLUSTERS"]),
                    "hotspot_crime_share_pct": _f(r["HOTSPOT_CRIME_SHARE_PCT"]),
                    "noise_crime_pct": _f(r["NOISE_CRIME_PCT"]),
                }
                for _, r in df_hotspots.iterrows()
            ],
            "narrative": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "recent_trend": r["RECENT_TREND"],
                    "recent_avg_monthly": _f(r["RECENT_AVG_MONTHLY"]),
                    "forecast_month": str(r["FORECAST_MONTH"]),
                    "forecasted_count": _f(r["FORECASTED_COUNT"]),
                    "train_mape": _f(r["TRAIN_MAPE"]),
                    "hotspot_clusters": _i(r["N_HOTSPOT_CLUSTERS"]),
                    "safety_narrative": _s(r, "SAFETY_NARRATIVE"),
                    "reliability": r["RELIABILITY_FLAG"],
                }
                for _, r in df_narrative.iterrows()
            ],
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: HOUSING
# Verified: MRT_NEIGHBORHOOD_HOUSING
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/housing")
async def domain_housing(neighborhood: Optional[str] = Query(None)):
    """Housing domain deep dive. Higher score = more affordable."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, HOUSING_SCORE, HOUSING_GRADE,
                   AVG_ESTIMATED_RENT, AVG_PRICE_PER_SQFT, AVG_ASSESSED_VALUE,
                   AVG_LIVING_AREA_SQFT, AVG_BEDROOMS, AVG_FULL_BATHS,
                   TOTAL_PROPERTIES, CONDO_COUNT, RENTAL_COUNT, RESIDENTIAL_COUNT,
                   AVG_PROPERTY_AGE, OLDEST_YEAR_BUILT, NEWEST_YEAR_BUILT,
                   PCT_HAS_AC, PCT_HAS_PARKING, AMENITY_RATE,
                   PCT_GOOD_CONDITION, AVG_CONDITION_SCORE,
                   PROPERTIES_PER_SQMILE, ROW_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING
            WHERE HOUSING_GRADE NOT IN ('INSUFFICIENT DATA')
              AND INSUFFICIENT_DATA = FALSE
              {HARBOR_FILTER} {hc}
            ORDER BY HOUSING_SCORE DESC
        """, conn)

        return {
            "domain": "housing",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "note": "Higher score = more affordable relative to Boston/Cambridge market.",
            "grade_distribution": df["HOUSING_GRADE"].value_counts().to_dict() if not df.empty else {},
            "neighborhoods": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "housing_score": _f(r["HOUSING_SCORE"]),
                    "housing_grade": r["HOUSING_GRADE"],
                    "avg_monthly_rent": _f(r["AVG_ESTIMATED_RENT"]),
                    "avg_price_per_sqft": _f(r["AVG_PRICE_PER_SQFT"]),
                    "avg_assessed_value": _f(r["AVG_ASSESSED_VALUE"]),
                    "avg_living_area_sqft": _i(r["AVG_LIVING_AREA_SQFT"]),
                    "avg_bedrooms": _f(r["AVG_BEDROOMS"]),
                    "avg_full_baths": _f(r["AVG_FULL_BATHS"]),
                    "total_properties": _i(r["TOTAL_PROPERTIES"]),
                    "condo_count": _i(r["CONDO_COUNT"]),
                    "rental_count": _i(r["RENTAL_COUNT"]),
                    "avg_property_age": _i(r["AVG_PROPERTY_AGE"]),
                    "oldest_year_built": _i(r["OLDEST_YEAR_BUILT"]),
                    "newest_year_built": _i(r["NEWEST_YEAR_BUILT"]),
                    "pct_has_ac": _f(r["PCT_HAS_AC"]),
                    "pct_has_parking": _f(r["PCT_HAS_PARKING"]),
                    "amenity_rate": _f(r["AMENITY_RATE"]),
                    "pct_good_condition": _f(r["PCT_GOOD_CONDITION"]),
                    "properties_per_sqmile": _f(r["PROPERTIES_PER_SQMILE"]),
                    "description": _s(r, "ROW_DESCRIPTION"),
                }
                for _, r in df.iterrows()
            ]
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: TRANSIT
# Verified: MRT_NEIGHBORHOOD_MBTA, MRT_NEIGHBORHOOD_BLUEBIKES
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/transit")
async def domain_transit(neighborhood: Optional[str] = Query(None)):
    """Transit domain deep dive: MBTA scores + BlueBikes."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df_mbta = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, TRANSIT_SCORE, TRANSIT_GRADE,
                   TOTAL_STOPS, RAPID_TRANSIT_STOPS, COMMUTER_RAIL_STOPS,
                   BUS_STOPS, FERRY_STOPS, ACCESSIBLE_STOPS,
                   TOTAL_ROUTES, RAPID_TRANSIT_ROUTES, COMMUTER_RAIL_ROUTES,
                   BUS_ROUTES, FERRY_ROUTES, RAPID_TRANSIT_ROUTE_NAMES,
                   HAS_RAPID_TRANSIT, HAS_COMMUTER_RAIL, HAS_BUS, HAS_FERRY,
                   PCT_ACCESSIBLE_STOPS, TRANSIT_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_MBTA
            WHERE TRANSIT_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY TRANSIT_SCORE DESC
        """, conn)

        df_bikes = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, BIKESHARE_SCORE, BIKESHARE_GRADE,
                   TOTAL_STATIONS, LARGE_STATIONS, MEDIUM_STATIONS, SMALL_STATIONS,
                   TOTAL_DOCKS, AVG_DOCKS_PER_STATION, STATIONS_PER_SQMILE,
                   BIKESHARE_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_BLUEBIKES
            WHERE BIKESHARE_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY BIKESHARE_SCORE DESC
        """, conn)

        has_rapid = int(df_mbta["HAS_RAPID_TRANSIT"].sum()) if not df_mbta.empty else 0
        has_cr = int(df_mbta["HAS_COMMUTER_RAIL"].sum()) if not df_mbta.empty else 0

        return {
            "domain": "transit",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "summary": {
                "neighborhoods_with_rapid_transit": has_rapid,
                "neighborhoods_with_commuter_rail": has_cr,
                "avg_transit_score": round(float(df_mbta["TRANSIT_SCORE"].mean()), 1) if not df_mbta.empty else None,
                "avg_bikeshare_score": round(float(df_bikes["BIKESHARE_SCORE"].mean()), 1) if not df_bikes.empty else None,
            },
            "mbta": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "transit_score": _f(r["TRANSIT_SCORE"]),
                    "transit_grade": r["TRANSIT_GRADE"],
                    "total_stops": _i(r["TOTAL_STOPS"]),
                    "rapid_transit_stops": _i(r["RAPID_TRANSIT_STOPS"]),
                    "commuter_rail_stops": _i(r["COMMUTER_RAIL_STOPS"]),
                    "bus_stops": _i(r["BUS_STOPS"]),
                    "ferry_stops": _i(r["FERRY_STOPS"]),
                    "accessible_stops": _i(r["ACCESSIBLE_STOPS"]),
                    "total_routes": _i(r["TOTAL_ROUTES"]),
                    "rapid_transit_routes": _i(r["RAPID_TRANSIT_ROUTES"]),
                    "rapid_transit_lines": _s(r, "RAPID_TRANSIT_ROUTE_NAMES") if _s(r, "RAPID_TRANSIT_ROUTE_NAMES") != "NO RAPID TRANSIT" else None,
                    "has_rapid_transit": bool(r["HAS_RAPID_TRANSIT"]),
                    "has_commuter_rail": bool(r["HAS_COMMUTER_RAIL"]),
                    "has_bus": bool(r["HAS_BUS"]),
                    "has_ferry": bool(r["HAS_FERRY"]),
                    "pct_accessible_stops": _f(r["PCT_ACCESSIBLE_STOPS"]),
                    "description": _s(r, "TRANSIT_DESCRIPTION"),
                }
                for _, r in df_mbta.iterrows()
            ],
            "bluebikes": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "bikeshare_score": _f(r["BIKESHARE_SCORE"]),
                    "bikeshare_grade": r["BIKESHARE_GRADE"],
                    "total_stations": _i(r["TOTAL_STATIONS"]),
                    "large_stations": _i(r["LARGE_STATIONS"]),
                    "medium_stations": _i(r["MEDIUM_STATIONS"]),
                    "small_stations": _i(r["SMALL_STATIONS"]),
                    "total_docks": _i(r["TOTAL_DOCKS"]),
                    "avg_docks_per_station": _f(r["AVG_DOCKS_PER_STATION"]),
                    "stations_per_sqmile": _f(r["STATIONS_PER_SQMILE"]),
                    "description": _s(r, "BIKESHARE_DESCRIPTION"),
                }
                for _, r in df_bikes.iterrows()
            ],
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: GROCERY
# Verified: MRT_NEIGHBORHOOD_GROCERY_STORES, GA_GROCERY_HOTSPOT_CLUSTERS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/grocery")
async def domain_grocery(neighborhood: Optional[str] = Query(None)):
    """Grocery domain deep dive: store breakdown + food access + hotspot clusters."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df_scores = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, GROCERY_SCORE, GROCERY_GRADE,
                   TOTAL_STORES, SUPERMARKET_COUNT, CONVENIENCE_STORE_COUNT,
                   PHARMACY_COUNT, SPECIALTY_FOOD_COUNT, FARMERS_MARKET_COUNT,
                   ESSENTIAL_STORE_COUNT, LARGE_FORMAT_COUNT,
                   STORES_PER_SQMILE, PCT_ESSENTIAL, PCT_CONVENIENCE,
                   ROW_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_GROCERY_STORES
            WHERE GROCERY_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY GROCERY_SCORE DESC
        """, conn)

        df_hotspots = _run(f"""
            SELECT NEIGHBORHOOD_NAME, TOTAL_STORES, N_STORE_CLUSTERS,
                   CLUSTERED_STORE_SHARE_PCT, ISOLATED_STORE_PCT, ACCESS_TIER
            FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.GA_GROCERY_HOTSPOT_CLUSTERS
            WHERE 1=1 {hc}
            ORDER BY CLUSTERED_STORE_SHARE_PCT DESC
        """, conn)

        food_desert_count = int((df_scores["GROCERY_GRADE"] == "FOOD_DESERT").sum()) if not df_scores.empty else 0

        return {
            "domain": "grocery",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "summary": {
                "food_desert_count": food_desert_count,
                "grade_distribution": df_scores["GROCERY_GRADE"].value_counts().to_dict() if not df_scores.empty else {},
            },
            "scores": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "grocery_score": _f(r["GROCERY_SCORE"]),
                    "grocery_grade": r["GROCERY_GRADE"],
                    "total_stores": _i(r["TOTAL_STORES"]),
                    "supermarkets": _i(r["SUPERMARKET_COUNT"]),
                    "convenience_stores": _i(r["CONVENIENCE_STORE_COUNT"]),
                    "pharmacies": _i(r["PHARMACY_COUNT"]),
                    "specialty_food": _i(r["SPECIALTY_FOOD_COUNT"]),
                    "farmers_markets": _i(r["FARMERS_MARKET_COUNT"]),
                    "essential_stores": _i(r["ESSENTIAL_STORE_COUNT"]),
                    "large_format": _i(r["LARGE_FORMAT_COUNT"]),
                    "stores_per_sqmile": _f(r["STORES_PER_SQMILE"]),
                    "pct_essential": _f(r["PCT_ESSENTIAL"]),
                    "pct_convenience": _f(r["PCT_CONVENIENCE"]),
                    "description": _s(r, "ROW_DESCRIPTION"),
                }
                for _, r in df_scores.iterrows()
            ],
            "hotspots": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "total_stores": _i(r["TOTAL_STORES"]),
                    "store_clusters": _i(r["N_STORE_CLUSTERS"]),
                    "clustered_store_share_pct": _f(r["CLUSTERED_STORE_SHARE_PCT"]),
                    "isolated_store_pct": _f(r["ISOLATED_STORE_PCT"]),
                    "access_tier": _s(r, "ACCESS_TIER"),
                }
                for _, r in df_hotspots.iterrows()
            ],
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: HEALTHCARE
# Verified: MRT_NEIGHBORHOOD_HEALTHCARE,
#           HA_HEALTHCARE_ACCESS_PROFILE, HA_HEALTHCARE_HOTSPOT_CLUSTERS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/healthcare")
async def domain_healthcare(neighborhood: Optional[str] = Query(None)):
    """Healthcare domain deep dive: scores + access profiles + hotspot clusters."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df_scores = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, HEALTHCARE_SCORE, HEALTHCARE_GRADE,
                   TOTAL_FACILITIES, HOSPITAL_COUNT, CLINIC_COUNT,
                   TOTAL_BED_CAPACITY, FACILITIES_PER_SQMILE,
                   DENSITY_SCORE, DIVERSITY_SCORE, SERVICE_PRESENCE_SCORE,
                   CAPACITY_SCORE, ROW_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HEALTHCARE
            WHERE HEALTHCARE_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY HEALTHCARE_SCORE DESC
        """, conn)

        df_profiles = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, TOTAL_FACILITIES,
                   HOSPITAL_COUNT, CLINIC_COUNT, INPATIENT_HOSPITAL_COUNT,
                   OUTPATIENT_CLINIC_COUNT, PUBLIC_HEALTH_COUNT, SPECIALTY_OTHER_COUNT,
                   FACILITY_TYPE_DIVERSITY, FACILITIES_PER_SQMILE,
                   PCT_CORE_CARE, PCT_VALID_PHONE,
                   DENSITY_SCORE, CORE_CARE_SCORE, CONTACT_QUALITY_SCORE, DIVERSITY_SCORE
            FROM NEIGHBOURWISE_DOMAINS.HEALTHCARE_ANALYSIS.HA_HEALTHCARE_ACCESS_PROFILE
            WHERE 1=1 {HARBOR_FILTER} {hc}
            ORDER BY DENSITY_SCORE DESC
        """, conn)

        df_hotspots = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, TOTAL_FACILITIES,
                   N_HEALTHCARE_CLUSTERS, CLUSTERED_FACILITY_SHARE_PCT,
                   ISOLATED_FACILITY_PCT
            FROM NEIGHBOURWISE_DOMAINS.HEALTHCARE_ANALYSIS.HA_HEALTHCARE_HOTSPOT_CLUSTERS
            WHERE 1=1 {HARBOR_FILTER} {hc}
            ORDER BY CLUSTERED_FACILITY_SHARE_PCT DESC
        """, conn)

        return {
            "domain": "healthcare",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "summary": {
                "grade_distribution": df_scores["HEALTHCARE_GRADE"].value_counts().to_dict() if not df_scores.empty else {},
                "avg_score": round(float(df_scores["HEALTHCARE_SCORE"].mean()), 1) if not df_scores.empty else None,
            },
            "scores": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "healthcare_score": _f(r["HEALTHCARE_SCORE"]),
                    "healthcare_grade": r["HEALTHCARE_GRADE"],
                    "total_facilities": _i(r["TOTAL_FACILITIES"]),
                    "hospitals": _i(r["HOSPITAL_COUNT"]),
                    "clinics": _i(r["CLINIC_COUNT"]),
                    "total_bed_capacity": _i(r["TOTAL_BED_CAPACITY"]),
                    "facilities_per_sqmile": _f(r["FACILITIES_PER_SQMILE"]),
                    "density_score": _f(r["DENSITY_SCORE"]),
                    "diversity_score": _f(r["DIVERSITY_SCORE"]),
                    "service_presence_score": _f(r["SERVICE_PRESENCE_SCORE"]),
                    "capacity_score": _f(r["CAPACITY_SCORE"]),
                    "description": _s(r, "ROW_DESCRIPTION"),
                }
                for _, r in df_scores.iterrows()
            ],
            "access_profiles": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "total_facilities": _i(r["TOTAL_FACILITIES"]),
                    "hospitals": _i(r["HOSPITAL_COUNT"]),
                    "clinics": _i(r["CLINIC_COUNT"]),
                    "inpatient_hospitals": _i(r["INPATIENT_HOSPITAL_COUNT"]),
                    "outpatient_clinics": _i(r["OUTPATIENT_CLINIC_COUNT"]),
                    "public_health": _i(r["PUBLIC_HEALTH_COUNT"]),
                    "specialty_other": _i(r["SPECIALTY_OTHER_COUNT"]),
                    "facility_type_diversity": _i(r["FACILITY_TYPE_DIVERSITY"]),
                    "facilities_per_sqmile": _f(r["FACILITIES_PER_SQMILE"]),
                    "pct_core_care": _f(r["PCT_CORE_CARE"]),
                    "pct_valid_phone": _f(r["PCT_VALID_PHONE"]),
                    "density_score": _f(r["DENSITY_SCORE"]),
                    "core_care_score": _f(r["CORE_CARE_SCORE"]),
                    "contact_quality_score": _f(r["CONTACT_QUALITY_SCORE"]),
                    "diversity_score": _f(r["DIVERSITY_SCORE"]),
                }
                for _, r in df_profiles.iterrows()
            ],
            "hotspots": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "total_facilities": _i(r["TOTAL_FACILITIES"]),
                    "healthcare_clusters": _i(r["N_HEALTHCARE_CLUSTERS"]),
                    "clustered_facility_share_pct": _f(r["CLUSTERED_FACILITY_SHARE_PCT"]),
                    "isolated_facility_pct": _f(r["ISOLATED_FACILITY_PCT"]),
                }
                for _, r in df_hotspots.iterrows()
            ],
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: SCHOOLS
# Verified: MRT_NEIGHBORHOOD_SCHOOLS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/schools")
async def domain_schools(neighborhood: Optional[str] = Query(None)):
    """Schools domain deep dive: level coverage + type mix."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, SCHOOL_SCORE, SCHOOL_GRADE,
                   TOTAL_SCHOOLS, PUBLIC_SCHOOL_COUNT, PRIVATE_SCHOOL_COUNT,
                   CHARTER_SCHOOL_COUNT, SPECIAL_ED_COUNT, VOCATIONAL_COUNT,
                   ELEMENTARY_COUNT, MIDDLE_COUNT, HIGH_COUNT, K12_COUNT,
                   HAS_ELEMENTARY, HAS_MIDDLE, HAS_HIGH, HAS_PUBLIC, HAS_CHARTER,
                   SCHOOLS_PER_SQMILE, PCT_PUBLIC, PCT_PRIVATE, PCT_CHARTER,
                   SCHOOL_TYPE_DIVERSITY, LEVEL_COVERAGE_SCORE, ROW_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SCHOOLS
            WHERE SCHOOL_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY SCHOOL_SCORE DESC
        """, conn)

        return {
            "domain": "schools",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "summary": {
                "grade_distribution": df["SCHOOL_GRADE"].value_counts().to_dict() if not df.empty else {},
                "total_schools_citywide": _i(df["TOTAL_SCHOOLS"].sum()) if not df.empty else None,
            },
            "neighborhoods": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "school_score": _f(r["SCHOOL_SCORE"]),
                    "school_grade": r["SCHOOL_GRADE"],
                    "total_schools": _i(r["TOTAL_SCHOOLS"]),
                    "public": _i(r["PUBLIC_SCHOOL_COUNT"]),
                    "private": _i(r["PRIVATE_SCHOOL_COUNT"]),
                    "charter": _i(r["CHARTER_SCHOOL_COUNT"]),
                    "special_ed": _i(r["SPECIAL_ED_COUNT"]),
                    "vocational": _i(r["VOCATIONAL_COUNT"]),
                    "elementary": _i(r["ELEMENTARY_COUNT"]),
                    "middle": _i(r["MIDDLE_COUNT"]),
                    "high_school": _i(r["HIGH_COUNT"]),
                    "k12": _i(r["K12_COUNT"]),
                    "has_elementary": bool(r["HAS_ELEMENTARY"]) if pd.notna(r["HAS_ELEMENTARY"]) else False,
                    "has_middle": bool(r["HAS_MIDDLE"]) if pd.notna(r["HAS_MIDDLE"]) else False,
                    "has_high": bool(r["HAS_HIGH"]) if pd.notna(r["HAS_HIGH"]) else False,
                    "schools_per_sqmile": _f(r["SCHOOLS_PER_SQMILE"]),
                    "pct_public": _f(r["PCT_PUBLIC"]),
                    "pct_private": _f(r["PCT_PRIVATE"]),
                    "pct_charter": _f(r["PCT_CHARTER"]),
                    "school_type_diversity": _i(r["SCHOOL_TYPE_DIVERSITY"]),
                    "level_coverage_score": _f(r["LEVEL_COVERAGE_SCORE"]),
                    "description": _s(r, "ROW_DESCRIPTION"),
                }
                for _, r in df.iterrows()
            ]
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: RESTAURANTS
# Verified: MRT_NEIGHBORHOOD_RESTAURANTS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/restaurants")
async def domain_restaurants(neighborhood: Optional[str] = Query(None)):
    """Restaurants domain deep dive: density + cuisine diversity + quality."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, RESTAURANT_SCORE, RESTAURANT_GRADE,
                   TOTAL_RESTAURANTS, RESTAURANTS_PER_SQMILE,
                   AVG_RATING, TOTAL_REVIEWS, PCT_HIGH_QUALITY,
                   BUDGET_COUNT, MID_RANGE_COUNT, UPSCALE_COUNT,
                   EXCELLENT_COUNT, GOOD_COUNT, AVERAGE_COUNT, POOR_COUNT,
                   CUISINE_DIVERSITY, ETHNIC_COUNT, PIZZA_COUNT, CAFE_BAKERY_COUNT,
                   FAST_FOOD_COUNT, HEALTHY_COUNT, AMERICAN_COUNT,
                   DELIVERY_COUNT, PICKUP_COUNT, PCT_DELIVERY,
                   RESTAURANT_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_RESTAURANTS
            WHERE RESTAURANT_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY RESTAURANT_SCORE DESC
        """, conn)

        return {
            "domain": "restaurants",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "summary": {
                "grade_distribution": df["RESTAURANT_GRADE"].value_counts().to_dict() if not df.empty else {},
                "total_restaurants_citywide": _i(df["TOTAL_RESTAURANTS"].sum()) if not df.empty else None,
                "avg_rating_citywide": round(float(df["AVG_RATING"].mean()), 2) if not df.empty else None,
            },
            "neighborhoods": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "restaurant_score": _f(r["RESTAURANT_SCORE"]),
                    "restaurant_grade": r["RESTAURANT_GRADE"],
                    "total_restaurants": _i(r["TOTAL_RESTAURANTS"]),
                    "restaurants_per_sqmile": _f(r["RESTAURANTS_PER_SQMILE"]),
                    "avg_rating": _f(r["AVG_RATING"]),
                    "total_reviews": _i(r["TOTAL_REVIEWS"]),
                    "pct_high_quality": _f(r["PCT_HIGH_QUALITY"]),
                    "budget": _i(r["BUDGET_COUNT"]),
                    "mid_range": _i(r["MID_RANGE_COUNT"]),
                    "upscale": _i(r["UPSCALE_COUNT"]),
                    "cuisine_diversity": _i(r["CUISINE_DIVERSITY"]),
                    "ethnic_count": _i(r["ETHNIC_COUNT"]),
                    "fast_food": _i(r["FAST_FOOD_COUNT"]),
                    "cafes_bakeries": _i(r["CAFE_BAKERY_COUNT"]),
                    "healthy": _i(r["HEALTHY_COUNT"]),
                    "delivery_count": _i(r["DELIVERY_COUNT"]),
                    "pct_delivery": _f(r["PCT_DELIVERY"]),
                    "description": _s(r, "RESTAURANT_DESCRIPTION"),
                }
                for _, r in df.iterrows()
            ]
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: UNIVERSITIES
# Verified: MRT_NEIGHBORHOOD_UNIVERSITIES
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/universities")
async def domain_universities(neighborhood: Optional[str] = Query(None)):
    """Universities domain deep dive: presence + research + student housing."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, EDUCATION_SCORE, EDUCATION_GRADE,
                   TOTAL_UNIVERSITIES, UNIVERSITIES_PER_SQMILE,
                   PUBLIC_COUNT, PRIVATE_COUNT,
                   FOUR_YEAR_PUBLIC_COUNT, FOUR_YEAR_PRIVATE_COUNT,
                   HIGHER_EDUCATION_COUNT, DOCTORATE_COUNT,
                   WITH_HOUSING_COUNT, HAS_UNIVERSITIES,
                   HAS_RESEARCH_INSTITUTIONS, HAS_STUDENT_HOUSING,
                   UNIVERSITY_NAMES, EDUCATION_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_UNIVERSITIES
            WHERE EDUCATION_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY EDUCATION_SCORE DESC
        """, conn)

        df_with = df[df["TOTAL_UNIVERSITIES"].fillna(0) > 0] if not df.empty else df

        return {
            "domain": "universities",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "summary": {
                "neighborhoods_with_universities": len(df_with),
                "grade_distribution": df["EDUCATION_GRADE"].value_counts().to_dict() if not df.empty else {},
            },
            "neighborhoods": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "education_score": _f(r["EDUCATION_SCORE"]),
                    "education_grade": r["EDUCATION_GRADE"],
                    "total_universities": _i(r["TOTAL_UNIVERSITIES"]),
                    "universities_per_sqmile": _f(r["UNIVERSITIES_PER_SQMILE"]),
                    "public": _i(r["PUBLIC_COUNT"]),
                    "private": _i(r["PRIVATE_COUNT"]),
                    "four_year_public": _i(r["FOUR_YEAR_PUBLIC_COUNT"]),
                    "four_year_private": _i(r["FOUR_YEAR_PRIVATE_COUNT"]),
                    "higher_education_count": _i(r["HIGHER_EDUCATION_COUNT"]),
                    "doctorate_programs": _i(r["DOCTORATE_COUNT"]),
                    "with_student_housing": _i(r["WITH_HOUSING_COUNT"]),
                    "has_universities": bool(r["HAS_UNIVERSITIES"]) if pd.notna(r["HAS_UNIVERSITIES"]) else False,
                    "has_research_institutions": bool(r["HAS_RESEARCH_INSTITUTIONS"]) if pd.notna(r["HAS_RESEARCH_INSTITUTIONS"]) else False,
                    "has_student_housing": bool(r["HAS_STUDENT_HOUSING"]) if pd.notna(r["HAS_STUDENT_HOUSING"]) else False,
                    "university_names": _s(r, "UNIVERSITY_NAMES"),
                    "description": _s(r, "EDUCATION_DESCRIPTION"),
                }
                for _, r in df.iterrows()
            ]
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN: BLUEBIKES
# Verified: MRT_NEIGHBORHOOD_BLUEBIKES
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/domain/bluebikes")
async def domain_bluebikes(neighborhood: Optional[str] = Query(None)):
    """Bluebikes domain deep dive: stations + docks + density."""
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, BIKESHARE_SCORE, BIKESHARE_GRADE,
                   TOTAL_STATIONS, LARGE_STATIONS, MEDIUM_STATIONS, SMALL_STATIONS,
                   TOTAL_DOCKS, AVG_DOCKS_PER_STATION, STATIONS_PER_SQMILE,
                   BIKESHARE_DESCRIPTION
            FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_BLUEBIKES
            WHERE BIKESHARE_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY BIKESHARE_SCORE DESC
        """, conn)

        return {
            "domain": "bluebikes",
            "filter": {"neighborhood": neighborhood or "ALL"},
            "summary": {
                "total_stations_citywide": _i(df["TOTAL_STATIONS"].sum()) if not df.empty else None,
                "total_docks_citywide": _i(df["TOTAL_DOCKS"].sum()) if not df.empty else None,
                "grade_distribution": df["BIKESHARE_GRADE"].value_counts().to_dict() if not df.empty else {},
            },
            "neighborhoods": [
                {
                    "neighborhood": _title(r["NEIGHBORHOOD_NAME"]),
                    "city": _title(r["CITY"]),
                    "bikeshare_score": _f(r["BIKESHARE_SCORE"]),
                    "bikeshare_grade": r["BIKESHARE_GRADE"],
                    "total_stations": _i(r["TOTAL_STATIONS"]),
                    "large_stations": _i(r["LARGE_STATIONS"]),
                    "medium_stations": _i(r["MEDIUM_STATIONS"]),
                    "small_stations": _i(r["SMALL_STATIONS"]),
                    "total_docks": _i(r["TOTAL_DOCKS"]),
                    "avg_docks_per_station": _f(r["AVG_DOCKS_PER_STATION"]),
                    "stations_per_sqmile": _f(r["STATIONS_PER_SQMILE"]),
                    "description": _s(r, "BIKESHARE_DESCRIPTION"),
                }
                for _, r in df.iterrows()
            ]
        }
    finally:
        conn.close()
    

@router.get("/domain-matrix")
async def get_domain_matrix(
    neighborhood: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=51)
):
    """
    All domain scores per neighborhood in one query.
    Used for the heatmap visualization on the home page.
    Returns: neighborhood, city, master_score + all 9 domain scores.
    """
    conn = _get_conn()
    hc = _hood(neighborhood)
    try:
        df = _run(f"""
            SELECT NEIGHBORHOOD_NAME, CITY, MASTER_SCORE, MASTER_GRADE,
                   TOP_STRENGTH, TOP_WEAKNESS,
                   SAFETY_SCORE, HOUSING_SCORE, TRANSIT_SCORE,
                   GROCERY_SCORE, HEALTHCARE_SCORE, SCHOOL_SCORE,
                   RESTAURANT_SCORE, EDUCATION_SCORE, BIKESHARE_SCORE
            FROM NEIGHBOURWISE_DOMAINS.ANALYTICS.NEIGHBORHOOD_MASTER_SCORE
            WHERE MASTER_SCORE IS NOT NULL
              {HARBOR_FILTER} {hc}
            ORDER BY MASTER_SCORE DESC
            LIMIT {limit}
        """, conn)

        return {
            "count": len(df),
            "neighborhoods": [
                {
                    "neighborhood":     _title(r["NEIGHBORHOOD_NAME"]),
                    "city":             _title(r["CITY"]),
                    "master_score":     _f(r["MASTER_SCORE"]),
                    "master_grade":     r["MASTER_GRADE"],
                    "top_strength":     _s(r, "TOP_STRENGTH"),
                    "top_weakness":     _s(r, "TOP_WEAKNESS"),
                    "Safety":           _f(r["SAFETY_SCORE"]),
                    "Housing":          _f(r["HOUSING_SCORE"]),
                    "Transit":          _f(r["TRANSIT_SCORE"]),
                    "Grocery":          _f(r["GROCERY_SCORE"]),
                    "Healthcare":       _f(r["HEALTHCARE_SCORE"]),
                    "Schools":          _f(r["SCHOOL_SCORE"]),
                    "Restaurants":      _f(r["RESTAURANT_SCORE"]),
                    "Universities":     _f(r["EDUCATION_SCORE"]),
                    "Bluebikes":        _f(r["BIKESHARE_SCORE"]),
                }
                for _, r in df.iterrows()
            ]
        }
    finally:
        conn.close()