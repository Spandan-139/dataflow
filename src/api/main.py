from fastapi import FastAPI, HTTPException, Query
from loguru import logger
from src.warehouse.db import (
    get_top_repos, get_event_distribution,
    get_hourly_activity, get_top_contributors,
    get_summary_stats, build_warehouse
)
from pathlib import Path

app = FastAPI(
    title="DataFlow Analytics API",
    description="GitHub Archive ELT pipeline analytics — Bronze/Silver/Gold medallion architecture",
    version="1.0.0"
)


@app.on_event("startup")
def startup():
    if Path("data/warehouse.db").exists():
        logger.info("Warehouse found — API ready")
    else:
        logger.warning("Warehouse not found — run the ETL pipeline first")


@app.get("/")
def root():
    return {
        "name": "DataFlow Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": ["/summary", "/repos", "/events", "/activity", "/contributors"]
    }


@app.get("/health")
def health():
    warehouse_exists = Path("data/warehouse.db").exists()
    return {
        "status": "ok" if warehouse_exists else "degraded",
        "warehouse": "ready" if warehouse_exists else "missing"
    }


@app.get("/summary")
def summary():
    try:
        return get_summary_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/repos")
def top_repos(limit: int = Query(default=10, ge=1, le=100)):
    try:
        return {"repos": get_top_repos(limit), "count": limit}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events")
def event_distribution():
    try:
        return {"events": get_event_distribution()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/activity")
def hourly_activity():
    try:
        return {"activity": get_hourly_activity()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/contributors")
def top_contributors(limit: int = Query(default=10, ge=1, le=100)):
    try:
        return {"contributors": get_top_contributors(limit), "count": limit}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/warehouse/rebuild")
def rebuild_warehouse():
    try:
        build_warehouse()
        return {"status": "ok", "message": "Warehouse rebuilt successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))