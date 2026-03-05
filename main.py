"""
MLR Analysis API - FastAPI Backend
===================================

Modern REST API for Medical Loss Ratio analysis and healthcare data insights.

Author: Casey's Healthcare Analytics Team
Version: 1.0.0
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # Load .env before any module that needs MOTHERDUCK_TOKEN

# Use same DuckDB and shared connection for banding (set before banding is imported)
from core import database as _core_db
os.environ.setdefault("DUCKDB_PATH", str(Path(_core_db.DB_PATH).resolve()))
os.environ["USE_SHARED_DB_CONNECTION"] = "1"

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from contextlib import asynccontextmanager
import uvicorn
import asyncio
from datetime import datetime, date

# Import routers (tariff pulls in Streamlit — skip on Render to avoid OOM)
from api.routes import mlr, utilization, health, enrollees, clients, finance, paclaims, admin, providers, banding
# Skip tariff on Render: RENDER is set by Render; avoid importing Streamlit-heavy modules (saves 512MB+)
_RENDER_DEPLOY = (
    os.getenv("RENDER", "").lower() in ("true", "1", "yes")
    or bool(os.getenv("RENDER_SERVICE_NAME"))
    or bool(os.getenv("RENDER_EXTERNAL_URL"))
)
if not _RENDER_DEPLOY:
    from api.routes import tariff
else:
    tariff = None  # type: ignore
from core.database import get_db_connection, close_all_connections, get_database_info, USE_LOCAL_DB

# Background task for cache warming
_cache_warming_task = None

async def warm_caches_background():
    """Background task to keep caches warm - runs every 8 minutes"""
    while True:
        try:
            await asyncio.sleep(480)  # Wait 8 minutes (before 10-min TTL expires)
            print("🔄 Background cache warming starting...")

            # Warm MLR cache
            try:
                mlr.load_mlr_data(force_refresh=True)
                print("  ✅ MLR cache warmed")
            except Exception as e:
                print(f"  ⚠️ MLR cache warming failed: {e}")

            # Warm Enrollee cache
            try:
                enrollees.load_enrollee_data(force_refresh=True)
                print("  ✅ Enrollee cache warmed")
            except Exception as e:
                print(f"  ⚠️ Enrollee cache warming failed: {e}")

            print("✅ Background cache warming completed")

        except asyncio.CancelledError:
            print("🛑 Cache warming task cancelled")
            break
        except Exception as e:
            print(f"⚠️ Cache warming error: {e}")
            await asyncio.sleep(60)  # Wait 1 minute on error before retrying


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for startup and shutdown events"""
    global _cache_warming_task

    # STARTUP
    print("🚀 Starting MLR Analysis API...")

    # Show database configuration
    db_info = get_database_info()
    print(f"📊 Database: {db_info['type']}")
    print(f"   Path: {db_info['path']}")

    # 1. Warm up database connection
    try:
        db_type = "Local DuckDB" if USE_LOCAL_DB else "MotherDuck"
        print(f"🔌 Warming up {db_type} connection...")
        conn = get_db_connection()
        conn.execute("SELECT 1")
        print(f"✅ {db_type} connection ready")
    except Exception as e:
        print(f"⚠️ Database connection warmup failed: {e}")

    # 2. Pre-load caches (skip on Render to stay under 512MB; data loads on first request)
    if not _RENDER_DEPLOY:
        try:
            print("📦 Pre-loading data caches...")
            mlr.load_mlr_data()
            print("  ✅ MLR data cached")
        except Exception as e:
            print(f"  ⚠️ MLR cache pre-load failed: {e}")
        try:
            enrollees.load_enrollee_data()
            print("  ✅ Enrollee data cached")
        except Exception as e:
            print(f"  ⚠️ Enrollee cache pre-load failed: {e}")
        _cache_warming_task = asyncio.create_task(warm_caches_background())
        print("🔄 Background cache warming task started")
    else:
        print("📦 Render: skipping pre-load and cache warming (data loads on first request)")

    print("✅ API startup complete!")

    yield  # Application runs here

    # SHUTDOWN
    print("🛑 Shutting down MLR Analysis API...")

    # Cancel background task
    if _cache_warming_task:
        _cache_warming_task.cancel()
        try:
            await _cache_warming_task
        except asyncio.CancelledError:
            pass

    # Close all database connections
    close_all_connections()
    print("✅ Shutdown complete")


# Create FastAPI app with lifespan manager
app = FastAPI(
    title="MLR Analysis API",
    description="Healthcare analytics API for Medical Loss Ratio and utilization analysis",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS configuration - allows React frontend to communicate
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # React development server
        "http://localhost:5173",  # Vite development server
        "http://localhost:5174",  # Eko React app (Vite)
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods
    allow_headers=["*"],  # Allows all headers
)

# Include routers
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(mlr.router, prefix="/api/v1/mlr", tags=["MLR Analysis"])
app.include_router(utilization.router, prefix="/api/v1/utilization", tags=["Utilization"])
app.include_router(enrollees.router, prefix="/api/v1/enrollees", tags=["Enrollee Management"])
app.include_router(clients.router, prefix="/api/v1/clients", tags=["Client Analysis"])
app.include_router(finance.router, prefix="/api/v1/finance", tags=["Finance"])
app.include_router(paclaims.router, prefix="/api/v1/pa-claims", tags=["PA & Claims"])
app.include_router(admin.router, prefix="/api/v1/admin", tags=["Admin"])
app.include_router(providers.router, prefix="/api/v1/providers", tags=["Providers"])
if tariff is not None:
    app.include_router(tariff.router, prefix="/api/v1/tariff", tags=["Tariff"])
app.include_router(banding.router, prefix="/api/v1/banding", tags=["Hospital Band Analysis"])

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint - API information"""
    return {
        "name": "MLR Analysis API",
        "version": "1.0.0",
        "status": "operational",
        "timestamp": datetime.now().isoformat(),
        "docs": "/docs",
        "health": "/api/v1/health"
    }


@app.head("/")
async def root_head():
    """HEAD / for Render (and other) health checks — returns 200 with no body."""
    return Response(status_code=200)

# Global error handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    # Run with: python main.py
    # Or use: uvicorn main:app --reload
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload on code changes
        log_level="info"
    )
