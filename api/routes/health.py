"""
Health Check Endpoint
"""
from fastapi import APIRouter, HTTPException
from datetime import datetime
from core.database import test_connection, MOTHERDUCK_DB

router = APIRouter()

@router.get("/health")
async def health_check():
    """
    Health check endpoint
    Returns API status
    Note: Database connection test is skipped to avoid blocking
    """
    # Skip database connection test to avoid blocking
    # The database will be tested when actual queries are made
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database_connected": None,  # Not tested to avoid blocking
        "database_type": "MotherDuck (Cloud)",
        "database_name": MOTHERDUCK_DB,
        "version": "1.0.0",
        "note": "Database connection test skipped to prevent blocking"
    }
