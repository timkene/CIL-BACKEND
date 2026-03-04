"""
Enrollee Module API Routes
"""

from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, date
from typing import Optional
import logging
import polars as pl
import pandas as pd

from core.database import get_db_connection
from services.enrollee_service import EnrolleeAnalyticsService

logger = logging.getLogger(__name__)
router = APIRouter()

# Simple in-process cache for enrollee base data to avoid reloading huge tables on every request
_ENROLLEE_DATA_CACHE = None
_ENROLLEE_DATA_CACHE_TS: Optional[datetime] = None
_ENROLLEE_DATA_TTL_SECONDS = 600  # 10 minutes (increased from 5 for better performance)


def load_enrollee_data(force_refresh: bool = False):
    """Load data needed for enrollee analytics, with a small in-memory cache."""
    global _ENROLLEE_DATA_CACHE, _ENROLLEE_DATA_CACHE_TS

    # Return cached data if still fresh and not forced to refresh
    if (
        not force_refresh
        and _ENROLLEE_DATA_CACHE is not None
        and _ENROLLEE_DATA_CACHE_TS is not None
    ):
        age = (datetime.now() - _ENROLLEE_DATA_CACHE_TS).total_seconds()
        if age < _ENROLLEE_DATA_TTL_SECONDS:
            return _ENROLLEE_DATA_CACHE

    try:
        conn = get_db_connection()

        CLAIMS = conn.execute(
            """
            SELECT enrollee_id as nhislegacynumber,
                   nhisgroupid, panumber,
                   encounterdatefrom, datesubmitted, approvedamount
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE encounterdatefrom >= DATE '2024-01-01'
            """
        ).fetchdf()

        PA = conn.execute(
            """
            SELECT panumber, groupname, IID,
                   requestdate, granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE requestdate >= TIMESTAMP '2024-01-01'
            """
        ).fetchdf()

        MEMBERS = conn.execute(
            """
            SELECT
                m.memberid,
                m.enrollee_id,
                m.groupid,
                COALESCE(mem.firstname, '') as firstname,
                COALESCE(mem.lastname, '') as surname,
                COALESCE(mem.dob, m.dob) as dateofbirth,
                COALESCE(mem.phone1, m.phone1) as phone,
                COALESCE(mem.email1, m.email1) as email,
                COALESCE(mem.address1, m.address1) as contactaddress,
                m.effectivedate,
                m.terminationdate,
                m.iscurrent,
                COALESCE(mem.genderid, m.genderid) as genderid
            FROM "AI DRIVEN DATA"."MEMBERS" m
            LEFT JOIN "AI DRIVEN DATA"."MEMBER" mem 
                ON CAST(m.memberid AS BIGINT) = mem.memberid
            """
        ).fetchdf()

        GROUPS = conn.execute(
            'SELECT groupid, groupname FROM "AI DRIVEN DATA"."GROUPS"'
        ).fetchdf()

        data = {
            'CLAIMS': pl.from_pandas(CLAIMS),
            'PA': pl.from_pandas(PA),
            'MEMBERS': pl.from_pandas(MEMBERS),
            'GROUPS': pl.from_pandas(GROUPS)
        }

        # Update cache
        _ENROLLEE_DATA_CACHE = data
        _ENROLLEE_DATA_CACHE_TS = datetime.now()

        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {str(e)}")


def calculate_enrollee_dashboard_basic(
    groupname: Optional[str],
    month: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    limit: int,
):
    """
    Core enrollee dashboard computation, analogous to calculate_mlr_basic /
    calculate_client_summary_basic.

    This function is the single place to change when we later move to a nightly
    batch job that writes results into an ENROLLEE_DASHBOARD_SUMMARY table.
    """
    data = load_enrollee_data()

    # Month / reference date
    if month and isinstance(month, str):
        reference_date = datetime.strptime(month, "%Y-%m")
    else:
        reference_date = datetime.now()

    # Parse date filters
    start_dt = None
    end_dt = None
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            start_dt = None
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            end_dt = None

    # Heavy computations delegated to EnrolleeAnalyticsService
    top_by_cost = EnrolleeAnalyticsService.get_top_enrollees_by_cost(
        CLAIMS=data["CLAIMS"],
        PA=data["PA"],
        MEMBERS=data["MEMBERS"],
        GROUPS=data["GROUPS"],
        limit=limit,
        groupname=groupname,
        start_date=start_dt,
        end_date=end_dt,
    )

    top_by_visits = EnrolleeAnalyticsService.get_top_enrollees_by_visits(
        CLAIMS=data["CLAIMS"],
        MEMBERS=data["MEMBERS"],
        GROUPS=data["GROUPS"],
        limit=limit,
        groupname=groupname,
        start_date=start_dt,
        end_date=end_dt,
    )

    enrollment_stats = EnrolleeAnalyticsService.get_enrollment_statistics(
        MEMBERS=data["MEMBERS"], reference_month=reference_date
    )

    data_quality = EnrolleeAnalyticsService.get_data_quality_metrics(
        MEMBERS=data["MEMBERS"]
    )

    gender_count = EnrolleeAnalyticsService.get_gender_count(
        MEMBERS=data["MEMBERS"]
    )

    dashboard = {
        "top_by_cost": top_by_cost.get("top_enrollees", [])[:limit],
        "top_by_visits": top_by_visits.get("top_enrollees", [])[:limit],
        "enrollment_statistics": enrollment_stats.get("statistics", {}),
        "data_quality": data_quality,
        "gender_count": gender_count.get("gender_count", {}),
    }

    # Expose reference month string for filters metadata
    reference_month_str = reference_date.strftime("%Y-%m")

    return {
        "dashboard": dashboard,
        "reference_month": reference_month_str,
    }


@router.get("/dashboard")
async def get_enrollee_dashboard(
    groupname: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=100)
):
    """Get complete enrollee dashboard"""
    try:
        result = calculate_enrollee_dashboard_basic(
            groupname=groupname,
            month=month,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        return {
            "success": True,
            "dashboard": result["dashboard"],
            "filters": {
                "groupname": groupname,
                "month": month or result["reference_month"],
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            },
            "timestamp": datetime.now().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profile/{enrollee_id:path}")
async def get_enrollee_profile(
    enrollee_id: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """Get comprehensive profile for a specific enrollee"""
    try:
        start_dt = None
        end_dt = None
        
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                pass
        
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                pass
        
        conn = get_db_connection()
        result = EnrolleeAnalyticsService.get_enrollee_profile(
            enrollee_id=enrollee_id,
            start_date=start_dt,
            end_date=end_dt,
            conn=conn
        )
        return result
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting enrollee profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-by-cost")
async def get_top_by_cost(
    limit: int = Query(50, ge=1, le=100),
    groupname: Optional[str] = None
):
    """Get top enrollees by cost"""
    try:
        data = load_enrollee_data()
        result = EnrolleeAnalyticsService.get_top_enrollees_by_cost(
            CLAIMS=data['CLAIMS'], PA=data['PA'],
            MEMBERS=data['MEMBERS'], GROUPS=data['GROUPS'],
            limit=limit, groupname=groupname
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-by-visits")
async def get_top_by_visits(
    limit: int = Query(50, ge=1, le=100),
    groupname: Optional[str] = None
):
    """Get top enrollees by visits"""
    try:
        data = load_enrollee_data()
        result = EnrolleeAnalyticsService.get_top_enrollees_by_visits(
            CLAIMS=data['CLAIMS'], MEMBERS=data['MEMBERS'],
            GROUPS=data['GROUPS'], limit=limit, groupname=groupname
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enrollment-stats")
async def get_enrollment_stats(month: Optional[str] = None):
    """Get enrollment statistics"""
    try:
        data = load_enrollee_data()
        reference_date = datetime.strptime(month, '%Y-%m') if month else datetime.now()
        result = EnrolleeAnalyticsService.get_enrollment_statistics(
            MEMBERS=data['MEMBERS'], reference_month=reference_date
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data-quality")
async def get_data_quality():
    """Get data quality metrics"""
    try:
        data = load_enrollee_data()
        result = EnrolleeAnalyticsService.get_data_quality_metrics(
            MEMBERS=data['MEMBERS']
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-by-cost-contract-period")
async def get_top_by_cost_contract_period(
    limit: int = Query(20, ge=1, le=100)
):
    """Get top enrollees by cost filtered by their client's contract period"""
    try:
        conn = get_db_connection()
        try:
            result = EnrolleeAnalyticsService.get_top_enrollees_by_cost_contract_period(
                limit=limit,
                conn=conn
            )
            return result
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error getting top enrollees by cost (contract period): {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-by-visits-contract-period")
async def get_top_by_visits_contract_period(
    limit: int = Query(20, ge=1, le=100)
):
    """Get top enrollees by visits filtered by their client's contract period"""
    try:
        conn = get_db_connection()
        try:
            result = EnrolleeAnalyticsService.get_top_enrollees_by_visits_contract_period(
                limit=limit,
                conn=conn
            )
            return result
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error getting top enrollees by visits (contract period): {e}")
        raise HTTPException(status_code=500, detail=str(e))
