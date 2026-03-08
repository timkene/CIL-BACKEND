"""
Provider Fraud Scores API
==========================
Serves monthly fraud scan results stored in Supabase.
"""

import os
from fastapi import APIRouter, Query
from supabase import create_client

router = APIRouter()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
TABLE = "provider_fraud_scores"


def _sb():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


@router.get("/latest-month")
def get_latest_month():
    """Return the most recent scan_month available."""
    sb = _sb()
    res = (
        sb.table(TABLE)
        .select("scan_month")
        .order("scan_month", desc=True)
        .limit(1)
        .execute()
    )
    data = res.data
    return {"scan_month": data[0]["scan_month"] if data else None}


@router.get("/")
def get_fraud_scores(
    scan_month: str = Query(None, description="YYYY-MM — defaults to latest"),
    status:     str = Query(None, description="Filter: ALERT | WATCHLIST | CLEAR"),
    search:     str = Query(None, description="Provider name substring"),
):
    """
    Return provider fraud scores for a given month.
    Defaults to the most recent scan month.
    """
    sb = _sb()

    # Resolve scan_month
    if not scan_month:
        res = (
            sb.table(TABLE)
            .select("scan_month")
            .order("scan_month", desc=True)
            .limit(1)
            .execute()
        )
        if not res.data:
            return {"scan_month": None, "providers": [], "summary": {}}
        scan_month = res.data[0]["scan_month"]

    query = sb.table(TABLE).select("*").eq("scan_month", scan_month)

    if status:
        query = query.eq("alert_status", status.upper())

    res = query.order("total_score", desc=True).execute()
    providers = res.data or []

    # Client-side name search
    if search:
        s = search.lower()
        providers = [p for p in providers if s in (p.get("provider_name") or "").lower()]

    alert_count     = sum(1 for p in providers if p.get("alert_status") == "ALERT")
    watchlist_count = sum(1 for p in providers if p.get("alert_status") == "WATCHLIST")
    clear_count     = sum(1 for p in providers if p.get("alert_status") == "CLEAR")

    return {
        "scan_month": scan_month,
        "providers":  providers,
        "summary": {
            "total":     len(providers),
            "alert":     alert_count,
            "watchlist": watchlist_count,
            "clear":     clear_count,
        },
    }
