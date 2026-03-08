"""
Client Analysis API Routes
==========================

Provides client-level analytics such as:
- Top clients by total medical cost
- Top clients by number of active members
"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from datetime import datetime
from typing import Optional
import asyncio
import logging
import polars as pl
import pandas as pd
import io
import httpx

from services.enrollee_service import EnrolleeAnalyticsService
from api.routes.enrollees import load_enrollee_data
from core.database import get_db_connection

logger = logging.getLogger(__name__)
router = APIRouter()

# Summary table for client dashboard (populated by nightly batch; API reads from here when available)
CLIENT_DASHBOARD_SUMMARY_SCHEMA = "AI DRIVEN DATA"
CLIENT_DASHBOARD_SUMMARY_TABLE = "CLIENT_DASHBOARD_SUMMARY"

def get_user_allocated_clients(user_id: Optional[int] = None) -> Optional[list]:
    """
    Get list of clients allocated to a user.
    Returns None if user is MGT/ADMIN/IT (can view all clients).
    Returns empty list if user has no allocations (should not see any clients).
    Returns list of groupnames if user has specific allocations.
    """
    if not user_id:
        return None
    
    try:
        conn = get_db_connection()
        
        # Get user's department
        user_result = conn.execute('''
            SELECT department
            FROM "AI DRIVEN DATA"."USERS"
            WHERE user_id = ?
        ''', [user_id]).fetchone()
        
        if not user_result:
            return None
        
        department = user_result[0]
        
        # MGT, ADMIN, IT can view all clients
        if department and department.upper() in ['MGT', 'ADMIN', 'IT']:
            return None  # None means no filtering (all clients)
        
        # Get user's allocated clients
        allocations_result = conn.execute('''
            SELECT groupname
            FROM "AI DRIVEN DATA"."STAFF_CLIENT_ALLOCATIONS"
            WHERE user_id = ?
        ''', [user_id]).fetchdf()
        
        
        if allocations_result.empty:
            # No allocations means user should not see any clients
            return []
        
        return allocations_result['groupname'].tolist()
    except Exception as e:
        logger.error(f"Error getting user allocated clients: {e}")
        import traceback
        traceback.print_exc()
        # On error, deny access (fail closed for security)
        return []


def load_client_dashboard_from_summary(conn=None):
    """
    Read client dashboard data from the precomputed summary table, if it exists and has data.
    Returns a Polars DataFrame or None if the table is missing/empty.
    """
    if conn is None:
        conn = get_db_connection()
    try:
        q = f'''
        SELECT * FROM "{CLIENT_DASHBOARD_SUMMARY_SCHEMA}"."{CLIENT_DASHBOARD_SUMMARY_TABLE}"
        '''
        df = conn.execute(q).fetchdf()
        if df is None or df.empty:
            return None
        return pl.from_pandas(df)
    except Exception as e:
        logger.debug("Client dashboard summary table not available: %s", e)
        return None


def populate_client_dashboard_summary_table(conn=None):
    """
    Compute client summary and write it to CLIENT_DASHBOARD_SUMMARY (for nightly batch).
    Uses get_db_connection() for compute; writes to the same connection.
    Returns (success: bool, rows: int, error: Optional[str]).
    """
    if conn is None:
        conn = get_db_connection()
    try:
        summary_df = calculate_client_summary_basic()
        if summary_df is None or summary_df.height == 0:
            # Still create/truncate table so API sees "fresh" empty state
            summary_pd = pd.DataFrame(columns=[
                "groupname", "claims_cost", "unclaimed_pa_cost", "total_cost",
                "visit_count", "unique_enrollees", "active_members"
            ])
        else:
            summary_pd = summary_df.to_pandas()
        summary_pd["refreshed_at"] = datetime.now()
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{CLIENT_DASHBOARD_SUMMARY_SCHEMA}"')
        conn.execute(f'DROP TABLE IF EXISTS "{CLIENT_DASHBOARD_SUMMARY_SCHEMA}"."{CLIENT_DASHBOARD_SUMMARY_TABLE}"')
        conn.register("_client_summary_df", summary_pd)
        conn.execute(
            f'CREATE TABLE "{CLIENT_DASHBOARD_SUMMARY_SCHEMA}"."{CLIENT_DASHBOARD_SUMMARY_TABLE}" '
            'AS SELECT * FROM _client_summary_df'
        )
        conn.unregister("_client_summary_df")
        n = len(summary_pd)
        logger.info("CLIENT_DASHBOARD_SUMMARY populated with %s rows", n)
        return (True, n, None)
    except Exception as e:
        logger.exception("Failed to populate CLIENT_DASHBOARD_SUMMARY: %s", e)
        return (False, 0, str(e))


def calculate_client_summary_basic() -> pl.DataFrame:
    """
    Core client summary computation, similar to calculate_mlr_basic.

    Returns a Polars DataFrame with one row per client (groupname) including:
    - claims_cost, unclaimed_pa_cost, total_cost, visit_count, unique_enrollees
    - active_members (from MEMBERS)

    This function is the single place to change when we later move to a nightly
    batch job that populates a CLIENT_ANALYSIS_SUMMARY table.
    """
    try:
        data = load_enrollee_data()

        # Load contract data (optional; used to restrict to current contracts)
        group_contract_pl: Optional[pl.DataFrame] = None
        try:
            conn = get_db_connection()
            group_contract_df = conn.execute(
                """
                SELECT 
                    groupid,
                    groupname,
                    startdate,
                    enddate,
                    iscurrent
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
                """
            ).fetchdf()
        except Exception as e:
            logger.warning(f"Could not load GROUP_CONTRACT for client summary: {e}")
            group_contract_df = None
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if group_contract_df is not None and not group_contract_df.empty:
            group_contract_pl = pl.from_pandas(group_contract_df)

        # Use a large limit so we effectively get all clients into the summary.
        # This is safe for current scale and can later be replaced with a direct
        # SELECT from a summary table populated by a nightly batch.
        large_limit = 10000

        top_cost_result = EnrolleeAnalyticsService.get_top_clients_by_total_cost(
            CLAIMS=data["CLAIMS"],
            PA=data["PA"],
            GROUPS=data["GROUPS"],
            GROUP_CONTRACT=group_contract_pl,
            limit=large_limit,
        )

        if not top_cost_result.get("success", False):
            raise HTTPException(
                status_code=500,
                detail=f"Error computing client cost summary: {top_cost_result.get('error')}",
            )

        cost_clients = top_cost_result.get("clients") or []
        if not cost_clients:
            return pl.DataFrame(
                {
                    "groupname": [],
                    "claims_cost": [],
                    "unclaimed_pa_cost": [],
                    "total_cost": [],
                    "visit_count": [],
                    "unique_enrollees": [],
                    "active_members": [],
                }
            )

        cost_df = pl.DataFrame(cost_clients)

        # Active members per client
        top_members_result = EnrolleeAnalyticsService.get_top_clients_by_active_members(
            MEMBERS=data["MEMBERS"],
            GROUPS=data["GROUPS"],
            limit=large_limit,
        )

        if not top_members_result.get("success", False):
            raise HTTPException(
                status_code=500,
                detail=f"Error computing client active members summary: {top_members_result.get('error')}",
            )

        members_clients = top_members_result.get("clients") or []
        if members_clients:
            members_df = pl.DataFrame(members_clients)
            # Only keep the pieces we need for joining
            cols_to_keep = [c for c in members_df.columns if c in ("groupname", "active_members")]
            members_df = members_df.select(cols_to_keep)

            summary_df = cost_df.join(
                members_df,
                on="groupname",
                how="left",
            ).with_columns(
                [
                    pl.col("active_members")
                    .fill_null(0)
                    .cast(pl.Int64)
                    .alias("active_members")
                ]
            )
        else:
            summary_df = cost_df.with_columns(
                [
                    pl.lit(0)
                    .cast(pl.Int64)
                    .alias("active_members")
                ]
            )

        return summary_df

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in calculate_client_summary_basic: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/quick")
async def get_client_dashboard_quick(
    limit: int = Query(20, ge=1, le=100),
    user_id: Optional[int] = Query(None, description="User ID for filtering allocated clients"),
):
    """
    Lightweight client list for fast response (e.g. when full dashboard times out on Render).
    Returns same JSON shape as /dashboard but with placeholder zeros; no heavy cost/member computation.
    """
    try:
        conn = get_db_connection()
        rows = conn.execute(
            """
            SELECT gc.groupname
            FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
            WHERE gc.iscurrent = 1
            ORDER BY gc.groupname
            LIMIT ?
            """,
            [min(limit * 3, 500)],  # fetch a bit more in case we filter by allocation
        ).fetchdf()
    except Exception as e:
        logger.warning(f"Dashboard quick query failed: {e}")
        rows = pd.DataFrame({"groupname": []})

    names = rows["groupname"].tolist() if not rows.empty else []
    allocated = get_user_allocated_clients(user_id)
    if allocated is not None:
        if not allocated:
            names = []
        else:
            names = [n for n in names if n in allocated][:limit]
    else:
        names = names[:limit]
    placeholder = [
        {
            "groupname": n,
            "claims_cost": 0,
            "unclaimed_pa_cost": 0,
            "total_cost": 0,
            "visit_count": 0,
            "unique_enrollees": 0,
            "active_members": 0,
        }
        for n in names
    ]
    return {
        "success": True,
        "dashboard": {
            "top_by_cost": placeholder,
            "top_by_active_members": placeholder,
        },
        "filters": {"limit": limit},
        "timestamp": datetime.now().isoformat(),
        "quick": True,
    }


@router.get("/dashboard")
async def get_client_dashboard(
    limit: int = Query(20, ge=1, le=100),
    user_id: Optional[int] = Query(None, description="User ID for filtering allocated clients"),
    quick: bool = Query(False, description="If true, return lightweight list only (avoids 502 on slow hosts like Render)"),
):
    """
    Client analytics landing data:
    1. Top clients by total medical cost (claims + unclaimed PA)
    2. Top clients by number of active members
    If user_id is provided, only returns clients allocated to that user.
    Use ?quick=1 for a fast response when the full computation times out (e.g. on Render).

    Internally this uses calculate_client_summary_basic(), which is the same
    core summary that a future nightly batch job can write into a summary table.
    """
    if quick:
        return await get_client_dashboard_quick(limit=limit, user_id=user_id)

    # Prefer precomputed summary table (populated by nightly batch) for fast response
    refreshed_at = None
    try:
        conn = get_db_connection()
        summary_df = load_client_dashboard_from_summary(conn)
    except Exception as e:
        logger.debug("Could not load client dashboard from summary: %s", e)
        summary_df = None
    if summary_df is not None and summary_df.height > 0:
        if "refreshed_at" in summary_df.columns:
            try:
                refreshed_at = summary_df["refreshed_at"][0]
                if hasattr(refreshed_at, "isoformat"):
                    refreshed_at = refreshed_at.isoformat()
            except Exception:
                pass
            summary_df = summary_df.select(
                [c for c in summary_df.columns if c != "refreshed_at"]
            )
    if summary_df is None or summary_df.height == 0:
        # Fallback: compute on the fly with timeout
        _DASHBOARD_TIMEOUT = 25.0
        try:
            loop = asyncio.get_event_loop()
            summary_df = await asyncio.wait_for(
                loop.run_in_executor(None, calculate_client_summary_basic),
                timeout=_DASHBOARD_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Client dashboard request timed out after %.0fs.",
                _DASHBOARD_TIMEOUT,
            )
            raise HTTPException(
                status_code=503,
                detail="Dashboard request took too long. Please try again or use ?quick=1.",
            )

    try:
        # Get user's allocated clients
        allocated_clients = get_user_allocated_clients(user_id)

        # Apply user-based filtering at the summary level
        if allocated_clients is not None:
            if len(allocated_clients) == 0:
                # User has no access - return empty lists
                top_by_cost_clients = []
                top_by_active_members_clients = []
            else:
                summary_df = summary_df.filter(pl.col("groupname").is_in(allocated_clients))

                top_by_cost_clients = (
                    summary_df.sort("total_cost", descending=True)
                    .head(limit)
                    .to_pandas()
                    .to_dict("records")
                )
                top_by_active_members_clients = (
                    summary_df.sort("active_members", descending=True)
                    .head(limit)
                    .to_pandas()
                    .to_dict("records")
                )
        else:
            # No filtering (MGT/ADMIN/IT or no user_id)
            top_by_cost_clients = (
                summary_df.sort("total_cost", descending=True)
                .head(limit)
                .to_pandas()
                .to_dict("records")
            )
            top_by_active_members_clients = (
                summary_df.sort("active_members", descending=True)
                .head(limit)
                .to_pandas()
                .to_dict("records")
            )

        out = {
            "success": True,
            "dashboard": {
                "top_by_cost": top_by_cost_clients,
                "top_by_active_members": top_by_active_members_clients,
            },
            "filters": {"limit": limit},
            "timestamp": datetime.now().isoformat(),
        }
        if refreshed_at is not None:
            out["refreshed_at"] = refreshed_at
        return out

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting client dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/companies")
async def get_client_companies(user_id: Optional[int] = Query(None, description="User ID for filtering allocated clients")):
    """
    Get list of all clients with active contracts.
    If user_id is provided, only returns clients allocated to that user.
    MGT, ADMIN, and IT can view all clients.
    """
    try:
        conn = get_db_connection()
        
        # Get user's allocated clients
        allocated_clients = get_user_allocated_clients(user_id)
        
        if allocated_clients is None:
            # No filtering - user can view all clients (MGT/ADMIN/IT or no user_id)
            companies = conn.execute(
                """
                SELECT DISTINCT groupname
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
                WHERE iscurrent = 1 AND enddate >= CURRENT_DATE
                ORDER BY groupname
                """
            ).fetchdf()
        elif len(allocated_clients) == 0:
            # User has no allocations - return empty list
            companies = pd.DataFrame(columns=['groupname'])
        else:
            # Filter by allocated clients
            placeholders = ','.join(['?' for _ in allocated_clients])
            companies = conn.execute(
                f"""
                SELECT DISTINCT groupname
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
                WHERE iscurrent = 1 
                AND enddate >= CURRENT_DATE
                AND groupname IN ({placeholders})
                ORDER BY groupname
                """,
                allocated_clients
            ).fetchdf()
        return {
            "success": True,
            "companies": companies['groupname'].tolist() if not companies.empty else []
        }
    except Exception as e:
        logger.error(f"Error getting client companies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profile/{groupname:path}")
async def get_client_profile(
    groupname: str,
    user_id: Optional[int] = Query(None, description="User ID for access control")
):
    """
    Get comprehensive profile for a specific client.
    If user_id is provided, verifies the user has access to this client.
    """
    try:
        # Check user access - user_id should always be provided for security
        logger.info(f"Profile request for {groupname} with user_id={user_id}")
        
        if not user_id:
            # If no user_id provided, deny access for security
            logger.warning(f"Profile request for {groupname} without user_id - denying access")
            raise HTTPException(
                status_code=401,
                detail="User authentication required. Please log in again."
            )
        
        allocated_clients = get_user_allocated_clients(user_id)
        logger.info(f"User {user_id} allocated clients: {allocated_clients}")
        
        # If user has no allocations (empty list), deny access
        if allocated_clients is not None and len(allocated_clients) == 0:
            logger.warning(f"User {user_id} has no allocated clients - denying access")
            raise HTTPException(
                status_code=403,
                detail="You do not have access to view any clients. Please contact your administrator."
            )
        
        # If user has specific allocations, check if this client is in the list
        if allocated_clients is not None and groupname not in allocated_clients:
            logger.warning(f"User {user_id} attempted to access {groupname} but only has access to {allocated_clients}")
            raise HTTPException(
                status_code=403,
                detail=f"You do not have access to view client '{groupname}'. Please contact your administrator."
            )
        
        # If we get here, user has access
        logger.info(f"User {user_id} granted access to {groupname}")
        
        conn = get_db_connection()
        try:
            profile_data = EnrolleeAnalyticsService.get_client_profile(
                groupname=groupname,
                conn=conn
            )
            return profile_data
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting client profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enrollees-cost-export/{groupname:path}")
async def export_enrollees_cost_csv(groupname: str):
    """
    Export CSV of total medical cost (claims + unclaimed PA) for each enrollee
    Returns raw data enrollee by enrollee
    """
    try:
        conn = get_db_connection()
        try:
            # Get client contract period
            contract_query = f"""
            SELECT 
                groupid,
                startdate,
                enddate
            FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
            WHERE groupname = '{groupname.replace("'", "''")}'
                AND iscurrent = 1
            LIMIT 1
            """
            contract_df = conn.execute(contract_query).fetchdf()
            
            if contract_df.empty:
                raise HTTPException(status_code=404, detail=f"Client '{groupname}' not found or no active contract")
            
            contract = contract_df.iloc[0]
            groupid = str(contract['groupid'])
            contract_start = str(contract['startdate'])
            contract_end = str(contract['enddate'])
            
            # Get total medical cost per enrollee (claims + unclaimed PA)
            enrollee_cost_query = f"""
            WITH client_enrollees AS (
                SELECT DISTINCT m.enrollee_id
                FROM "AI DRIVEN DATA"."MEMBERS" m
                WHERE m.groupid = {groupid}
                    AND m.iscurrent = 1
            ),
            claims_costs AS (
                SELECT 
                    c.enrollee_id,
                    SUM(c.approvedamount) as claims_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN client_enrollees ce ON c.enrollee_id = ce.enrollee_id
                WHERE c.encounterdatefrom >= DATE '{contract_start}'
                    AND c.encounterdatefrom <= DATE '{contract_end}'
                    AND c.approvedamount > 0
                GROUP BY c.enrollee_id
            ),
            claimed_pa_numbers AS (
                SELECT DISTINCT CAST(panumber AS INT64) as panumber_int
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE CAST(nhisgroupid AS VARCHAR) = '{groupid}'
                    AND panumber IS NOT NULL
                    AND encounterdatefrom >= DATE '{contract_start}'
                    AND encounterdatefrom <= DATE '{contract_end}'
            ),
            unclaimed_pa_costs AS (
                SELECT 
                    pa.IID as enrollee_id,
                    SUM(pa.granted) as pa_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                LEFT JOIN claimed_pa_numbers claimed ON CAST(pa.panumber AS INT64) = claimed.panumber_int
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end}'
                    AND pa.granted > 0
                    AND claimed.panumber_int IS NULL
                GROUP BY pa.IID
            )
            SELECT 
                COALESCE(cc.enrollee_id, upc.enrollee_id) as enrollee_id,
                COALESCE(cc.claims_cost, 0) as claims_cost,
                COALESCE(upc.pa_cost, 0) as unclaimed_pa_cost,
                COALESCE(cc.claims_cost, 0) + COALESCE(upc.pa_cost, 0) as total_medical_cost
            FROM claims_costs cc
            FULL OUTER JOIN unclaimed_pa_costs upc ON cc.enrollee_id = upc.enrollee_id
            ORDER BY total_medical_cost DESC
            """
            
            enrollee_cost_df = conn.execute(enrollee_cost_query).fetchdf()
            
            if enrollee_cost_df.empty:
                raise HTTPException(status_code=404, detail="No enrollee cost data found for this client")
            
            # Convert to CSV
            output = io.StringIO()
            enrollee_cost_df.to_csv(output, index=False)
            output.seek(0)
            
            # Create streaming response
            return StreamingResponse(
                io.BytesIO(output.getvalue().encode('utf-8')),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={groupname.replace(' ', '_')}_enrollee_costs_{datetime.now().strftime('%Y%m%d')}.csv"
                }
            )
            
        finally:
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting enrollee costs CSV: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/payment-schedules")
async def get_payment_schedules():
    """
    Get payment schedules from external invoice API.
    This endpoint proxies the external API to avoid CORS issues.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                "https://invoice-backend.clearlinehmo.com/api/v1/coverage-payment-schedule/public"
            )
            response.raise_for_status()
            return response.json()
    except httpx.TimeoutException:
        logger.error("Timeout fetching payment schedules from external API")
        raise HTTPException(status_code=504, detail="Timeout fetching payment schedules")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching payment schedules: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=f"External API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching payment schedules: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/payment-schedule/{groupname:path}")
async def get_client_payment_schedule(groupname: str):
    """
    Get payment schedule for a specific client by name.
    Filters the payment schedules to return only the matching client's schedule.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                "https://invoice-backend.clearlinehmo.com/api/v1/coverage-payment-schedule/public"
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("success") and data.get("data") and data["data"].get("schedules"):
                # Find the schedule for the specified client
                client_schedule = None
                for schedule in data["data"]["schedules"]:
                    if schedule.get("groupName") and \
                       schedule["groupName"].lower().strip() == groupname.lower().strip():
                        client_schedule = schedule
                        break
                
                if client_schedule:
                    return {
                        "success": True,
                        "data": client_schedule
                    }
                else:
                    return {
                        "success": False,
                        "message": f"No payment schedule found for client: {groupname}",
                        "data": None
                    }
            else:
                return {
                    "success": False,
                    "message": "No payment schedules available",
                    "data": None
                }
                
    except httpx.TimeoutException:
        logger.error("Timeout fetching payment schedule from external API")
        raise HTTPException(status_code=504, detail="Timeout fetching payment schedule")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching payment schedule: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=f"External API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching payment schedule: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/invoices")
async def get_all_invoices(
    perPage: str = Query("all", description="Number of invoices per page or 'all'"),
    status: Optional[str] = Query(None, description="Filter by invoice status (e.g., COMPLETE)")
):
    """
    Get all invoices from external API.
    Proxies request to external invoice backend API to avoid CORS issues.
    """
    try:
        params = {"perPage": perPage}
        if status:
            params["status"] = status
            
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                "https://invoice-backend.clearlinehmo.com/api/v1/invoice/public",
                params=params
            )
            response.raise_for_status()
            return response.json()
            
    except httpx.TimeoutException:
        logger.error("Timeout fetching invoices from external API")
        raise HTTPException(status_code=504, detail="External API timeout")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching invoices: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=f"External API error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_all_invoices: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/contract-periods")
async def get_contract_periods():
    """
    Get contract periods for all clients (current contracts only).
    Returns a mapping of client company names to their contract periods.
    """
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            groupname,
            startdate as contract_start,
            enddate as contract_end
        FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
        WHERE iscurrent = 1
        ORDER BY groupname
        """
        df = conn.execute(query).fetchdf()
        conn.close()
        
        # Convert to dictionary for easy lookup
        contract_periods = {}
        for _, row in df.iterrows():
            contract_periods[row['groupname']] = {
                'start': row['contract_start'].isoformat() if hasattr(row['contract_start'], 'isoformat') else str(row['contract_start']),
                'end': row['contract_end'].isoformat() if hasattr(row['contract_end'], 'isoformat') else str(row['contract_end'])
            }
        
        return {
            "success": True,
            "data": contract_periods
        }
    except Exception as e:
        logger.error(f"Error fetching contract periods: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/client-reconciliation")
async def get_client_reconciliation():
    """
    Get client reconciliation data: debit notes and cash received within contract periods.
    Returns total debit note amount, total cash received, and outstanding cash for each client.
    """
    try:
        conn = get_db_connection()
        
        query = """
        WITH contract_periods AS (
            SELECT 
                gc.groupid,
                gc.groupname,
                gc.startdate as contract_start,
                gc.enddate as contract_end
            FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
            WHERE gc.iscurrent = 1
        ),
        debit_note_totals AS (
            SELECT 
                cp.groupname,
                COALESCE(SUM(dn.Amount), 0) as total_debit_amount
            FROM contract_periods cp
            LEFT JOIN "AI DRIVEN DATA"."DEBIT_NOTE" dn 
                ON cp.groupname = dn.CompanyName
                AND dn."From" >= CAST(cp.contract_start AS DATE)
                AND dn."To" <= CAST(cp.contract_end AS DATE)
            GROUP BY cp.groupname
        ),
        cash_received_2023 AS (
            SELECT 
                cp.groupname,
                COALESCE(SUM(ccr.Amount), 0) as cash_2023
            FROM contract_periods cp
            LEFT JOIN "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2023" ccr 
                ON cp.groupname = ccr.groupname
                AND ccr.Date >= CAST(cp.contract_start AS DATE)
                AND ccr.Date <= CAST(cp.contract_end AS DATE)
            GROUP BY cp.groupname
        ),
        cash_received_2024 AS (
            SELECT 
                cp.groupname,
                COALESCE(SUM(ccr.Amount), 0) as cash_2024
            FROM contract_periods cp
            LEFT JOIN "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2024" ccr 
                ON cp.groupname = ccr.groupname
                AND ccr.Date >= CAST(cp.contract_start AS DATE)
                AND ccr.Date <= CAST(cp.contract_end AS DATE)
            GROUP BY cp.groupname
        ),
        cash_received_2025 AS (
            SELECT 
                cp.groupname,
                COALESCE(SUM(ccr.Amount), 0) as cash_2025
            FROM contract_periods cp
            LEFT JOIN "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2025" ccr 
                ON cp.groupname = ccr.groupname
                AND ccr.Date >= CAST(cp.contract_start AS DATE)
                AND ccr.Date <= CAST(cp.contract_end AS DATE)
            GROUP BY cp.groupname
        ),
        cash_received_combined AS (
            SELECT 
                cp.groupname,
                COALESCE(c2023.cash_2023, 0) + 
                COALESCE(c2024.cash_2024, 0) + 
                COALESCE(c2025.cash_2025, 0) as total_cash_received
            FROM contract_periods cp
            LEFT JOIN cash_received_2023 c2023 ON cp.groupname = c2023.groupname
            LEFT JOIN cash_received_2024 c2024 ON cp.groupname = c2024.groupname
            LEFT JOIN cash_received_2025 c2025 ON cp.groupname = c2025.groupname
        ),
        cash_month_before_2023 AS (
            SELECT 
                cp.groupname,
                COALESCE(SUM(ccr.Amount), 0) as cash_mb_2023
            FROM contract_periods cp
            LEFT JOIN "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2023" ccr 
                ON cp.groupname = ccr.groupname
                AND ccr.Date >= (DATE_TRUNC('month', cp.contract_start)::DATE - INTERVAL 1 MONTH)::DATE
                AND ccr.Date <= (DATE_TRUNC('month', cp.contract_start)::DATE - INTERVAL 1 DAY)::DATE
            GROUP BY cp.groupname
        ),
        cash_month_before_2024 AS (
            SELECT 
                cp.groupname,
                COALESCE(SUM(ccr.Amount), 0) as cash_mb_2024
            FROM contract_periods cp
            LEFT JOIN "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2024" ccr 
                ON cp.groupname = ccr.groupname
                AND ccr.Date >= (DATE_TRUNC('month', cp.contract_start)::DATE - INTERVAL 1 MONTH)::DATE
                AND ccr.Date <= (DATE_TRUNC('month', cp.contract_start)::DATE - INTERVAL 1 DAY)::DATE
            GROUP BY cp.groupname
        ),
        cash_month_before_2025 AS (
            SELECT 
                cp.groupname,
                COALESCE(SUM(ccr.Amount), 0) as cash_mb_2025
            FROM contract_periods cp
            LEFT JOIN "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2025" ccr 
                ON cp.groupname = ccr.groupname
                AND ccr.Date >= (DATE_TRUNC('month', cp.contract_start)::DATE - INTERVAL 1 MONTH)::DATE
                AND ccr.Date <= (DATE_TRUNC('month', cp.contract_start)::DATE - INTERVAL 1 DAY)::DATE
            GROUP BY cp.groupname
        ),
        cash_month_before_combined AS (
            SELECT 
                cp.groupname,
                COALESCE(mb2023.cash_mb_2023, 0) + 
                COALESCE(mb2024.cash_mb_2024, 0) + 
                COALESCE(mb2025.cash_mb_2025, 0) as total_cash_received_month_before
            FROM contract_periods cp
            LEFT JOIN cash_month_before_2023 mb2023 ON cp.groupname = mb2023.groupname
            LEFT JOIN cash_month_before_2024 mb2024 ON cp.groupname = mb2024.groupname
            LEFT JOIN cash_month_before_2025 mb2025 ON cp.groupname = mb2025.groupname
        )
        SELECT 
            cp.groupname,
            cp.contract_start,
            cp.contract_end,
            COALESCE(dnt.total_debit_amount, 0) as total_debit_amount,
            COALESCE(crc.total_cash_received, 0) as total_cash_received,
            COALESCE(cmb.total_cash_received_month_before, 0) as total_cash_received_month_before,
            COALESCE(dnt.total_debit_amount, 0) - COALESCE(crc.total_cash_received, 0) as outstanding_cash,
            CASE 
                WHEN COALESCE(dnt.total_debit_amount, 0) = 0 AND COALESCE(crc.total_cash_received, 0) = 0 THEN 1
                ELSE 0
            END as no_data
        FROM contract_periods cp
        LEFT JOIN debit_note_totals dnt ON cp.groupname = dnt.groupname
        LEFT JOIN cash_received_combined crc ON cp.groupname = crc.groupname
        LEFT JOIN cash_month_before_combined cmb ON cp.groupname = cmb.groupname
        ORDER BY cp.groupname
        """
        
        df = conn.execute(query).fetchdf()
        conn.close()
        
        # Convert to list of dictionaries
        reconciliation_data = []
        for _, row in df.iterrows():
            reconciliation_data.append({
                'clientName': row['groupname'],
                'contractStart': row['contract_start'].isoformat() if hasattr(row['contract_start'], 'isoformat') else str(row['contract_start']),
                'contractEnd': row['contract_end'].isoformat() if hasattr(row['contract_end'], 'isoformat') else str(row['contract_end']),
                'totalDebitAmount': float(row['total_debit_amount']) if row['total_debit_amount'] else 0.0,
                'totalCashReceived': float(row['total_cash_received']) if row['total_cash_received'] else 0.0,
                'totalCashReceivedMonthBeforeContract': float(row['total_cash_received_month_before']) if row['total_cash_received_month_before'] else 0.0,
                'outstandingCash': float(row['outstanding_cash']) if row['outstanding_cash'] else 0.0,
                'noData': bool(row['no_data'])
            })
        
        return {
            "success": True,
            "data": reconciliation_data
        }
    except Exception as e:
        logger.error(f"Error fetching client reconciliation: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/invoices/client/{groupname:path}")
async def get_client_invoices(groupname: str):
    """
    Get invoices for a specific client (groupname).
    Filters invoices from external API by client name.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                "https://invoice-backend.clearlinehmo.com/api/v1/invoice/public",
                params={"perPage": "all", "status": "COMPLETE"}
            )
            response.raise_for_status()
            data = response.json()
            
            # Filter invoices by groupname (match against clientCompany field)
            if data.get("success") and data.get("data", {}).get("invoices"):
                invoices = data["data"]["invoices"]
                matching_invoices = [
                    inv for inv in invoices 
                    if inv.get("clientCompany", "").upper().strip() == groupname.upper().strip()
                ]
                
                return {
                    "success": True,
                    "data": {
                        "invoices": matching_invoices,
                        "count": len(matching_invoices)
                    }
                }
            
            return {
                "success": True,
                "data": {
                    "invoices": [],
                    "count": 0
                }
            }
            
    except httpx.TimeoutException:
        logger.error("Timeout fetching client invoices from external API")
        raise HTTPException(status_code=504, detail="External API timeout")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching client invoices: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=f"External API error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_client_invoices: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
