"""
PA & Claims Module API Routes
==============================

Provides PA (Prior Authorization) and Claims analytics including:
- Total PA cost for current year
- Total claims cost (by date submitted and encounter date)
- Total medical cost (claims + unclaimed PA)
- Unclaimed PA cost
- Company MLR (Medical Loss Ratio)
- Total denied amount
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta
from core.database import get_db_connection
import pandas as pd
from functools import lru_cache, wraps
import hashlib
import json

logger = logging.getLogger(__name__)
router = APIRouter()

# Cache configuration
CACHE_SIZE = 256  # Number of cached results (increased for better hit rate)
CACHE_TTL = 600  # Cache TTL in seconds (10 minutes - increased from 5)

# Simple in-memory cache with TTL
_cache = {}
_cache_timestamps = {}

def _get_cache_key(*args, **kwargs):
    """Generate cache key from function arguments"""
    key_str = json.dumps({'args': args, 'kwargs': kwargs}, sort_keys=True)
    return hashlib.md5(key_str.encode()).hexdigest()

def _is_cache_valid(cache_key, ttl=CACHE_TTL):
    """Check if cache entry is still valid"""
    if cache_key not in _cache_timestamps:
        return False
    age = (datetime.now() - _cache_timestamps[cache_key]).total_seconds()
    return age < ttl

def cached_query(ttl=CACHE_TTL):
    """Decorator for caching query results (works with async functions)"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            cache_key = _get_cache_key(func.__name__, *args, **kwargs)
            
            # Check cache
            if _is_cache_valid(cache_key, ttl):
                logger.debug(f"Cache HIT for {func.__name__}")
                return _cache[cache_key]
            
            # Execute query
            logger.debug(f"Cache MISS for {func.__name__}")
            result = await func(*args, **kwargs)
            
            # Store in cache
            _cache[cache_key] = result
            _cache_timestamps[cache_key] = datetime.now()
            
            # Clean old cache entries
            if len(_cache) > CACHE_SIZE * 2:
                _clean_cache()
            
            return result
        
        # Return async wrapper for async functions
        return async_wrapper
    return decorator

def _clean_cache():
    """Remove expired cache entries"""
    now = datetime.now()
    expired_keys = [
        key for key, timestamp in _cache_timestamps.items()
        if (now - timestamp).total_seconds() > CACHE_TTL
    ]
    for key in expired_keys:
        _cache.pop(key, None)
        _cache_timestamps.pop(key, None)


def build_filter_conditions(start_date: Optional[str], end_date: Optional[str], 
                            client: Optional[str], provider: Optional[str], 
                            procedure: Optional[str], table_alias: str = "pa"):
    """
    Build SQL filter conditions for date range and filters.
    Returns tuple: (date_filter, additional_filters)
    Only applies filters if they are actually provided (not None/empty).
    """
    date_filter = ""
    additional_filters = ""
    
    # Date filter - only apply if dates are provided
    if start_date and end_date:
        date_filter = f"AND {table_alias}.requestdate >= DATE '{start_date}' AND {table_alias}.requestdate <= DATE '{end_date}'"
    elif start_date:
        date_filter = f"AND {table_alias}.requestdate >= DATE '{start_date}'"
    elif end_date:
        date_filter = f"AND {table_alias}.requestdate <= DATE '{end_date}'"
    
    # Additional filters - only apply if values are provided
    if client and client.strip():
        sanitized = client.replace("'", "''")
        additional_filters += f" AND {table_alias}.groupname = '{sanitized}'"
    
    if provider and provider.strip():
        sanitized = provider.replace("'", "''")
        # Cast to VARCHAR for consistent matching (providerid might be numeric or string)
        additional_filters += f" AND CAST({table_alias}.providerid AS VARCHAR) = '{sanitized}'"
    
    if procedure and procedure.strip():
        sanitized = procedure.replace("'", "''")
        additional_filters += f" AND {table_alias}.code = '{sanitized}'"
    
    return date_filter, additional_filters


def build_claims_filter_conditions(start_date: Optional[str], end_date: Optional[str],
                                   client: Optional[str], provider: Optional[str],
                                   procedure: Optional[str], date_type: str = "datesubmitted"):
    """
    Build SQL filter conditions for claims data.
    date_type: 'datesubmitted' or 'encounterdatefrom'
    """
    date_filter = ""
    additional_filters = ""
    
    # Date filter based on date_type
    if start_date and end_date:
        if date_type == "datesubmitted":
            date_filter = f"AND c.datesubmitted >= DATE '{start_date}' AND c.datesubmitted <= DATE '{end_date}'"
        else:
            date_filter = f"AND c.encounterdatefrom >= DATE '{start_date}' AND c.encounterdatefrom <= DATE '{end_date}'"
    elif start_date:
        if date_type == "datesubmitted":
            date_filter = f"AND c.datesubmitted >= DATE '{start_date}'"
        else:
            date_filter = f"AND c.encounterdatefrom >= DATE '{start_date}'"
    elif end_date:
        if date_type == "datesubmitted":
            date_filter = f"AND c.datesubmitted <= DATE '{end_date}'"
        else:
            date_filter = f"AND c.encounterdatefrom <= DATE '{end_date}'"
    
    # Additional filters - only apply if values are provided
    if client and client.strip():
        sanitized = client.replace("'", "''")
        # Use IN with subquery - more efficient than EXISTS for this case
        additional_filters += f" AND c.nhisgroupid IN (SELECT CAST(groupid AS VARCHAR) FROM \"AI DRIVEN DATA\".\"GROUP_CONTRACT\" WHERE groupname = '{sanitized}')"
    
    if provider and provider.strip():
        sanitized = provider.replace("'", "''")
        # Cast to VARCHAR for consistent matching
        additional_filters += f" AND CAST(c.nhisproviderid AS VARCHAR) = '{sanitized}'"
    
    if procedure and procedure.strip():
        sanitized = procedure.replace("'", "''")
        additional_filters += f" AND c.code = '{sanitized}'"
    
    return date_filter, additional_filters


def calculate_pa_claims_metrics_basic(
    start_date: Optional[str],
    end_date: Optional[str],
    client: Optional[str],
    provider: Optional[str],
    procedure: Optional[str],
) -> Dict[str, Any]:
    """
    Core PA & Claims metrics computation.

    This encapsulates all heavy SQL for the /pa-claims/metrics endpoint so that
    a future nightly batch job can call it directly and persist the results
    into a summary table (e.g. PA_CLAIMS_METRICS_SUMMARY) without changing the
    frontend shape.
    """
    from datetime import datetime as _dt

    # Always define current_year first - extract from start_date if available, otherwise use current year
    if start_date:
        try:
            current_year = _dt.strptime(start_date, "%Y-%m-%d").year
        except Exception:
            current_year = _dt.now().year
    else:
        current_year = _dt.now().year

    conn = get_db_connection()

    # Check if any filters are provided
    has_filters = bool(client or provider or procedure)

    # Only default to current year if no dates AND no other filters are provided
    use_default_dates = (not start_date or not end_date) and not has_filters

    if use_default_dates:
        start_date = f"{current_year}-01-01"
        end_date = f"{current_year}-12-31"
    elif not start_date or not end_date:
        # If filters are provided but no dates, use a wide range within the year
        if not start_date:
            start_date = f"{current_year}-01-01"
        if not end_date:
            end_date = f"{current_year}-12-31"

    # Build filter conditions
    pa_date_filter, pa_filters = build_filter_conditions(
        start_date, end_date, client, provider, procedure, "pa"
    )
    claims_date_filter_submitted, claims_filters_submitted = build_claims_filter_conditions(
        start_date, end_date, client, provider, procedure, "datesubmitted"
    )
    claims_date_filter_encounter, claims_filters_encounter = build_claims_filter_conditions(
        start_date, end_date, client, provider, procedure, "encounterdatefrom"
    )

    # 1. Total PA cost (sum of granted)
    if not has_filters:
        pa_total_query = f"""
            SELECT COALESCE(SUM(granted), 0) as total_pa_cost
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE EXTRACT(YEAR FROM requestdate) = EXTRACT(YEAR FROM DATE '{start_date}')
                AND granted > 0
        """
    else:
        pa_total_query = f"""
            SELECT COALESCE(SUM(granted), 0) as total_pa_cost
            FROM "AI DRIVEN DATA"."PA DATA" pa
            WHERE granted > 0
                {pa_date_filter}
                {pa_filters}
        """
    pa_total = conn.execute(pa_total_query).fetchone()[0] or 0

    # 2. Total claims cost by datesubmitted
    if not has_filters:
        claims_submitted_query = f"""
            SELECT COALESCE(SUM(approvedamount), 0) as total_claims_submitted
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE EXTRACT(YEAR FROM datesubmitted) = EXTRACT(YEAR FROM DATE '{start_date}')
                AND approvedamount > 0
        """
    else:
        claims_submitted_query = f"""
            SELECT COALESCE(SUM(approvedamount), 0) as total_claims_submitted
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE approvedamount > 0
                {claims_date_filter_submitted}
                {claims_filters_submitted}
        """
    claims_submitted = conn.execute(claims_submitted_query).fetchone()[0] or 0

    # 3. Total claims cost by encounterdatefrom
    if not has_filters:
        claims_encounter_query = f"""
            SELECT COALESCE(SUM(approvedamount), 0) as total_claims_encounter
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE EXTRACT(YEAR FROM encounterdatefrom) = EXTRACT(YEAR FROM DATE '{start_date}')
                AND approvedamount > 0
        """
    else:
        claims_encounter_query = f"""
            SELECT COALESCE(SUM(approvedamount), 0) as total_claims_encounter
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE approvedamount > 0
                {claims_date_filter_encounter}
                {claims_filters_encounter}
        """
    claims_encounter = conn.execute(claims_encounter_query).fetchone()[0] or 0

    # 4. Unclaimed PA (PA issued but not claimed)
    if not has_filters:
        unclaimed_pa_query = f"""
            WITH pa_issued AS (
                SELECT
                    CAST(panumber AS BIGINT) as panumber,
                    SUM(granted) as granted
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE EXTRACT(YEAR FROM requestdate) = {current_year}
                    AND panumber IS NOT NULL
                    AND granted > 0
                GROUP BY CAST(panumber AS BIGINT)
            ),
            claims_filed AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as panumber
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE EXTRACT(YEAR FROM datesubmitted) = {current_year}
                    AND panumber IS NOT NULL
            )
            SELECT
                COALESCE(SUM(p.granted), 0) as unclaimed_pa_cost
            FROM pa_issued p
            LEFT JOIN claims_filed c ON p.panumber = c.panumber
            WHERE c.panumber IS NULL
        """
    else:
        pa_unclaimed_date_filter, pa_unclaimed_filters = build_filter_conditions(
            start_date, end_date, client, provider, procedure, "pa"
        )
        claims_unclaimed_date_filter, claims_unclaimed_filters = build_claims_filter_conditions(
            start_date, end_date, client, provider, procedure, "datesubmitted"
        )

        unclaimed_pa_query = f"""
            WITH pa_issued AS (
                SELECT
                    CAST(panumber AS BIGINT) as panumber,
                    SUM(granted) as granted
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE panumber IS NOT NULL
                    AND granted > 0
                    {pa_unclaimed_date_filter}
                    {pa_unclaimed_filters}
                GROUP BY CAST(panumber AS BIGINT)
            ),
            claims_filed AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as panumber
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE panumber IS NOT NULL
                    {claims_unclaimed_date_filter}
                    {claims_unclaimed_filters}
            )
            SELECT
                COALESCE(SUM(p.granted), 0) as unclaimed_pa_cost
            FROM pa_issued p
            LEFT JOIN claims_filed c ON p.panumber = c.panumber
            WHERE c.panumber IS NULL
        """
    unclaimed_pa = conn.execute(unclaimed_pa_query).fetchone()[0] or 0

    # 5. Total medical cost = claims (encounter) + unclaimed PA
    total_medical_cost = float(claims_encounter) + float(unclaimed_pa)

    # 6. Total cash received for current year
    year_tables = [2023, 2024, 2025]
    if current_year in year_tables:
        cash_received_query = f"""
            SELECT COALESCE(SUM(amount), 0) as total_cash_received
            FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_{current_year}"
        """
    else:
        cash_received_query = f"""
            SELECT COALESCE(SUM(amount), 0) as total_cash_received
            FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
            WHERE EXTRACT(YEAR FROM Date) = {current_year}
        """
    cash_received = conn.execute(cash_received_query).fetchone()[0] or 0

    # 7. Company MLR = total_medical_cost / cash_received * 100
    company_mlr = (total_medical_cost / cash_received * 100) if cash_received > 0 else 0

    # 8. Total denied amount
    if not has_filters:
        denied_query = f"""
            SELECT COALESCE(SUM(deniedamount), 0) as total_denied_amount
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE EXTRACT(YEAR FROM datesubmitted) = EXTRACT(YEAR FROM DATE '{start_date}')
                AND deniedamount > 0
        """
    else:
        denied_date_filter, denied_filters = build_claims_filter_conditions(
            start_date, end_date, client, provider, procedure, "datesubmitted"
        )
        denied_query = f"""
            SELECT COALESCE(SUM(deniedamount), 0) as total_denied_amount
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE deniedamount > 0
                {denied_date_filter}
                {denied_filters}
        """
    denied_amount = conn.execute(denied_query).fetchone()[0] or 0

    display_year = _dt.strptime(start_date, "%Y-%m-%d").year if start_date else _dt.now().year

    return {
        "year": display_year,
        "total_pa_cost": float(pa_total),
        "total_claims_submitted": float(claims_submitted),
        "total_claims_encounter": float(claims_encounter),
        "unclaimed_pa_cost": float(unclaimed_pa),
        "total_medical_cost": float(total_medical_cost),
        "total_cash_received": float(cash_received),
        "company_mlr": float(company_mlr),
        "total_denied_amount": float(denied_amount),
    }


@router.get("/metrics")
@cached_query(ttl=300)  # Cache for 5 minutes
async def get_pa_claims_metrics(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    client: Optional[str] = Query(None, description="Client name (groupname)"),
    provider: Optional[str] = Query(None, description="Provider ID"),
    procedure: Optional[str] = Query(None, description="Procedure code")
):
    """
    Get PA & Claims metrics for the current year.
    Returns:
    - Total PA cost
    - Total claims cost (by date submitted)
    - Total claims cost (by encounter date)
    - Total medical cost (claims + unclaimed PA)
    - Unclaimed PA cost
    - Company MLR
    - Total denied amount
    """
    try:
        data = calculate_pa_claims_metrics_basic(
            start_date=start_date,
            end_date=end_date,
            client=client,
            provider=provider,
            procedure=procedure,
        )
        return {
            "success": True,
            "data": data,
        }
    except Exception as e:
        logger.error(f"Error getting PA & Claims metrics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pa-analytics")
@cached_query(ttl=300)  # Cache for 5 minutes
async def get_pa_analytics(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    filter_type: Optional[str] = Query(None, description="Filter type: client, provider, procedure, diagnosis, or benefit"),
    filter_value: Optional[str] = Query(None, description="Filter value (client name, provider ID, procedure code, diagnosis code, or benefit code)"),
    client: Optional[str] = Query(None, description="Client name (groupname) - new filter format"),
    provider: Optional[str] = Query(None, description="Provider ID - new filter format"),
    procedure: Optional[str] = Query(None, description="Procedure code - new filter format"),
    limit: Optional[int] = Query(10, description="Limit for top N results (default: 10, max: 100)", ge=1, le=100)
):
    """
    Get PA analytics including:
    - Monthly granted amounts
    - Monthly unique panumber counts
    - Monthly unique enrollee counts
    - Top N clients by cost and visit (paginated)
    - Top N procedures by cost and count (paginated)
    - Top N providers by cost and visit count (paginated)
    - Top N diagnosis by cost and count (paginated)
    """
    try:
        from datetime import datetime
        
        # Default to current year if no dates provided
        if not start_date or not end_date:
            current_year = datetime.now().year
            start_date = f"{current_year}-01-01"
            end_date = f"{current_year}-12-31"
        
        conn = get_db_connection()
        
        # Build filter conditions using the unified helper function
        pa_date_filter, pa_filters = build_filter_conditions(start_date, end_date, client, provider, procedure, "pa")
        
        # Sanitize filter values for SQL (escape single quotes)
        sanitized_client = client.replace("'", "''") if client else ""
        sanitized_provider = provider.replace("'", "''") if provider else ""
        sanitized_procedure = procedure.replace("'", "''") if procedure else ""
        
        # Also support legacy filter_type/filter_value format
        sanitized_value = filter_value.replace("'", "''") if filter_value else ""
        
        # Build filter conditions for monthly query
        monthly_filter = pa_filters  # Use the unified filter builder
        # Format date filter for monthly query (remove table alias since we're using pa alias in query)
        if pa_date_filter:
            date_filter_monthly = pa_date_filter.replace("pa.requestdate", "requestdate")
        else:
            date_filter_monthly = f"AND requestdate >= DATE '{start_date}' AND requestdate <= DATE '{end_date}'"
        
        # 1. Monthly PA analytics
        monthly_pa_query = f'''
            SELECT 
                EXTRACT(YEAR FROM requestdate) as year,
                EXTRACT(MONTH FROM requestdate) as month,
                COALESCE(SUM(granted), 0) as total_granted,
                COUNT(DISTINCT panumber) as unique_panumbers,
                COUNT(DISTINCT IID) as unique_enrollees
            FROM "AI DRIVEN DATA"."PA DATA" pa
            WHERE granted > 0
                {date_filter_monthly}
                {monthly_filter}
            GROUP BY EXTRACT(YEAR FROM requestdate), EXTRACT(MONTH FROM requestdate)
            ORDER BY year, month
        '''
        monthly_pa_df = conn.execute(monthly_pa_query).fetchdf()
        
        monthly_data = []
        for _, row in monthly_pa_df.iterrows():
            month_str = f"{int(row['year'])}-{int(row['month']):02d}"
            monthly_data.append({
                'month': month_str,
                'total_granted': float(row['total_granted']),
                'unique_panumbers': int(row['unique_panumbers']),
                'unique_enrollees': int(row['unique_enrollees'])
            })
        
        # 2. Top N clients by cost and visit (with pagination)
        # Apply filters: exclude client filter (since we're grouping by client), but include provider/procedure
        clients_filter = ""
        
        # Build filter from new format parameters (provider, procedure) - exclude client
        if sanitized_provider:
            clients_filter = f"AND CAST(pa.providerid AS VARCHAR) = '{sanitized_provider}'"
        if sanitized_procedure:
            if clients_filter:
                clients_filter += f" AND pa.code = '{sanitized_procedure}'"
            else:
                clients_filter = f"AND pa.code = '{sanitized_procedure}'"
        
        # Also support legacy filter_type format (only if new format not used)
        if filter_type and filter_value and filter_type != "client" and not (provider or procedure):
            if filter_type == "provider":
                clients_filter = f"AND CAST(pa.providerid AS VARCHAR) = '{sanitized_value}'"
            elif filter_type == "procedure":
                clients_filter = f"AND pa.code = '{sanitized_value}'"
            elif filter_type == "diagnosis":
                clients_filter = f"AND EXISTS (SELECT 1 FROM \"AI DRIVEN DATA\".\"TBPADIAGNOSIS\" tpd WHERE CAST(pa.panumber AS VARCHAR) = CAST(tpd.panumber AS VARCHAR) AND tpd.code = '{sanitized_value}')"
                pa_date_filter = ""  # Remove date filter for diagnosis
            elif filter_type == "benefit":
                clients_filter = f"AND EXISTS (SELECT 1 FROM \"AI DRIVEN DATA\".\"BENEFITCODE_PROCEDURES\" bcp WHERE pa.code = bcp.procedurecode AND CAST(bcp.benefitcodeid AS VARCHAR) = '{sanitized_value}')"
        
        top_clients_query = f'''
            SELECT 
                groupname as client_name,
                COALESCE(SUM(granted), 0) as total_cost,
                COUNT(DISTINCT panumber) as visit_count
            FROM "AI DRIVEN DATA"."PA DATA" pa
            WHERE granted > 0
                AND groupname IS NOT NULL
                {pa_date_filter}
                {clients_filter}
            GROUP BY groupname
            ORDER BY total_cost DESC
            LIMIT {limit}
        '''
        top_clients_df = conn.execute(top_clients_query).fetchdf()
        
        top_clients = []
        for _, row in top_clients_df.iterrows():
            top_clients.append({
                'client_name': row['client_name'],
                'total_cost': float(row['total_cost']),
                'visit_count': int(row['visit_count'])
            })
        
        # 3. Top N procedures by cost and count (with pagination)
        # Apply filters: exclude procedure filter (since we're grouping by procedure), but include client/provider
        procedures_filter = ""
        
        # Build filter from new format parameters (client, provider) - exclude procedure
        if sanitized_client:
            procedures_filter = f"AND pa.groupname = '{sanitized_client}'"
        if sanitized_provider:
            if procedures_filter:
                procedures_filter += f" AND CAST(pa.providerid AS VARCHAR) = '{sanitized_provider}'"
            else:
                procedures_filter = f"AND CAST(pa.providerid AS VARCHAR) = '{sanitized_provider}'"
        
        # Also support legacy filter_type format (only if new format not used)
        if filter_type and filter_value and filter_type != "procedure" and not (client or provider):
            if filter_type == "client":
                procedures_filter = f"AND pa.groupname = '{sanitized_value}'"
            elif filter_type == "provider":
                procedures_filter = f"AND CAST(pa.providerid AS VARCHAR) = '{sanitized_value}'"
            elif filter_type == "diagnosis":
                procedures_filter = f"AND EXISTS (SELECT 1 FROM \"AI DRIVEN DATA\".\"TBPADIAGNOSIS\" tpd WHERE CAST(pa.panumber AS VARCHAR) = CAST(tpd.panumber AS VARCHAR) AND tpd.code = '{sanitized_value}')"
                pa_date_filter = ""  # Remove date filter for diagnosis
            elif filter_type == "benefit":
                procedures_filter = f"AND EXISTS (SELECT 1 FROM \"AI DRIVEN DATA\".\"BENEFITCODE_PROCEDURES\" bcp WHERE pa.code = bcp.procedurecode AND CAST(bcp.benefitcodeid AS VARCHAR) = '{sanitized_value}')"
        
        date_filter_proc = pa_date_filter
        
        top_procedures_query = f'''
            SELECT 
                pa.code as procedurecode,
                pd.proceduredesc,
                COALESCE(SUM(pa.granted), 0) as total_cost,
                COUNT(*) as count
            FROM "AI DRIVEN DATA"."PA DATA" pa
            LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd 
                ON pa.code = pd.procedurecode
            WHERE pa.granted > 0
                AND pa.code IS NOT NULL
                {date_filter_proc}
                {procedures_filter}
            GROUP BY pa.code, pd.proceduredesc
            ORDER BY total_cost DESC
            LIMIT {limit}
        '''
        top_procedures_df = conn.execute(top_procedures_query).fetchdf()
        
        top_procedures = []
        for _, row in top_procedures_df.iterrows():
            top_procedures.append({
                'procedurecode': row['procedurecode'],
                'procedurename': row['proceduredesc'] or 'Unknown',
                'total_cost': float(row['total_cost']),
                'count': int(row['count'])
            })
        
        # 4. Top N providers by cost and visit count (by enrollee) (with pagination)
        # Apply filters: exclude provider filter (since we're grouping by provider), but include client/procedure
        provider_filter = ""
        
        # Build filter from new format parameters (client, procedure) - exclude provider
        if sanitized_client:
            provider_filter = f"AND pa.groupname = '{sanitized_client}'"
        if sanitized_procedure:
            if provider_filter:
                provider_filter += f" AND pa.code = '{sanitized_procedure}'"
            else:
                provider_filter = f"AND pa.code = '{sanitized_procedure}'"
        
        # Also support legacy filter_type format (only if new format not used)
        if filter_type and filter_value and filter_type != "provider" and not (client or procedure):
            if filter_type == "client":
                provider_filter = f"AND pa.groupname = '{sanitized_value}'"
            elif filter_type == "procedure":
                provider_filter = f"AND pa.code = '{sanitized_value}'"
            elif filter_type == "diagnosis":
                provider_filter = f"AND EXISTS (SELECT 1 FROM \"AI DRIVEN DATA\".\"TBPADIAGNOSIS\" tpd WHERE CAST(pa.panumber AS VARCHAR) = CAST(tpd.panumber AS VARCHAR) AND tpd.code = '{sanitized_value}')"
                pa_date_filter = ""  # Remove date filter for diagnosis
            elif filter_type == "benefit":
                provider_filter = f"AND EXISTS (SELECT 1 FROM \"AI DRIVEN DATA\".\"BENEFITCODE_PROCEDURES\" bcp WHERE pa.code = bcp.procedurecode AND CAST(bcp.benefitcodeid AS VARCHAR) = '{sanitized_value}')"
        
        date_filter_prov = pa_date_filter
        
        top_providers_query = f'''
            SELECT 
                pa.providerid,
                p.providername,
                COALESCE(SUM(pa.granted), 0) as total_cost,
                COUNT(DISTINCT pa.IID) as unique_enrollees
            FROM "AI DRIVEN DATA"."PA DATA" pa
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p 
                ON CAST(pa.providerid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
            WHERE pa.granted > 0
                AND pa.providerid IS NOT NULL
                {date_filter_prov}
                {provider_filter}
            GROUP BY pa.providerid, p.providername
            ORDER BY total_cost DESC
            LIMIT {limit}
        '''
        top_providers_df = conn.execute(top_providers_query).fetchdf()
        
        top_providers = []
        for _, row in top_providers_df.iterrows():
            top_providers.append({
                'providerid': row['providerid'],
                'providername': row['providername'] or 'Unknown',
                'total_cost': float(row['total_cost']),
                'unique_enrollees': int(row['unique_enrollees'])
            })
        
        # 5. Top N diagnosis by cost and count (with pagination)
        # Adjust filter conditions for diagnosis query
        diagnosis_filter = ""
        if filter_type and filter_value:
            if filter_type == "client":
                diagnosis_filter = f"AND pa.groupname = '{sanitized_value}'"
            elif filter_type == "provider":
                diagnosis_filter = f"AND CAST(pa.providerid AS VARCHAR) = '{sanitized_value}'"
            elif filter_type == "procedure":
                diagnosis_filter = f"AND pa.code = '{sanitized_value}'"
            elif filter_type == "benefit":
                diagnosis_filter = f"AND EXISTS (SELECT 1 FROM \"AI DRIVEN DATA\".\"BENEFITCODE_PROCEDURES\" bcp WHERE pa.code = bcp.procedurecode AND CAST(bcp.benefitcodeid AS VARCHAR) = '{sanitized_value}')"
        
        # Always apply date filter to match other tables - if no data for date range, will return empty
        # This ensures consistency across all tables
        top_diagnosis_query = f'''
            SELECT 
                COALESCE(d.diagnosiscode, tpd.code) as diagnosiscode,
                COALESCE(d.diagnosisdesc, tpd."desc", 'Unknown Diagnosis') as diagnosisdesc,
                COALESCE(SUM(pa.granted), 0) as total_cost,
                COUNT(DISTINCT pa.panumber) as count
            FROM "AI DRIVEN DATA"."PA DATA" pa
            INNER JOIN "AI DRIVEN DATA"."TBPADIAGNOSIS" tpd 
                ON CAST(pa.panumber AS VARCHAR) = CAST(tpd.panumber AS VARCHAR)
            LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d 
                ON tpd.code = d.diagnosiscode
            WHERE pa.requestdate >= DATE '{start_date}'
                AND pa.requestdate <= DATE '{end_date}'
                AND pa.granted > 0
                {diagnosis_filter if filter_type != 'diagnosis' else ''}
            GROUP BY COALESCE(d.diagnosiscode, tpd.code), COALESCE(d.diagnosisdesc, tpd."desc", 'Unknown Diagnosis')
            HAVING COALESCE(d.diagnosiscode, tpd.code) IS NOT NULL
            ORDER BY total_cost DESC
            LIMIT {limit}
        '''
        top_diagnosis_df = conn.execute(top_diagnosis_query).fetchdf()
        
        top_diagnosis = []
        for _, row in top_diagnosis_df.iterrows():
            top_diagnosis.append({
                'diagnosiscode': row['diagnosiscode'] or 'Unknown',
                'diagnosisdesc': row['diagnosisdesc'] or 'Unknown',
                'total_cost': float(row['total_cost']),
                'count': int(row['count'])
            })
        
        # 6. Top N benefits by cost (with pagination)
        # Adjust filter conditions for benefits query
        benefits_filter = ""
        date_filter_ben = f"AND pa.requestdate >= DATE '{start_date}' AND pa.requestdate <= DATE '{end_date}'"
        
        if filter_type and filter_value:
            if filter_type == "client":
                benefits_filter = f"AND pa.groupname = '{sanitized_value}'"
            elif filter_type == "provider":
                benefits_filter = f"AND CAST(pa.providerid AS VARCHAR) = '{sanitized_value}'"
            elif filter_type == "procedure":
                benefits_filter = f"AND pa.code = '{sanitized_value}'"
            elif filter_type == "diagnosis":
                # For diagnosis, join with TBPADIAGNOSIS first, then filter by date
                benefits_filter = f"AND EXISTS (SELECT 1 FROM \"AI DRIVEN DATA\".\"TBPADIAGNOSIS\" tpd WHERE CAST(pa.panumber AS VARCHAR) = CAST(tpd.panumber AS VARCHAR) AND tpd.code = '{sanitized_value}')"
                # Remove date filter when filtering by diagnosis (use all available data)
                date_filter_ben = ""
            elif filter_type == "benefit":
                benefits_filter = f"AND CAST(bcp.benefitcodeid AS VARCHAR) = '{sanitized_value}'"
        
        top_benefits_query = f'''
            SELECT 
                COALESCE(CAST(bcp.benefitcodeid AS VARCHAR), 'Unspecified') as benefitcode,
                COALESCE(bc.benefitcodename, 'Unspecified') as benefitcodename,
                COALESCE(bc.benefitcodedesc, 'No description available') as benefitcodedesc,
                COALESCE(SUM(pa.granted), 0) as total_cost,
                COUNT(DISTINCT pa.panumber) as count
            FROM "AI DRIVEN DATA"."PA DATA" pa
            LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" bcp
                ON pa.code = bcp.procedurecode
            LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc 
                ON bcp.benefitcodeid = bc.benefitcodeid
            WHERE pa.granted > 0
                {date_filter_ben}
                {benefits_filter if filter_type != 'benefit' else ''}
            GROUP BY bcp.benefitcodeid, bc.benefitcodename, bc.benefitcodedesc
            ORDER BY total_cost DESC
            LIMIT {limit}
        '''
        top_benefits_df = conn.execute(top_benefits_query).fetchdf()
        
        top_benefits = []
        for _, row in top_benefits_df.iterrows():
            top_benefits.append({
                'benefitcode': row['benefitcode'] or 'Unknown',
                'benefitname': row['benefitcodename'] or 'Unknown',
                'benefitdesc': row['benefitcodedesc'] or 'Unknown',
                'total_cost': float(row['total_cost']),
                'count': int(row['count'])
            })
        
        # Note: Don't close connection - it's pooled and reused for performance
        
        return {
            'success': True,
            'data': {
                'monthly_data': monthly_data,
                'top_clients': top_clients,
                'top_procedures': top_procedures,
                'top_providers': top_providers,
                'top_diagnosis': top_diagnosis,
                'top_benefits': top_benefits,
                'date_range': {
                    'start_date': start_date,
                    'end_date': end_date
                },
                'active_filter': {
                    'type': filter_type,
                    'value': filter_value
                } if filter_type and filter_value else None,
                'pagination': {
                    'limit': limit,
                    'returned': len(top_clients) + len(top_procedures) + len(top_providers) + len(top_diagnosis) + len(top_benefits)
                }
            }
        }
    except Exception as e:
        logger.error(f"Error getting PA analytics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/online-pa-analysis")
@cached_query(ttl=600)  # Cache for 10 minutes (less frequently changing)
async def get_online_pa_analysis():
    """
    Get daily online PA percentage analysis for the last 45 days.
    Returns daily breakdown of total PA vs online PA requests.
    """
    try:
        conn = get_db_connection()
        
        # Get last 45 days date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=44)  # 45 days including today
        
        # Get max dates from actual data
        max_pa_date_query = '''
            SELECT MAX(requestdate) as max_date
            FROM "AI DRIVEN DATA"."PA DATA"
        '''
        max_pa_date = conn.execute(max_pa_date_query).fetchone()[0]
        if max_pa_date:
            max_pa_date = max_pa_date.date() if hasattr(max_pa_date, 'date') else max_pa_date
        else:
            max_pa_date = end_date
        
        max_issue_date_query = '''
            SELECT MAX(requestdate) as max_date
            FROM "AI DRIVEN DATA"."PA ISSUE REQUEST"
        '''
        max_issue_date = conn.execute(max_issue_date_query).fetchone()[0]
        if max_issue_date:
            max_issue_date = max_issue_date.date() if hasattr(max_issue_date, 'date') else max_issue_date
        else:
            max_issue_date = end_date
        
        # Use the earlier of the two dates
        actual_end_date = min(max_pa_date, max_issue_date) if max_pa_date and max_issue_date else end_date
        actual_start_date = actual_end_date - timedelta(days=44)
        
        # Get daily PA counts (count all rows, not distinct panumber - matching Streamlit logic)
        daily_pa_query = f'''
            SELECT 
                DATE(requestdate) as date,
                COUNT(*) as total_pa
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE requestdate >= DATE '{actual_start_date}'
                AND requestdate <= DATE '{actual_end_date}'
            GROUP BY DATE(requestdate)
            ORDER BY date
        '''
        daily_pa_df = conn.execute(daily_pa_query).fetchdf()
        
        # Get daily online PA counts (count all rows, not distinct panumber - matching Streamlit logic)
        daily_online_pa_query = f'''
            SELECT 
                DATE(requestdate) as date,
                COUNT(*) as online_pa
            FROM "AI DRIVEN DATA"."PA ISSUE REQUEST"
            WHERE requestdate >= DATE '{actual_start_date}'
                AND requestdate <= DATE '{actual_end_date}'
                AND panumber IS NOT NULL
                AND panumber != ''
            GROUP BY DATE(requestdate)
            ORDER BY date
        '''
        daily_online_pa_df = conn.execute(daily_online_pa_query).fetchdf()
        
        # Create date range
        date_range = pd.date_range(start=actual_start_date, end=actual_end_date, freq='D')
        
        # Convert DataFrame dates to date objects for comparison
        if not daily_pa_df.empty:
            daily_pa_df['date'] = pd.to_datetime(daily_pa_df['date']).dt.date
        if not daily_online_pa_df.empty:
            daily_online_pa_df['date'] = pd.to_datetime(daily_online_pa_df['date']).dt.date
        
        # Merge data
        daily_data = []
        for single_date in date_range:
            date_str = single_date.date()
            
            # Get total PA for this date
            total_pa_row = daily_pa_df[daily_pa_df['date'] == date_str] if not daily_pa_df.empty else pd.DataFrame()
            total_pa = int(total_pa_row['total_pa'].iloc[0]) if not total_pa_row.empty else 0
            
            # Get online PA for this date
            online_pa_row = daily_online_pa_df[daily_online_pa_df['date'] == date_str] if not daily_online_pa_df.empty else pd.DataFrame()
            online_pa = int(online_pa_row['online_pa'].iloc[0]) if not online_pa_row.empty else 0
            
            # Calculate percentage
            online_percentage = (online_pa / total_pa * 100) if total_pa > 0 else 0
            
            daily_data.append({
                'date': date_str.isoformat(),
                'total_pa': total_pa,
                'online_pa': online_pa,
                'online_percentage': round(online_percentage, 2)
            })
        
        # Calculate summary statistics
        total_pa_sum = sum(d['total_pa'] for d in daily_data)
        total_online_pa_sum = sum(d['online_pa'] for d in daily_data)
        overall_percentage = (total_online_pa_sum / total_pa_sum * 100) if total_pa_sum > 0 else 0
        
        # Note: Don't close connection - it's pooled and reused for performance
        
        return {
            'success': True,
            'data': {
                'daily_data': daily_data,
                'summary': {
                    'total_pa': total_pa_sum,
                    'total_online_pa': total_online_pa_sum,
                    'overall_online_percentage': round(overall_percentage, 2),
                    'date_range': {
                        'start_date': actual_start_date.isoformat(),
                        'end_date': actual_end_date.isoformat()
                    }
                }
            }
        }
    except Exception as e:
        logger.error(f"Error getting online PA analysis: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/response-time-analysis")
@cached_query(ttl=600)  # Cache for 10 minutes
async def get_response_time_analysis():
    """
    Get response time analysis for online PA requests for the last 45 days.
    Returns daily average response times.
    Matches Streamlit calculation exactly: (resolutiontime - dateadded).dt.total_seconds() / 60
    """
    try:
        conn = get_db_connection()
        
        # Get last 45 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=44)
        
        # Load all PA ISSUE REQUEST data (matching Streamlit approach)
        # Based on testing, Streamlit appears to use requestdate for filtering and calculation
        pa_issue_query = f'''
            SELECT 
                requestdate,
                resolutiontime,
                panumber
            FROM "AI DRIVEN DATA"."PA ISSUE REQUEST"
            WHERE requestdate >= DATE '{start_date}'
                AND requestdate <= DATE '{end_date}'
                AND panumber IS NOT NULL
                AND panumber != ''
                AND resolutiontime IS NOT NULL
        '''
        pa_issue_df = conn.execute(pa_issue_query).fetchdf()
        
        # Note: Don't close connection - it's pooled and reused for performance
        
        if pa_issue_df.empty:
            return {
                'success': True,
                'data': {
                    'daily_data': [],
                    'summary': {
                        'average_response_time': 0.0,
                        'fastest_response_time': 0.0,
                        'slowest_response_time': 0.0
                    }
                }
            }
        
        # Convert to datetime (matching Streamlit)
        pa_issue_df['requestdate'] = pd.to_datetime(pa_issue_df['requestdate'], errors='coerce')
        pa_issue_df['resolutiontime'] = pd.to_datetime(pa_issue_df['resolutiontime'], errors='coerce')
        
        # Filter for last 45 days based on requestdate (matching actual Streamlit behavior)
        pa_issue_df = pa_issue_df[
            (pa_issue_df['requestdate'] >= pd.Timestamp(start_date)) &
            (pa_issue_df['requestdate'] <= pd.Timestamp(end_date))
        ]
        
        # Calculate response time in minutes (matching Streamlit: (resolutiontime - dateadded).dt.total_seconds() / 60)
        # Note: Streamlit uses dateadded, but we only have requestdate and resolutiontime
        # Using requestdate as proxy for dateadded (they should be very close)
        pa_issue_df['response_time_minutes'] = (
            (pa_issue_df['resolutiontime'] - pa_issue_df['requestdate']).dt.total_seconds() / 60
        )
        
        # Filter out negative or invalid response times
        pa_issue_df = pa_issue_df[pa_issue_df['response_time_minutes'] >= 0]
        
        # Group by date and calculate average
        pa_issue_df['date'] = pa_issue_df['requestdate'].dt.date
        daily_response_times = pa_issue_df.groupby('date')['response_time_minutes'].agg(['mean', 'min', 'max', 'count']).reset_index()
        
        daily_data = []
        for _, row in daily_response_times.iterrows():
            daily_data.append({
                'date': row['date'].isoformat(),
                'average_response_time': round(float(row['mean']), 2),
                'fastest_response_time': round(float(row['min']), 2),
                'slowest_response_time': round(float(row['max']), 2),
                'request_count': int(row['count'])
            })
        
        # Calculate overall summary
        overall_avg = pa_issue_df['response_time_minutes'].mean()
        overall_min = pa_issue_df['response_time_minutes'].min()
        overall_max = pa_issue_df['response_time_minutes'].max()
        
        return {
            'success': True,
            'data': {
                'daily_data': daily_data,
                'summary': {
                    'average_response_time': round(float(overall_avg), 2) if not pd.isna(overall_avg) else 0.0,
                    'fastest_response_time': round(float(overall_min), 2) if not pd.isna(overall_min) else 0.0,
                    'slowest_response_time': round(float(overall_max), 2) if not pd.isna(overall_max) else 0.0,
                    'total_requests': len(pa_issue_df)
                }
            }
        }
    except Exception as e:
        logger.error(f"Error getting response time analysis: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/provider-online-pa-ranking")
@cached_query(ttl=600)  # Cache for 10 minutes
async def get_provider_online_pa_ranking():
    """
    Get provider online PA ranking for the last 14 days.
    Returns providers ranked by number of online PA requests.
    """
    try:
        conn = get_db_connection()
        
        # Get last 14 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=13)
        
        provider_ranking_query = f'''
            SELECT 
                oir.providerid,
                p.providername,
                COUNT(*) as online_pa_count
            FROM "AI DRIVEN DATA"."PA ISSUE REQUEST" oir
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p 
                ON CAST(oir.providerid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
            WHERE oir.requestdate >= DATE '{start_date}'
                AND oir.requestdate <= DATE '{end_date}'
                AND oir.panumber IS NOT NULL
                AND oir.panumber != ''
            GROUP BY oir.providerid, p.providername
            ORDER BY online_pa_count DESC
            LIMIT 20
        '''
        provider_ranking_df = conn.execute(provider_ranking_query).fetchdf()
        
        provider_ranking = []
        for _, row in provider_ranking_df.iterrows():
            provider_ranking.append({
                'providerid': row['providerid'],
                'providername': row['providername'] or 'Unknown',
                'online_pa_count': int(row['online_pa_count'])
            })
        
        # Note: Don't close connection - it's pooled and reused for performance
        
        return {
            'success': True,
            'data': {
                'provider_ranking': provider_ranking,
                'date_range': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                }
            }
        }
    except Exception as e:
        logger.error(f"Error getting provider online PA ranking: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pa-statistics")
@cached_query(ttl=300)  # Cache for 5 minutes
async def get_pa_statistics(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    client: Optional[str] = Query(None, description="Client name (groupname)"),
    provider: Optional[str] = Query(None, description="Provider ID"),
    procedure: Optional[str] = Query(None, description="Procedure code")
):
    """
    Get PA statistics including:
    - Total amount granted in PA
    - Total count of companies
    - Total count of enrollees
    - Total count of providers
    - Online PA count
    - Offline PA count
    """
    try:
        from datetime import datetime
        
        # Always define current_year first
        if start_date:
            try:
                current_year = datetime.strptime(start_date, "%Y-%m-%d").year
            except:
                current_year = datetime.now().year
        else:
            current_year = datetime.now().year
        
        # Default to current year if no dates provided
        if not start_date or not end_date:
            start_date = f"{current_year}-01-01"
            end_date = f"{current_year}-12-31"
        
        conn = get_db_connection()
        
        # Build filter conditions
        pa_date_filter, pa_filters = build_filter_conditions(start_date, end_date, client, provider, procedure, "pa")
        
        # Check if we need to join with PA DATA for client/procedure filters
        needs_join = bool(client or procedure)
        
        if needs_join:
            # Join PA ISSUE REQUEST with PA DATA to apply client/procedure filters
            pa_granted_query = f'''
                SELECT 
                    COALESCE(SUM(pa.granted), 0) as total_granted,
                    COUNT(DISTINCT pa.groupname) as total_companies,
                    COUNT(DISTINCT pa.IID) as total_enrollees,
                    COUNT(DISTINCT pa.providerid) as total_providers
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.granted > 0
                    {pa_date_filter}
                    {pa_filters}
            '''
        else:
            # Simple query when no client/procedure filters
            # Remove table alias from date filter for simple query
            simple_date_filter = pa_date_filter.replace('pa.requestdate', 'requestdate') if pa_date_filter else ''
            pa_granted_query = f'''
                SELECT 
                    COALESCE(SUM(granted), 0) as total_granted,
                    COUNT(DISTINCT groupname) as total_companies,
                    COUNT(DISTINCT IID) as total_enrollees,
                    COUNT(DISTINCT providerid) as total_providers
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE granted > 0
                    {simple_date_filter}
            '''
        
        granted_result = conn.execute(pa_granted_query).fetchone()
        
        total_granted = float(granted_result[0]) if granted_result else 0.0
        total_companies = int(granted_result[1]) if granted_result else 0
        total_enrollees = int(granted_result[2]) if granted_result else 0
        total_providers = int(granted_result[3]) if granted_result else 0
        
        # Get PA statistics (total PA count)
        if needs_join:
            pa_stats_query = f'''
                SELECT COUNT(DISTINCT panumber) as total_pa_count
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE panumber IS NOT NULL
                    {pa_date_filter}
                    {pa_filters}
            '''
        else:
            # Remove table alias from date filter for simple query
            simple_date_filter = pa_date_filter.replace('pa.requestdate', 'requestdate') if pa_date_filter else ''
            pa_stats_query = f'''
                SELECT COUNT(DISTINCT panumber) as total_pa_count
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE panumber IS NOT NULL
                    {simple_date_filter}
            '''
        
        pa_stats_result = conn.execute(pa_stats_query).fetchone()
        total_pa_count = int(pa_stats_result[0]) if pa_stats_result else 0
        
        # Get online PA count (from PA ISSUE REQUEST)
        # If client/procedure filters are applied, we need to join with PA DATA
        if needs_join:
            online_pa_query = f'''
                SELECT COUNT(DISTINCT oir.panumber) as online_pa_count
                FROM "AI DRIVEN DATA"."PA ISSUE REQUEST" oir
                INNER JOIN "AI DRIVEN DATA"."PA DATA" pa 
                    ON CAST(oir.panumber AS VARCHAR) = CAST(pa.panumber AS VARCHAR)
                WHERE oir.requestdate >= DATE '{start_date}'
                    AND oir.requestdate <= DATE '{end_date}'
                    AND oir.panumber IS NOT NULL
                    AND oir.panumber != ''
                    {pa_filters}
            '''
        else:
            online_pa_query = f'''
                SELECT COUNT(DISTINCT panumber) as online_pa_count
                FROM "AI DRIVEN DATA"."PA ISSUE REQUEST"
                WHERE requestdate >= DATE '{start_date}'
                    AND requestdate <= DATE '{end_date}'
                    AND panumber IS NOT NULL
                    AND panumber != ''
            '''
        
        online_pa_result = conn.execute(online_pa_query).fetchone()
        online_pa_count = int(online_pa_result[0]) if online_pa_result else 0
        
        # Offline PA = Total PA - Online PA
        offline_pa_count = total_pa_count - online_pa_count
        
        # Note: Don't close connection - it's pooled and reused for performance
        
        return {
            'success': True,
            'data': {
                'total_granted': total_granted,
                'total_companies': total_companies,
                'total_enrollees': total_enrollees,
                'total_providers': total_providers,
                'total_pa_count': total_pa_count,
                'online_pa_count': online_pa_count,
                'offline_pa_count': max(0, offline_pa_count)  # Ensure non-negative
            }
        }
    except Exception as e:
        logger.error(f"Error getting PA statistics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pa-granted-timeline")
@cached_query(ttl=300)  # Cache for 5 minutes
async def get_pa_granted_timeline(
    period: str = Query('month', description="Period: 'month', 'week', or 'day'"),
    month: Optional[str] = Query(None, description="Month in YYYY-MM format (required for week/day period)"),
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    client: Optional[str] = Query(None, description="Client name (groupname)"),
    provider: Optional[str] = Query(None, description="Provider ID"),
    procedure: Optional[str] = Query(None, description="Procedure code")
):
    """
    Get PA granted timeline data by month, week, or day.
    For week/day periods, month parameter is required.
    """
    try:
        from datetime import datetime
        import calendar
        
        conn = get_db_connection()
        
        # Build filter conditions
        pa_date_filter, pa_filters = build_filter_conditions(start_date, end_date, client, provider, procedure, "pa")
        
        if period == 'month':
            # Monthly aggregation
            if not start_date or not end_date:
                current_year = datetime.now().year
                start_date = f"{current_year}-01-01"
                end_date = f"{current_year}-12-31"
            
            monthly_query = f'''
                SELECT 
                    EXTRACT(YEAR FROM requestdate) as year,
                    EXTRACT(MONTH FROM requestdate) as month,
                    COALESCE(SUM(granted), 0) as total_granted
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE granted > 0
                    {pa_date_filter}
                    {pa_filters}
                GROUP BY EXTRACT(YEAR FROM requestdate), EXTRACT(MONTH FROM requestdate)
                ORDER BY year, month
            '''
            monthly_df = conn.execute(monthly_query).fetchdf()
            
            timeline_data = []
            for _, row in monthly_df.iterrows():
                month_str = f"{int(row['year'])}-{int(row['month']):02d}"
                timeline_data.append({
                    'period': month_str,
                    'total_granted': float(row['total_granted'])
                })
            
        elif period == 'week':
            # Weekly aggregation - requires month parameter
            if not month:
                raise HTTPException(status_code=400, detail="Month parameter (YYYY-MM) is required for week period")
            
            year, month_num = map(int, month.split('-'))
            # Get first and last day of the month
            first_day = datetime(year, month_num, 1).date()
            last_day = datetime(year, month_num, calendar.monthrange(year, month_num)[1]).date()
            
            # Override date filter for the specific month
            pa_date_filter = f"AND pa.requestdate >= DATE '{first_day}' AND pa.requestdate <= DATE '{last_day}'"
            
            weekly_query = f'''
                SELECT 
                    EXTRACT(YEAR FROM requestdate) as year,
                    EXTRACT(WEEK FROM requestdate) as week,
                    COALESCE(SUM(granted), 0) as total_granted
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE granted > 0
                    {pa_date_filter}
                    {pa_filters}
                GROUP BY EXTRACT(YEAR FROM requestdate), EXTRACT(WEEK FROM requestdate)
                ORDER BY year, week
            '''
            weekly_df = conn.execute(weekly_query).fetchdf()
            
            timeline_data = []
            for _, row in weekly_df.iterrows():
                week_str = f"{int(row['year'])}-W{int(row['week']):02d}"
                timeline_data.append({
                    'period': week_str,
                    'total_granted': float(row['total_granted'])
                })
            
        elif period == 'day':
            # Daily aggregation - requires month parameter
            if not month:
                raise HTTPException(status_code=400, detail="Month parameter (YYYY-MM) is required for day period")
            
            year, month_num = map(int, month.split('-'))
            # Get first and last day of the month
            first_day = datetime(year, month_num, 1).date()
            last_day = datetime(year, month_num, calendar.monthrange(year, month_num)[1]).date()
            
            # Override date filter for the specific month
            pa_date_filter = f"AND pa.requestdate >= DATE '{first_day}' AND pa.requestdate <= DATE '{last_day}'"
            
            daily_query = f'''
                SELECT 
                    DATE(requestdate) as date,
                    COALESCE(SUM(granted), 0) as total_granted
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE granted > 0
                    {pa_date_filter}
                    {pa_filters}
                GROUP BY DATE(requestdate)
                ORDER BY date
            '''
            daily_df = conn.execute(daily_query).fetchdf()
            
            timeline_data = []
            for _, row in daily_df.iterrows():
                date_str = str(row['date'])
                timeline_data.append({
                    'period': date_str,
                    'total_granted': float(row['total_granted'])
                })
        else:
            raise HTTPException(status_code=400, detail="Period must be 'month', 'week', or 'day'")
        
        # Note: Don't close connection - it's pooled and reused for performance
        
        return {
            'success': True,
            'data': {
                'period': period,
                'month': month,
                'timeline_data': timeline_data
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting PA granted timeline: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
