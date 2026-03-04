"""
Providers Module API Routes
"""

from fastapi import APIRouter, HTTPException
from datetime import datetime, date
import logging

from core.database import get_db_connection

logger = logging.getLogger(__name__)
router = APIRouter()

# Exclude NHIA/NHIS from provider filters (reusable snippet)
_EXCLUDE_NHIA = """(p.providername IS NULL 
     OR (LOWER(TRIM(COALESCE(p.providername, ''))) NOT LIKE 'nhia%'
         AND LOWER(TRIM(COALESCE(p.providername, ''))) NOT LIKE 'nhis%'))"""


def _enrollee_id_col():
    """Enrollee identifier column in CLAIMS DATA (alias c)."""
    return "c.enrollee_id"


def _first_last_day_of_month(y, m):
    """First and last day of month (y, m) as (date, date)."""
    from calendar import monthrange
    last = monthrange(y, m)[1]
    return date(y, m, 1), date(y, m, last)


def _last_n_calendar_months(n):
    """Last n full calendar months as list of (year, month, start_date, end_date). Most recent month first."""
    from calendar import monthrange
    now = datetime.now()
    out = []
    y, m = now.year, now.month
    for _ in range(n):
        if m == 1:
            y, m = y - 1, 12
        else:
            m -= 1
        last = monthrange(y, m)[1]
        out.append((y, m, date(y, m, 1), date(y, m, last)))
    return out  # e.g. [(2025, 12, ...), (2025, 11, ...), (2025, 10, ...)] for n=3 in Jan 2026


def _norm_provider_name(raw, pid):
    """Resolve provider_name from DB; avoid None/NaN/empty. Fallback: 'Provider {id}'."""
    if raw is None:
        return 'Provider ' + str(pid)
    if isinstance(raw, float) and (raw != raw):  # NaN
        return 'Provider ' + str(pid)
    s = str(raw).strip()
    return s if s and s != 'nan' else ('Provider ' + str(pid))


def _provider_id_eq(left_col, right_col):
    """SQL: provider ID equality normalizing leading zeros (00089=89). Handles int/varchar; no COALESCE with '' to avoid INT32 conversion. Fallback to string compare when both non-numeric."""
    return f"""(
  TRY_CAST(TRIM(CAST({left_col} AS VARCHAR)) AS INTEGER) = TRY_CAST(TRIM(CAST({right_col} AS VARCHAR)) AS INTEGER)
) OR (
  TRY_CAST(TRIM(CAST({left_col} AS VARCHAR)) AS INTEGER) IS NULL
  AND TRY_CAST(TRIM(CAST({right_col} AS VARCHAR)) AS INTEGER) IS NULL
  AND TRIM(CAST({left_col} AS VARCHAR)) = TRIM(CAST({right_col} AS VARCHAR))
)"""


def _provider_id_eq_literal(col, escaped_literal):
    """SQL: provider ID column equals literal, normalizing so 123 / '123' / '0123' match. escaped_literal must already have quotes escaped."""
    return f"""(
  TRY_CAST(TRIM(CAST({col} AS VARCHAR)) AS INTEGER) = TRY_CAST(TRIM('{escaped_literal}') AS INTEGER)
) OR (
  TRY_CAST(TRIM(CAST({col} AS VARCHAR)) AS INTEGER) IS NULL
  AND TRY_CAST(TRIM('{escaped_literal}') AS INTEGER) IS NULL
  AND TRIM(CAST({col} AS VARCHAR)) = TRIM('{escaped_literal}')
)"""


def calculate_providers_dashboard_basic():
    """
    Core providers dashboard computation, analogous to calculate_mlr_basic and
    calculate_client_summary_basic.

    Returns a dict with:
    - total_providers
    - providers_by_band: list of bands with provider_count, total_pa_cost, total_claims_cost, new_providers_this_month

    This is the single place to change when we later move to a nightly batch job
    writing into a PROVIDERS_DASHBOARD_SUMMARY table.
    """
    conn = get_db_connection()
    try:
        # 1) Total providers (excluding NHIA/NHIS)
        total_q = """
            SELECT COUNT(*) as total_count
            FROM "AI DRIVEN DATA"."PROVIDERS"
            WHERE LOWER(TRIM(COALESCE(providername, ''))) NOT LIKE '%nhia%'
              AND LOWER(TRIM(COALESCE(providername, ''))) NOT LIKE '%nhis%'
        """
        row = conn.execute(total_q).fetchone()
        total_providers = int(row[0] or 0) if row else 0

        # 2) Providers by band with aggregated PA and claims cost (current year)
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        year_start = f"{current_year}-01-01"
        year_end = f"{current_year}-12-31"
        month_start_date, month_end_date = _first_last_day_of_month(current_year, current_month)
        month_start = month_start_date.isoformat()
        month_end = month_end_date.isoformat()

        bands_q = f"""
            WITH base_providers AS (
                SELECT
                    p.providerid,
                    COALESCE(NULLIF(TRIM(p.bands), ''), 'Unspecified') AS band,
                    DATE(p.dateadded) AS dateadded
                FROM "AI DRIVEN DATA"."PROVIDERS" p
                WHERE LOWER(TRIM(COALESCE(p.providername, ''))) NOT LIKE '%nhia%'
                  AND LOWER(TRIM(COALESCE(p.providername, ''))) NOT LIKE '%nhis%'
            ),
            pa_by_provider AS (
                SELECT
                    CAST(pa.providerid AS VARCHAR) AS providerid,
                    SUM(pa.granted) AS pa_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.granted > 0
                  AND pa.requestdate >= DATE '{year_start}' AND pa.requestdate <= DATE '{year_end}'
                GROUP BY CAST(pa.providerid AS VARCHAR)
            ),
            claims_by_provider AS (
                SELECT
                    CAST(c.nhisproviderid AS VARCHAR) AS providerid,
                    SUM(c.approvedamount) AS claims_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE c.approvedamount > 0
                  AND COALESCE(c.encounterdatefrom, c.datesubmitted) >= DATE '{year_start}'
                  AND COALESCE(c.encounterdatefrom, c.datesubmitted) <= DATE '{year_end}'
                GROUP BY CAST(c.nhisproviderid AS VARCHAR)
            )
            SELECT
                bp.band,
                COUNT(*) AS provider_count,
                COALESCE(SUM(pb.pa_cost), 0) AS total_pa_cost,
                COALESCE(SUM(cb.claims_cost), 0) AS total_claims_cost,
                SUM(CASE 
                        WHEN bp.dateadded IS NOT NULL 
                             AND bp.dateadded >= DATE '{month_start}' 
                             AND bp.dateadded <= DATE '{month_end}'
                        THEN 1 ELSE 0 
                    END) AS new_providers_this_month
            FROM base_providers bp
            LEFT JOIN pa_by_provider pb
              ON ({_provider_id_eq('bp.providerid', 'pb.providerid')})
            LEFT JOIN claims_by_provider cb
              ON ({_provider_id_eq('bp.providerid', 'cb.providerid')})
            GROUP BY bp.band
            ORDER BY bp.band
        """
        band_rows = conn.execute(bands_q).fetchall()
        providers_by_band = [
            {
                "band": str(r[0] or "Unspecified"),
                "provider_count": int(r[1] or 0),
                "total_pa_cost": float(r[2] or 0),
                "total_claims_cost": float(r[3] or 0),
                "new_providers_this_month": int(r[4] or 0),
            }
            for r in band_rows
        ]

        return {
            "total_providers": total_providers,
            "providers_by_band": providers_by_band,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/dashboard")
async def get_providers_dashboard():
    """
    Get providers dashboard:
    - Total count of providers excluding names containing 'NHIA' or 'NHIS' (case insensitive)
    - Per band/category:
        * Provider count
        * Total PA cost (sum of granted amounts, current year)
        * Total claims cost (sum of approvedamount, current year)

    Internally this uses calculate_providers_dashboard_basic(), which is the same
    core summary that a future nightly batch job can write into a summary table.
    """
    try:
        data = calculate_providers_dashboard_basic()
        return {
            "success": True,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching providers dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/new-by-band")
async def get_new_providers_by_band(start_date: str, end_date: str):
    """
    Table of bands vs number of providers added between start_date and end_date.
    - Dates affect ONLY this table, not the main dashboard cards.
    - Excludes providers whose name starts with NHIA/NHIS.
    """
    try:
        conn = get_db_connection()

        # Validate/normalize dates – DuckDB will error if invalid; we just pass through.
        start = start_date
        end = end_date

        q = f"""
            WITH base_providers AS (
                SELECT
                    COALESCE(NULLIF(TRIM(p.bands), ''), 'Unspecified') AS band,
                    DATE(p.dateadded) AS dateadded
                FROM "AI DRIVEN DATA"."PROVIDERS" p
                WHERE LOWER(TRIM(COALESCE(p.providername, ''))) NOT LIKE '%nhia%'
                  AND LOWER(TRIM(COALESCE(p.providername, ''))) NOT LIKE '%nhis%'
            )
            SELECT
                band,
                SUM(CASE 
                        WHEN dateadded IS NOT NULL 
                             AND dateadded >= DATE '{start}'
                             AND dateadded <= DATE '{end}'
                        THEN 1 ELSE 0 
                    END) AS new_providers
            FROM base_providers
            GROUP BY band
            ORDER BY band
        """

        rows = conn.execute(q).fetchall()
        data = [
            {
                "band": str(r[0] or "Unspecified"),
                "new_providers": int(r[1] or 0),
            }
            for r in rows
        ]

        return {
            "success": True,
            "data": data,
            "start_date": start,
            "end_date": end,
        }
    except Exception as e:
        logger.error(f"Error fetching new providers by band: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top")
async def get_top_providers():
    """
    Top providers (current year):
    - top_pa_by_cost: top 10 providers by total PA granted
    - top_pa_by_visits: top 10 providers by unique PA enrollee visits
    - top_claims_by_cost: top 10 providers by total claims approvedamount
    All excluding NHIA/NHIS providers.
    """
    try:
        conn = get_db_connection()
        now = datetime.now()
        current_year = now.year
        year_start = f"{current_year}-01-01"
        year_end = f"{current_year}-12-31"

        # 1) Top 10 providers by PA cost (granted)
        pa_cost_q = f"""
            WITH pa_agg AS (
                SELECT 
                    CAST(pa.providerid AS VARCHAR) AS providerid,
                    SUM(pa.granted) AS total_pa_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.granted > 0
                  AND pa.requestdate >= DATE '{year_start}' AND pa.requestdate <= DATE '{year_end}'
                GROUP BY CAST(pa.providerid AS VARCHAR)
            )
            SELECT 
                TRIM(COALESCE(p.providername, 'Provider ' || pa.providerid)) AS provider_name,
                COALESCE(p.bands, '') AS band,
                pa.total_pa_cost
            FROM pa_agg pa
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
              ON ({_provider_id_eq('pa.providerid', 'p.providerid')})
            WHERE {_EXCLUDE_NHIA}
            ORDER BY pa.total_pa_cost DESC
            LIMIT 10
        """
        pa_cost_rows = conn.execute(pa_cost_q).fetchall()
        top_pa_by_cost = [
            {
                "provider_name": str(r[0] or ""),
                "band": str(r[1] or ""),
                "total_pa_cost": float(r[2] or 0),
            }
            for r in pa_cost_rows
        ]

        # 2) Top 10 providers by unique PA enrollee visits
        pa_visits_q = f"""
            WITH pa_visits AS (
                SELECT 
                    CAST(pa.providerid AS VARCHAR) AS providerid,
                    COUNT(DISTINCT (TRIM(COALESCE(pa.IID, '')) || '|' || CAST(DATE(pa.requestdate) AS VARCHAR))) AS visits
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.requestdate >= DATE '{year_start}' AND pa.requestdate <= DATE '{year_end}'
                  AND pa.IID IS NOT NULL AND TRIM(pa.IID) <> ''
                GROUP BY CAST(pa.providerid AS VARCHAR)
            )
            SELECT 
                TRIM(COALESCE(p.providername, 'Provider ' || v.providerid)) AS provider_name,
                COALESCE(p.bands, '') AS band,
                v.visits AS unique_visits
            FROM pa_visits v
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
              ON ({_provider_id_eq('v.providerid', 'p.providerid')})
            WHERE {_EXCLUDE_NHIA}
            ORDER BY v.visits DESC
            LIMIT 10
        """
        pa_visits_rows = conn.execute(pa_visits_q).fetchall()
        top_pa_by_visits = [
            {
                "provider_name": str(r[0] or ""),
                "band": str(r[1] or ""),
                "unique_visits": int(r[2] or 0),
            }
            for r in pa_visits_rows
        ]

        # 3) Top 10 providers by claims cost
        claims_cost_q = f"""
            WITH claims_agg AS (
                SELECT 
                    CAST(c.nhisproviderid AS VARCHAR) AS providerid,
                    SUM(c.approvedamount) AS total_claims_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE c.approvedamount > 0
                  AND c.datesubmitted >= DATE '{year_start}'
                  AND c.datesubmitted <= DATE '{year_end}'
                GROUP BY CAST(c.nhisproviderid AS VARCHAR)
            )
            SELECT 
                TRIM(COALESCE(p.providername, 'Provider ' || ca.providerid)) AS provider_name,
                COALESCE(p.bands, '') AS band,
                ca.total_claims_cost
            FROM claims_agg ca
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
              ON ({_provider_id_eq('ca.providerid', 'p.providerid')})
            WHERE {_EXCLUDE_NHIA}
            ORDER BY ca.total_claims_cost DESC
            LIMIT 10
        """
        claims_cost_rows = conn.execute(claims_cost_q).fetchall()
        top_claims_by_cost = [
            {
                "provider_name": str(r[0] or ""),
                "band": str(r[1] or ""),
                "total_claims_cost": float(r[2] or 0),
            }
            for r in claims_cost_rows
        ]

        return {
            "success": True,
            "year_start": year_start,
            "year_end": year_end,
            "data": {
                "top_pa_by_cost": top_pa_by_cost,
                "top_pa_by_visits": top_pa_by_visits,
                "top_claims_by_cost": top_claims_by_cost,
            },
        }
    except Exception as e:
        logger.error(f"Error fetching top providers: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-cer")
async def get_top_cer():
    """
    Top 10 providers by Claims per Enrollee Ratio (CER) for the last 3 calendar months.
    CER = total claims / total unique enrollees (per provider per month). Uses datesubmitted.
    Returns: provider_name, month1_cer, month2_cer, month3_cer, average_cer (5 columns).
    Only fetches top 10 in SQL to avoid timeout. Excludes NHIA/NHIS.
    """
    try:
        conn = get_db_connection()
        eid = _enrollee_id_col()
        months = _last_n_calendar_months(3)  # [(y3,m3,s3,e3), (y2,m2,s2,e2), (y1,m1,s1,e1)] most recent first
        # Build month bounds for SQL: m_num 1 = oldest, m_num 3 = most recent
        m1_start, m1_end = months[2][2], months[2][3]
        m2_start, m2_end = months[1][2], months[1][3]
        m3_start, m3_end = months[0][2], months[0][3]
        month_labels = [
            f"{months[2][1]:02d}/{months[2][0]}",
            f"{months[1][1]:02d}/{months[1][0]}",
            f"{months[0][1]:02d}/{months[0][0]}",
        ]

        cer_q = f"""
            WITH month_bounds AS (
                SELECT 1 AS m_num, DATE '{m1_start}' AS start_d, DATE '{m1_end}' AS end_d
                UNION ALL SELECT 2, DATE '{m2_start}', DATE '{m2_end}'
                UNION ALL SELECT 3, DATE '{m3_start}', DATE '{m3_end}'
            ),
            per_provider_month AS (
                SELECT
                    CAST(c.nhisproviderid AS VARCHAR) AS providerid,
                    mb.m_num,
                    COUNT(*) AS claim_count,
                    COUNT(DISTINCT {eid}) AS enrollee_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                CROSS JOIN month_bounds mb
                WHERE c.datesubmitted >= mb.start_d AND c.datesubmitted <= mb.end_d
                  AND c.nhisproviderid IS NOT NULL
                GROUP BY CAST(c.nhisproviderid AS VARCHAR), mb.m_num
            ),
            cer_per_provider_month AS (
                SELECT
                    providerid,
                    m_num,
                    CASE WHEN enrollee_count > 0 THEN claim_count * 1.0 / enrollee_count ELSE 0 END AS cer
                FROM per_provider_month
            ),
            provider_avg_cer AS (
                SELECT
                    cpm.providerid,
                    AVG(cpm.cer) AS avg_cer
                FROM cer_per_provider_month cpm
                INNER JOIN "AI DRIVEN DATA"."PROVIDERS" p
                  ON ({_provider_id_eq('cpm.providerid', 'p.providerid')})
                WHERE {_EXCLUDE_NHIA}
                GROUP BY cpm.providerid
            ),
            top10_providers AS (
                SELECT providerid
                FROM provider_avg_cer
                ORDER BY avg_cer DESC
                LIMIT 10
            ),
            pivoted AS (
                SELECT
                    cpm.providerid,
                    MAX(CASE WHEN cpm.m_num = 1 THEN cpm.cer END) AS m1_cer,
                    MAX(CASE WHEN cpm.m_num = 2 THEN cpm.cer END) AS m2_cer,
                    MAX(CASE WHEN cpm.m_num = 3 THEN cpm.cer END) AS m3_cer,
                    pac.avg_cer
                FROM cer_per_provider_month cpm
                INNER JOIN provider_avg_cer pac ON cpm.providerid = pac.providerid
                INNER JOIN top10_providers t10 ON cpm.providerid = t10.providerid
                GROUP BY cpm.providerid, pac.avg_cer
            )
            SELECT
                TRIM(COALESCE(p.providername, 'Provider ' || piv.providerid)) AS provider_name,
                piv.m1_cer,
                piv.m2_cer,
                piv.m3_cer,
                piv.avg_cer
            FROM pivoted piv
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
              ON ({_provider_id_eq('piv.providerid', 'p.providerid')})
            ORDER BY piv.avg_cer DESC
        """
        rows = conn.execute(cer_q).fetchall()
        data = [
            {
                "provider_name": str(r[0] or ""),
                "month1_cer": float(r[1]) if r[1] is not None else None,
                "month2_cer": float(r[2]) if r[2] is not None else None,
                "month3_cer": float(r[3]) if r[3] is not None else None,
                "average_cer": float(r[4] or 0),
            }
            for r in rows
        ]
        return {
            "success": True,
            "month_labels": month_labels,
            "data": data,
        }
    except Exception as e:
        logger.error(f"Error fetching top CER: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-cvr")
async def get_top_cvr():
    """
    Top 10 providers by Claims per Visit Ratio (CVR) for the last 3 calendar months.
    CVR = total claims / total unique visits (one visit = one panumber). Uses datesubmitted.
    Returns: provider_name, month1_cvr, month2_cvr, month3_cvr, average_cvr (5 columns).
    Only fetches top 10 in SQL to avoid timeout. Excludes NHIA/NHIS.
    >3 = unbundling or line-item abuse.
    """
    try:
        conn = get_db_connection()
        months = _last_n_calendar_months(3)
        m1_start, m1_end = months[2][2], months[2][3]
        m2_start, m2_end = months[1][2], months[1][3]
        m3_start, m3_end = months[0][2], months[0][3]
        month_labels = [
            f"{months[2][1]:02d}/{months[2][0]}",
            f"{months[1][1]:02d}/{months[1][0]}",
            f"{months[0][1]:02d}/{months[0][0]}",
        ]

        cvr_q = f"""
            WITH month_bounds AS (
                SELECT 1 AS m_num, DATE '{m1_start}' AS start_d, DATE '{m1_end}' AS end_d
                UNION ALL SELECT 2, DATE '{m2_start}', DATE '{m2_end}'
                UNION ALL SELECT 3, DATE '{m3_start}', DATE '{m3_end}'
            ),
            per_provider_month AS (
                SELECT
                    CAST(c.nhisproviderid AS VARCHAR) AS providerid,
                    mb.m_num,
                    COUNT(*) AS claim_count,
                    COUNT(DISTINCT c.panumber) AS visit_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                CROSS JOIN month_bounds mb
                WHERE c.datesubmitted >= mb.start_d AND c.datesubmitted <= mb.end_d
                  AND c.nhisproviderid IS NOT NULL
                  AND c.panumber IS NOT NULL
                GROUP BY CAST(c.nhisproviderid AS VARCHAR), mb.m_num
            ),
            cvr_per_provider_month AS (
                SELECT
                    providerid,
                    m_num,
                    CASE WHEN visit_count > 0 THEN claim_count * 1.0 / visit_count ELSE 0 END AS cvr
                FROM per_provider_month
            ),
            provider_avg_cvr AS (
                SELECT
                    cpm.providerid,
                    AVG(cpm.cvr) AS avg_cvr
                FROM cvr_per_provider_month cpm
                INNER JOIN "AI DRIVEN DATA"."PROVIDERS" p
                  ON ({_provider_id_eq('cpm.providerid', 'p.providerid')})
                WHERE {_EXCLUDE_NHIA}
                GROUP BY cpm.providerid
            ),
            top10_providers AS (
                SELECT providerid
                FROM provider_avg_cvr
                ORDER BY avg_cvr DESC
                LIMIT 10
            ),
            pivoted AS (
                SELECT
                    cpm.providerid,
                    MAX(CASE WHEN cpm.m_num = 1 THEN cpm.cvr END) AS m1_cvr,
                    MAX(CASE WHEN cpm.m_num = 2 THEN cpm.cvr END) AS m2_cvr,
                    MAX(CASE WHEN cpm.m_num = 3 THEN cpm.cvr END) AS m3_cvr,
                    pac.avg_cvr
                FROM cvr_per_provider_month cpm
                INNER JOIN provider_avg_cvr pac ON cpm.providerid = pac.providerid
                INNER JOIN top10_providers t10 ON cpm.providerid = t10.providerid
                GROUP BY cpm.providerid, pac.avg_cvr
            )
            SELECT
                TRIM(COALESCE(p.providername, 'Provider ' || piv.providerid)) AS provider_name,
                piv.m1_cvr,
                piv.m2_cvr,
                piv.m3_cvr,
                piv.avg_cvr
            FROM pivoted piv
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
              ON ({_provider_id_eq('piv.providerid', 'p.providerid')})
            ORDER BY piv.avg_cvr DESC
        """
        rows = conn.execute(cvr_q).fetchall()
        data = [
            {
                "provider_name": str(r[0] or ""),
                "month1_cvr": float(r[1]) if r[1] is not None else None,
                "month2_cvr": float(r[2]) if r[2] is not None else None,
                "month3_cvr": float(r[3]) if r[3] is not None else None,
                "average_cvr": float(r[4] or 0),
            }
            for r in rows
        ]
        return {
            "success": True,
            "month_labels": month_labels,
            "data": data,
        }
    except Exception as e:
        logger.error(f"Error fetching top CVR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def get_providers_list():
    """List provider id and name for selector: distinct from claims, excluding NHIA/NHIS.
    Uses distinct-providers subquery then one JOIN to avoid scanning full CLAIMS with two joins."""
    try:
        conn = get_db_connection()
        q = f"""
            WITH distinct_providers AS (
                SELECT DISTINCT nhisproviderid
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE nhisproviderid IS NOT NULL
            )
            SELECT
                CAST(d.nhisproviderid AS VARCHAR) as provider_id,
                COALESCE(TRIM(p.providername), 'Provider ' || CAST(d.nhisproviderid AS VARCHAR)) as provider_name
            FROM distinct_providers d
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON ({_provider_id_eq('d.nhisproviderid', 'p.providerid')})
            WHERE {_EXCLUDE_NHIA}
            ORDER BY provider_name
        """
        df = conn.execute(q).fetchdf()
        out = [{'provider_id': str(r['provider_id']), 'provider_name': _norm_provider_name(r.get('provider_name'), str(r['provider_id']))} for _, r in df.iterrows()]
        return {'success': True, 'data': out}
    except Exception as e:
        logger.error(f"Error fetching providers list: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detail/{provider_id}")
async def get_provider_detail(provider_id: str):
    """
    Full analysis for a selected provider (current year unless noted):
    total_medical_cost, total_enrollee_visits, top5_diagnosis_by_cost, top5_procedure_by_cost,
    top5_procedure_by_count, top5_diagnosis_by_count, monthly_claims_enrollee_ratio,
    state, lga, top5_clients_by_cost, top5_clients_by_visits, band.
    """
    try:
        conn = get_db_connection()
        current_year = datetime.now().year
        year_start = f"{current_year}-01-01"
        year_end = f"{current_year}-12-31"
        eid = _enrollee_id_col()
        sid = provider_id.replace("'", "''")

        # Provider basics: name, state, lga, band
        basics_q = f"""
            SELECT TRIM(COALESCE(p.providername, '')), TRIM(COALESCE(p.statename, '')), TRIM(COALESCE(p.lganame, '')), COALESCE(p.bands, '')
            FROM "AI DRIVEN DATA"."PROVIDERS" p
            WHERE {_provider_id_eq_literal('p.providerid', sid)}
            LIMIT 1
        """
        basics = conn.execute(basics_q).fetchone()
        if not basics:
            provider_name, state, lga, band = 'Provider ' + provider_id, 'N/A', 'N/A', 'N/A'
        else:
            provider_name = str(basics[0] or '').strip() or ('Provider ' + provider_id)
            state = str(basics[1] or '').strip() or 'N/A'
            lga = str(basics[2] or '').strip() or 'N/A'
            band = str(basics[3] or '').strip() or 'N/A'

        # Total medical cost (claims by encounter + unclaimed PA) and enrollee visits (claims visits + unclaimed PA visits).
        # Also total_claim_lines (COUNT(*)) for cost_per_claim_line (claims-only).
        claims_cost_visits_q = f"""
            SELECT 
                SUM(c.approvedamount) as claims_cost,
                COUNT(DISTINCT ({eid} || '|' || CAST(DATE(COALESCE(c.encounterdatefrom, c.datesubmitted)) AS VARCHAR))) as claims_visits,
                COUNT(*) as total_claim_lines
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                AND c.approvedamount > 0
                AND COALESCE(c.encounterdatefrom, c.datesubmitted) >= DATE '{year_start}' AND COALESCE(c.encounterdatefrom, c.datesubmitted) <= DATE '{year_end}'
                AND {eid} IS NOT NULL
        """
        cv = conn.execute(claims_cost_visits_q).fetchone()
        claims_cost = float(cv[0] or 0)
        claims_visits = int(cv[1] or 0)
        total_claim_lines = int(cv[2] or 0)

        unclaimed_pa_detail_q = f"""
            WITH pa_issued AS (
                SELECT CAST(pa.panumber AS BIGINT) as pnum, SUM(pa.granted) as g
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE ({_provider_id_eq_literal('pa.providerid', sid)})
                    AND pa.panumber IS NOT NULL AND pa.granted > 0
                    AND pa.requestdate >= DATE '{year_start}' AND pa.requestdate <= DATE '{year_end}'
                GROUP BY CAST(pa.panumber AS BIGINT)
            ),
            claims_filed AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as pnum FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE panumber IS NOT NULL AND datesubmitted >= DATE '{year_start}' AND datesubmitted <= DATE '{year_end}'
            )
            SELECT COALESCE(SUM(pa.g), 0) as ucost, COUNT(*) as uvisits
            FROM pa_issued pa
            LEFT JOIN claims_filed c ON pa.pnum = c.pnum
            WHERE c.pnum IS NULL
        """
        u = conn.execute(unclaimed_pa_detail_q).fetchone()
        unclaimed_cost = float(u[0] or 0)
        unclaimed_visits = int(u[1] or 0)
        total_medical_cost = claims_cost + unclaimed_cost
        total_enrollee_visits = claims_visits + unclaimed_visits

        # Top 5 diagnosis by cost
        dx_cost_q = f"""
            SELECT COALESCE(d.diagnosisdesc, c.diagnosiscode, 'Unknown') as name, c.diagnosiscode as code,
                   SUM(c.approvedamount) as total_cost
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON c.diagnosiscode = d.diagnosiscode
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                AND c.datesubmitted >= DATE '{year_start}' AND c.datesubmitted <= DATE '{year_end}'
                AND c.approvedamount > 0
            GROUP BY c.diagnosiscode, d.diagnosisdesc
            ORDER BY total_cost DESC
            LIMIT 5
        """
        dx_cost = [{'code': str(r[1] or ''), 'name': str(r[0] or ''), 'total_cost': float(r[2] or 0)} for r in conn.execute(dx_cost_q).fetchall()]

        # Top 5 procedure by cost
        proc_cost_q = f"""
            SELECT COALESCE(pd.proceduredesc, c.code, 'Unknown') as name, c.code as proc_code,
                   SUM(c.approvedamount) as total_cost
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON c.code = pd.procedurecode
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                AND c.datesubmitted >= DATE '{year_start}' AND c.datesubmitted <= DATE '{year_end}'
                AND c.approvedamount > 0
            GROUP BY c.code, pd.proceduredesc
            ORDER BY total_cost DESC
            LIMIT 5
        """
        proc_cost = [{'code': str(r[1] or ''), 'name': str(r[0] or ''), 'total_cost': float(r[2] or 0)} for r in conn.execute(proc_cost_q).fetchall()]

        # Top 5 procedure by count
        proc_count_q = f"""
            SELECT COALESCE(pd.proceduredesc, c.code, 'Unknown') as name, c.code as proc_code, COUNT(*) as cnt
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON c.code = pd.procedurecode
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                AND c.datesubmitted >= DATE '{year_start}' AND c.datesubmitted <= DATE '{year_end}'
            GROUP BY c.code, pd.proceduredesc
            ORDER BY cnt DESC
            LIMIT 5
        """
        proc_count = [{'code': str(r[1] or ''), 'name': str(r[0] or ''), 'count': int(r[2] or 0)} for r in conn.execute(proc_count_q).fetchall()]

        # Top 5 diagnosis by count
        dx_count_q = f"""
            SELECT COALESCE(d.diagnosisdesc, c.diagnosiscode, 'Unknown') as name, c.diagnosiscode as code, COUNT(*) as cnt
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON c.diagnosiscode = d.diagnosiscode
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                AND c.datesubmitted >= DATE '{year_start}' AND c.datesubmitted <= DATE '{year_end}'
            GROUP BY c.diagnosiscode, d.diagnosisdesc
            ORDER BY cnt DESC
            LIMIT 5
        """
        dx_count = [{'code': str(r[1] or ''), 'name': str(r[0] or ''), 'count': int(r[2] or 0)} for r in conn.execute(dx_count_q).fetchall()]

        # Month-by-month Claims per Visit (YTD). Visits = distinct (enrollee, visit_date); visit_date = DATE(COALESCE(encounterdatefrom, datesubmitted)).
        monthly_cpv_q = f"""
            SELECT 
                EXTRACT(MONTH FROM COALESCE(c.encounterdatefrom, c.datesubmitted)) as m,
                EXTRACT(YEAR FROM COALESCE(c.encounterdatefrom, c.datesubmitted)) as y,
                COUNT(*) as claim_lines,
                COUNT(DISTINCT ({eid} || '|' || CAST(DATE(COALESCE(c.encounterdatefrom, c.datesubmitted)) AS VARCHAR))) as visits
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                AND COALESCE(c.encounterdatefrom, c.datesubmitted) >= DATE '{year_start}'
                AND COALESCE(c.encounterdatefrom, c.datesubmitted) <= DATE '{year_end}'
                AND c.approvedamount > 0
                AND {eid} IS NOT NULL
                AND (c.encounterdatefrom IS NOT NULL OR c.datesubmitted IS NOT NULL)
            GROUP BY EXTRACT(YEAR FROM COALESCE(c.encounterdatefrom, c.datesubmitted)), EXTRACT(MONTH FROM COALESCE(c.encounterdatefrom, c.datesubmitted))
            ORDER BY y, m
        """
        _cpv_band = lambda v: 'Normal' if v < 3 else ('Watchlist' if v < 4 else 'Red flag')
        rows = conn.execute(monthly_cpv_q).fetchall()
        monthly_claims_per_visit = []
        for r in rows:
            vis = int(r[3] or 0)
            cpv = round((r[2] / vis) if vis > 0 else 0, 4)
            monthly_claims_per_visit.append({
                'month': int(r[0]),
                'year': int(r[1]),
                'claim_lines': int(r[2]),
                'visits': vis,
                'claims_per_visit': cpv,
                'interpretation': _cpv_band(cpv),
            })

        # Top 5 clients by total medical cost (current year)
        # GROUP_CONTRACT: groupid, groupname. CLAIMS: nhisgroupid. Join on groupid. GROUP_CONTRACT may not have all groupids; we can also try a GROUPS table. Use GROUP_CONTRACT.
        clients_cost_q = f"""
            SELECT COALESCE(gc.groupname, CAST(c.nhisgroupid AS VARCHAR)) as client_name, SUM(c.approvedamount) as total_cost
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."GROUP_CONTRACT" gc ON CAST(c.nhisgroupid AS VARCHAR) = CAST(gc.groupid AS VARCHAR) AND gc.iscurrent = 1
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                AND c.datesubmitted >= DATE '{year_start}' AND c.datesubmitted <= DATE '{year_end}'
                AND c.approvedamount > 0
            GROUP BY gc.groupname, c.nhisgroupid
            ORDER BY total_cost DESC
            LIMIT 5
        """
        clients_cost = [{'client_name': str(r[0] or ''), 'total_cost': float(r[1] or 0)} for r in conn.execute(clients_cost_q).fetchall()]

        # Top 5 clients by unique visits (current year)
        clients_visits_q = f"""
            WITH v AS (
                SELECT c.nhisgroupid, {eid} as e, DATE(COALESCE(c.encounterdatefrom, c.datesubmitted)) as d
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                    AND c.datesubmitted >= DATE '{year_start}' AND c.datesubmitted <= DATE '{year_end}'
                    AND {eid} IS NOT NULL AND (c.encounterdatefrom IS NOT NULL OR c.datesubmitted IS NOT NULL)
                GROUP BY c.nhisgroupid, {eid}, DATE(COALESCE(c.encounterdatefrom, c.datesubmitted))
            )
            SELECT COALESCE(gc.groupname, CAST(v.nhisgroupid AS VARCHAR)) as client_name, COUNT(*) as unique_visits
            FROM v
            LEFT JOIN "AI DRIVEN DATA"."GROUP_CONTRACT" gc ON CAST(v.nhisgroupid AS VARCHAR) = CAST(gc.groupid AS VARCHAR) AND gc.iscurrent = 1
            GROUP BY gc.groupname, v.nhisgroupid
            ORDER BY unique_visits DESC
            LIMIT 5
        """
        clients_visits = [{'client_name': str(r[0] or ''), 'unique_visits': int(r[1] or 0)} for r in conn.execute(clients_visits_q).fetchall()]

        # Cost per Visit and Cost per Claim Line (claims-only; for fraud/cost abuse view)
        cost_per_visit = round(claims_cost / claims_visits, 2) if claims_visits and claims_visits > 0 else None
        cost_per_claim_line = round(claims_cost / total_claim_lines, 2) if total_claim_lines and total_claim_lines > 0 else None

        return {
            'success': True,
            'data': {
                'provider_id': provider_id,
                'provider_name': provider_name,
                'state': state,
                'lga': lga,
                'band': band,
                'total_medical_cost': total_medical_cost,
                'total_enrollee_visits': total_enrollee_visits,
                'cost_per_visit': cost_per_visit,
                'cost_per_claim_line': cost_per_claim_line,
                'top5_diagnosis_by_cost': dx_cost,
                'top5_procedure_by_cost': proc_cost,
                'top5_procedure_by_count': proc_count,
                'top5_diagnosis_by_count': dx_count,
                'monthly_claims_per_visit': monthly_claims_per_visit,
                'top5_clients_by_cost': clients_cost,
                'top5_clients_by_visits': clients_visits,
            },
        }
    except Exception as e:
        logger.error(f"Error fetching provider detail: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/{provider_id}")
async def get_provider_analysis(provider_id: str):
    """
    Provider analysis (like client analysis): metrics and last-6-months charts.
    - total_pa_granted_this_year, total_claimed_pa_cost, total_unclaimed, total_claims_this_year_datesubmitted
    - panumber_count_by_month_last_6, claims_cost_by_month_last_6 (line chart data)
    - top5_companies_by_cost_last_6, top5_companies_by_enrollee_count_last_6
    All claim/PA filters by provider use nhisproviderid/providerid match. Last 6 months use datesubmitted.
    """
    try:
        conn = get_db_connection()
        current_year = datetime.now().year
        year_start = f"{current_year}-01-01"
        year_end = f"{current_year}-12-31"
        sid = provider_id.replace("'", "''")
        # Last 6 full calendar months (for PA numbers & claims cost charts); helper returns most recent month first.
        months = _last_n_calendar_months(6)
        # For 6‑month window, use the oldest month start and most recent month end.
        m_start = months[-1][2].isoformat()
        m_end = months[0][3].isoformat()
        eid = _enrollee_id_col()

        # 1) Total PA granted this year (this provider)
        pa_granted_q = f"""
            SELECT COALESCE(SUM(pa.granted), 0)
            FROM "AI DRIVEN DATA"."PA DATA" pa
            WHERE ({_provider_id_eq_literal('pa.providerid', sid)})
              AND pa.requestdate >= DATE '{year_start}' AND pa.requestdate <= DATE '{year_end}'
              AND pa.granted IS NOT NULL
        """
        total_pa_granted_this_year = float(conn.execute(pa_granted_q).fetchone()[0] or 0)

        # 2) Total claimed PA cost = PA granted this year whose panumber appears in claims (so <= total PA granted)
        claimed_pa_q = f"""
            WITH pa_issued AS (
                SELECT CAST(pa.panumber AS BIGINT) AS pnum, SUM(pa.granted) AS g
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE ({_provider_id_eq_literal('pa.providerid', sid)})
                  AND pa.panumber IS NOT NULL AND pa.granted > 0
                  AND pa.requestdate >= DATE '{year_start}' AND pa.requestdate <= DATE '{year_end}'
                GROUP BY CAST(pa.panumber AS BIGINT)
            ),
            claims_filed AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) AS pnum FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE panumber IS NOT NULL AND datesubmitted >= DATE '{year_start}' AND datesubmitted <= DATE '{year_end}'
            )
            SELECT COALESCE(SUM(pa.g), 0) FROM pa_issued pa
            INNER JOIN claims_filed c ON pa.pnum = c.pnum
        """
        total_claimed_pa_cost = float(conn.execute(claimed_pa_q).fetchone()[0] or 0)

        # Total claims this year (datesubmitted) — for reference only
        claims_yr_q = f"""
            SELECT COALESCE(SUM(c.approvedamount), 0)
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
              AND c.datesubmitted >= DATE '{year_start}' AND c.datesubmitted <= DATE '{year_end}'
        """
        total_claims_this_year_datesubmitted = float(conn.execute(claims_yr_q).fetchone()[0] or 0)

        # 3) Total unclaimed (PA granted by provider where panumber not in claims)
        unclaimed_q = f"""
            WITH pa_issued AS (
                SELECT CAST(pa.panumber AS BIGINT) AS pnum, SUM(pa.granted) AS g
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE ({_provider_id_eq_literal('pa.providerid', sid)})
                  AND pa.panumber IS NOT NULL AND pa.granted > 0
                  AND pa.requestdate >= DATE '{year_start}' AND pa.requestdate <= DATE '{year_end}'
                GROUP BY CAST(pa.panumber AS BIGINT)
            ),
            claims_filed AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) AS pnum FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE panumber IS NOT NULL AND datesubmitted >= DATE '{year_start}' AND datesubmitted <= DATE '{year_end}'
            )
            SELECT COALESCE(SUM(pa.g), 0) FROM pa_issued pa
            LEFT JOIN claims_filed c ON pa.pnum = c.pnum
            WHERE c.pnum IS NULL
        """
        total_unclaimed = float(conn.execute(unclaimed_q).fetchone()[0] or 0)

        # 4) Panumber count by month (last 6 months) — from PA DATA by requestdate (PA issued per month)
        #    Using PA DATA avoids 0s when claims are submitted in a different month than the PA was issued.
        #    Chronological order (oldest first) for chart.
        panumber_by_month = []
        for y, m, s, e in reversed(months):
            q = f"""
                SELECT COUNT(DISTINCT pa.panumber)
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE ({_provider_id_eq_literal('pa.providerid', sid)})
                  AND pa.panumber IS NOT NULL
                  AND pa.requestdate >= DATE '{s.isoformat()}' AND pa.requestdate <= DATE '{e.isoformat()}'
            """
            cnt = int(conn.execute(q).fetchone()[0] or 0)
            panumber_by_month.append({
                "year": y,
                "month": m,
                "month_label": f"{m:02d}/{y}",
                "count": cnt,
            })

        # 5) Claims cost and PA cost by month (last 6 months). Single query per series for correct per-month aggregation.
        #    - cost: total approved amount by datesubmitted
        #    - pa_cost: total granted by requestdate
        #    - cost_encounter: total approved amount by encounterdatefrom
        months_chrono = list(reversed(months))  # oldest first
        # Build explicit month bounds for SQL (m_num 1 = oldest, 6 = newest)
        month_bounds_sql = " UNION ALL ".join(
            f"SELECT {i} AS m_num, DATE '{months_chrono[i - 1][2].isoformat()}' AS start_d, DATE '{months_chrono[i - 1][3].isoformat()}' AS end_d"
            for i in range(1, len(months_chrono) + 1)
        )
        cost_6m_q = f"""
            WITH month_bounds AS ({month_bounds_sql}),
            claims_sub AS (
                SELECT mb.m_num, COALESCE(SUM(c.approvedamount), 0) AS cost
                FROM month_bounds mb
                LEFT JOIN "AI DRIVEN DATA"."CLAIMS DATA" c
                  ON ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                 AND c.datesubmitted >= mb.start_d AND c.datesubmitted <= mb.end_d
                GROUP BY mb.m_num
            ),
            claims_enc AS (
                SELECT mb.m_num, COALESCE(SUM(c.approvedamount), 0) AS cost_encounter
                FROM month_bounds mb
                LEFT JOIN "AI DRIVEN DATA"."CLAIMS DATA" c
                  ON ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                 AND c.encounterdatefrom IS NOT NULL
                 AND c.encounterdatefrom >= mb.start_d AND c.encounterdatefrom <= mb.end_d
                GROUP BY mb.m_num
            ),
            pa_by_m AS (
                SELECT mb.m_num, COALESCE(SUM(pa.granted), 0) AS pa_cost
                FROM month_bounds mb
                LEFT JOIN "AI DRIVEN DATA"."PA DATA" pa
                  ON ({_provider_id_eq_literal('pa.providerid', sid)})
                 AND pa.requestdate >= mb.start_d AND pa.requestdate <= mb.end_d
                 AND pa.granted IS NOT NULL
                GROUP BY mb.m_num
            )
            SELECT mb.m_num, COALESCE(cs.cost, 0) AS cost, COALESCE(ce.cost_encounter, 0) AS cost_encounter, COALESCE(pm.pa_cost, 0) AS pa_cost
            FROM month_bounds mb
            LEFT JOIN claims_sub cs ON mb.m_num = cs.m_num
            LEFT JOIN claims_enc ce ON mb.m_num = ce.m_num
            LEFT JOIN pa_by_m pm ON mb.m_num = pm.m_num
            ORDER BY mb.m_num
        """
        rows_6m = conn.execute(cost_6m_q).fetchall()
        cost_by_month = []
        for i, r in enumerate(rows_6m):
            m_num = int(r[0] or 0)
            if 1 <= m_num <= len(months_chrono):
                y, m, _s, _e = months_chrono[m_num - 1]
                cost_by_month.append({
                    "year": y,
                    "month": m,
                    "month_label": f"{m:02d}/{y}",
                    "cost": float(r[1] or 0),
                    "cost_encounter": float(r[2] or 0),
                    "pa_cost": float(r[3] or 0),
                })
        claims_avg_6 = round(sum(p["cost"] for p in cost_by_month) / len(cost_by_month), 2) if cost_by_month else 0.0
        claims_encounter_avg_6 = round(sum(p["cost_encounter"] for p in cost_by_month) / len(cost_by_month), 2) if cost_by_month else 0.0
        pa_avg_6 = round(sum(p["pa_cost"] for p in cost_by_month) / len(cost_by_month), 2) if cost_by_month else 0.0

        # 6) Top 5 companies by cost (last 6 months, datesubmitted)
        # Use GROUPS for groupname so we get a name for every nhisgroupid (GROUP_CONTRACT only has current contracts)
        top5_cost_q = f"""
            SELECT COALESCE(TRIM(g.groupname), CAST(c.nhisgroupid AS VARCHAR)) AS company_name,
                   SUM(c.approvedamount) AS total_cost
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."GROUPS" g
              ON CAST(c.nhisgroupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
              AND c.datesubmitted >= DATE '{m_start}' AND c.datesubmitted <= DATE '{m_end}'
              AND c.approvedamount > 0
            GROUP BY g.groupname, c.nhisgroupid
            ORDER BY total_cost DESC
            LIMIT 5
        """
        top5_by_cost = [
            {"company_name": str(r[0] or "").strip() or "—", "total_cost": float(r[1] or 0)}
            for r in conn.execute(top5_cost_q).fetchall()
        ]

        # 7) Top 5 companies by unique enrollee count (last 6 months, datesubmitted)
        top5_enrollees_q = f"""
            SELECT COALESCE(TRIM(g.groupname), CAST(c.nhisgroupid AS VARCHAR)) AS company_name,
                   COUNT(DISTINCT {eid}) AS enrollee_count
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."GROUPS" g
              ON CAST(c.nhisgroupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
            WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
              AND c.datesubmitted >= DATE '{m_start}' AND c.datesubmitted <= DATE '{m_end}'
              AND {eid} IS NOT NULL
            GROUP BY g.groupname, c.nhisgroupid
            ORDER BY enrollee_count DESC
            LIMIT 5
        """
        top5_by_enrollee_count = [
            {"company_name": str(r[0] or "").strip() or "—", "enrollee_count": int(r[1] or 0)}
            for r in conn.execute(top5_enrollees_q).fetchall()
        ]

        # 8) Claims per Enrollee Ratio (CER) month by month for last 10 months
        #    - cer_datesubmitted: uses datesubmitted (existing view)
        #    - cer_encounter: uses encounterdatefrom (new view)
        months10 = _last_n_calendar_months(10)
        cer_by_month = []
        for y, m, s, e in reversed(months10):  # oldest first for chart
            # CER by datesubmitted
            cer_ds_q = f"""
                SELECT 
                    COUNT(*) AS claim_count,
                    COUNT(DISTINCT {eid}) AS enrollee_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                  AND c.datesubmitted >= DATE '{s.isoformat()}' AND c.datesubmitted <= DATE '{e.isoformat()}'
                  AND {eid} IS NOT NULL
            """
            row_ds = conn.execute(cer_ds_q).fetchone()
            claim_count_ds = int(row_ds[0] or 0)
            enrollee_count_ds = int(row_ds[1] or 0)
            cer_ds = (claim_count_ds / enrollee_count_ds) if enrollee_count_ds > 0 else 0.0

            # CER by encounterdatefrom
            cer_enc_q = f"""
                SELECT 
                    COUNT(*) AS claim_count,
                    COUNT(DISTINCT {eid}) AS enrollee_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                  AND c.encounterdatefrom IS NOT NULL
                  AND c.encounterdatefrom >= DATE '{s.isoformat()}' AND c.encounterdatefrom <= DATE '{e.isoformat()}'
                  AND {eid} IS NOT NULL
            """
            row_enc = conn.execute(cer_enc_q).fetchone()
            claim_count_enc = int(row_enc[0] or 0)
            enrollee_count_enc = int(row_enc[1] or 0)
            cer_enc = (claim_count_enc / enrollee_count_enc) if enrollee_count_enc > 0 else 0.0

            cer_by_month.append(
                {
                    "year": y,
                    "month": m,
                    "month_label": f"{m:02d}/{y}",
                    "claim_count_datesubmitted": claim_count_ds,
                    "enrollee_count_datesubmitted": enrollee_count_ds,
                    "cer_datesubmitted": round(cer_ds, 4),
                    "claim_count_encounter": claim_count_enc,
                    "enrollee_count_encounter": enrollee_count_enc,
                    "cer_encounter": round(cer_enc, 4),
                }
            )

        valid_cer_ds = [p["cer_datesubmitted"] for p in cer_by_month if p["enrollee_count_datesubmitted"] > 0]
        valid_cer_enc = [p["cer_encounter"] for p in cer_by_month if p["enrollee_count_encounter"] > 0]
        cer_avg_last_10_datesubmitted = (
            round(sum(valid_cer_ds) / len(valid_cer_ds), 4) if valid_cer_ds else 0.0
        )
        cer_avg_last_10_encounter = (
            round(sum(valid_cer_enc) / len(valid_cer_enc), 4) if valid_cer_enc else 0.0
        )
        # Backwards-compat: keep original field pointing to datesubmitted-based average
        cer_avg_last_10 = cer_avg_last_10_datesubmitted

        # 9) Visits per Enrollee (VPE) month by month for last 10 months (encounterdatefrom)
        # Visits logic:
        #  - Rows with panumber: each distinct panumber = one visit (per day per visit).
        #  - Rows without panumber: visit = distinct (enrollee_id, DATE(encounterdatefrom)).
        # Also compute claim line count per month so we can derive:
        #  - Average Claims per Visit = total claim rows / total visits
        vpe_by_month = []
        for y, m, s, e in reversed(months10):  # oldest first
            vpe_q = f"""
                WITH pa_visits AS (
                    SELECT COUNT(DISTINCT c.panumber) AS v
                    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                    WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                      AND c.panumber IS NOT NULL
                      AND c.encounterdatefrom IS NOT NULL
                      AND c.encounterdatefrom >= DATE '{s.isoformat()}' AND c.encounterdatefrom <= DATE '{e.isoformat()}'
                ),
                non_pa_visits AS (
                    SELECT COUNT(*) AS v
                    FROM (
                        SELECT DISTINCT {eid} AS enrollee, DATE(c.encounterdatefrom) AS d
                        FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                        WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                          AND c.panumber IS NULL
                          AND {eid} IS NOT NULL
                          AND c.encounterdatefrom IS NOT NULL
                          AND c.encounterdatefrom >= DATE '{s.isoformat()}' AND c.encounterdatefrom <= DATE '{e.isoformat()}'
                    ) t
                ),
                enrollee_counts AS (
                    SELECT COUNT(DISTINCT {eid}) AS unique_enrollees
                    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                    WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                      AND {eid} IS NOT NULL
                      AND c.encounterdatefrom IS NOT NULL
                      AND c.encounterdatefrom >= DATE '{s.isoformat()}' AND c.encounterdatefrom <= DATE '{e.isoformat()}'
                ),
                claims_lines AS (
                    SELECT 
                        COUNT(*) AS cnt,
                        COALESCE(SUM(c.approvedamount), 0) AS total_amount
                    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                    WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                      AND c.encounterdatefrom IS NOT NULL
                      AND c.encounterdatefrom >= DATE '{s.isoformat()}' AND c.encounterdatefrom <= DATE '{e.isoformat()}'
                )
                SELECT
                    COALESCE((SELECT v FROM pa_visits), 0) + COALESCE((SELECT v FROM non_pa_visits), 0) AS total_visits,
                    COALESCE((SELECT unique_enrollees FROM enrollee_counts), 0) AS unique_enrollees,
                    COALESCE((SELECT cnt FROM claims_lines), 0) AS claim_lines,
                    COALESCE((SELECT total_amount FROM claims_lines), 0) AS total_amount
            """
            row = conn.execute(vpe_q).fetchone()
            total_visits = int(row[0] or 0)
            unique_enrollees = int(row[1] or 0)
            claim_lines = int(row[2] or 0)
            total_amount = float(row[3] or 0)
            vpe = (total_visits / unique_enrollees) if unique_enrollees > 0 else 0.0
            cpv = (claim_lines / total_visits) if total_visits > 0 else 0.0
            vpe_by_month.append(
                {
                    "year": y,
                    "month": m,
                    "month_label": f"{m:02d}/{y}",
                    "total_visits": total_visits,
                    "unique_enrollees": unique_enrollees,
                    "vpe": round(vpe, 4),
                    "claim_lines": claim_lines,
                    "cpv": round(cpv, 4),
                    "total_amount": round(total_amount, 2),
                }
            )

        valid_vpe_values = [p["vpe"] for p in vpe_by_month if p["unique_enrollees"] > 0]
        vpe_avg_last_10 = round(sum(valid_vpe_values) / len(valid_vpe_values), 4) if valid_vpe_values else 0.0
        valid_cpv_values = [p["cpv"] for p in vpe_by_month if p["total_visits"] > 0]
        cpv_avg_last_10 = round(sum(valid_cpv_values) / len(valid_cpv_values), 4) if valid_cpv_values else 0.0
        # Average Cost per Visit (ACV) and Cost per Enrollee (CPE)
        # ACV = total billed amount / total visits
        # CPE = total billed amount / unique enrollees
        acv_values = [
            (p["total_amount"] / p["total_visits"])
            for p in vpe_by_month
            if p["total_visits"] > 0
        ]
        cpe_values = [
            (p["total_amount"] / p["unique_enrollees"])
            for p in vpe_by_month
            if p["unique_enrollees"] > 0
        ]
        acv_avg_last_10 = round(sum(acv_values) / len(acv_values), 2) if acv_values else 0.0
        cpe_avg_last_10 = round(sum(cpe_values) / len(cpe_values), 2) if cpe_values else 0.0

        # 10) Top procedures (last 3 calendar months by encounterdatefrom) with usage ratios per enrollee
        # - Top 10 by count
        # - Top 10 by cost
        months3 = _last_n_calendar_months(3)
        # Oldest first for clearer table columns
        months3_chrono = list(reversed(months3))
        proc_month_labels = [f"{m:02d}/{y}" for (y, m, _s, _e) in months3_chrono]

        if months3_chrono:
            (y1, m1, s1, e1) = months3_chrono[0]
            (y2, m2, s2, e2) = months3_chrono[1] if len(months3_chrono) > 1 else months3_chrono[0]
            (y3, m3, s3, e3) = months3_chrono[2] if len(months3_chrono) > 2 else months3_chrono[-1]

            month_bounds_cte = f"""
                WITH month_bounds AS (
                    SELECT 1 AS m_num, DATE '{s1.isoformat()}' AS start_d, DATE '{e1.isoformat()}' AS end_d
                    UNION ALL SELECT 2, DATE '{s2.isoformat()}', DATE '{e2.isoformat()}'
                    UNION ALL SELECT 3, DATE '{s3.isoformat()}', DATE '{e3.isoformat()}'
                )
            """

            # Per-procedure counts & costs per month
            proc_q = f"""
                {month_bounds_cte},
                per_proc_month AS (
                    SELECT
                        TRIM(UPPER(c.code)) AS procedure_code,
                        mb.m_num,
                        COUNT(*) AS line_count,
                        COALESCE(SUM(c.approvedamount), 0) AS total_cost
                    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                    CROSS JOIN month_bounds mb
                    WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                      AND c.encounterdatefrom IS NOT NULL
                      AND c.encounterdatefrom >= mb.start_d AND c.encounterdatefrom <= mb.end_d
                      AND c.code IS NOT NULL
                      AND TRIM(c.code) <> ''
                    GROUP BY TRIM(UPPER(c.code)), mb.m_num
                )
                SELECT procedure_code, m_num, line_count, total_cost
                FROM per_proc_month
            """
            proc_rows = conn.execute(proc_q).fetchall()

            # Enrollee counts per month (denominator for ratios)
            enrollees_q = f"""
                {month_bounds_cte}
                SELECT
                    mb.m_num,
                    COUNT(DISTINCT {eid}) AS enrollee_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                CROSS JOIN month_bounds mb
                WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                  AND {eid} IS NOT NULL
                  AND c.encounterdatefrom IS NOT NULL
                  AND c.encounterdatefrom >= mb.start_d AND c.encounterdatefrom <= mb.end_d
                GROUP BY mb.m_num
            """
            enrollee_rows = conn.execute(enrollees_q).fetchall()

            enrollees_by_m = {int(r[0]): int(r[1] or 0) for r in enrollee_rows}

            # Aggregate per-procedure stats across 3 months
            proc_stats = {}
            for code, m_num, line_count, total_cost in proc_rows:
                if not code:
                    continue
                m_num = int(m_num or 0)
                st = proc_stats.setdefault(
                    code,
                    {
                        "procedure_code": code,
                        "cnt_m1": 0,
                        "cnt_m2": 0,
                        "cnt_m3": 0,
                        "cost_m1": 0.0,
                        "cost_m2": 0.0,
                        "cost_m3": 0.0,
                    },
                )
                lc = int(line_count or 0)
                amt = float(total_cost or 0)
                if m_num == 1:
                    st["cnt_m1"] += lc
                    st["cost_m1"] += amt
                elif m_num == 2:
                    st["cnt_m2"] += lc
                    st["cost_m2"] += amt
                elif m_num == 3:
                    st["cnt_m3"] += lc
                    st["cost_m3"] += amt

            # Default outputs
            top_procs_by_count = []
            top_procs_by_cost = []

            if proc_stats:
                # Enrollee denominators
                e1 = enrollees_by_m.get(1, 0)
                e2 = enrollees_by_m.get(2, 0)
                e3 = enrollees_by_m.get(3, 0)

                for st in proc_stats.values():
                    cnt_m1 = st["cnt_m1"]
                    cnt_m2 = st["cnt_m2"]
                    cnt_m3 = st["cnt_m3"]
                    cost_m1 = st["cost_m1"]
                    cost_m2 = st["cost_m2"]
                    cost_m3 = st["cost_m3"]
                    total_cnt = cnt_m1 + cnt_m2 + cnt_m3
                    total_cost = cost_m1 + cost_m2 + cost_m3

                    # Ratios per month (procedure lines per unique enrollee)
                    r1 = (cnt_m1 / e1) if e1 > 0 else 0.0
                    r2 = (cnt_m2 / e2) if e2 > 0 else 0.0
                    r3 = (cnt_m3 / e3) if e3 > 0 else 0.0
                    valid_rs = [r for r in (r1, r2, r3) if r > 0]
                    r_avg = (sum(valid_rs) / len(valid_rs)) if valid_rs else 0.0

                    st.update(
                        {
                            "total_count_3m": total_cnt,
                            "total_cost_3m": total_cost,
                            "ratio_m1": round(r1, 4),
                            "ratio_m2": round(r2, 4),
                            "ratio_m3": round(r3, 4),
                            "ratio_avg": round(r_avg, 4),
                        }
                    )

                # Fetch procedure names for the codes we have
                codes = [c for c in proc_stats.keys() if c]
                if codes:
                    escaped_codes = [c.replace("'", "''") for c in codes]
                    in_list = ", ".join(f"'{c}'" for c in escaped_codes)
                    names_q = f"""
                        SELECT
                            TRIM(UPPER(procedurecode)) AS code,
                            MAX(TRIM(COALESCE(proceduredesc, ''))) AS name
                        FROM "AI DRIVEN DATA"."PROCEDURE DATA"
                        WHERE TRIM(UPPER(procedurecode)) IN ({in_list})
                        GROUP BY TRIM(UPPER(procedurecode))
                    """
                    name_rows = conn.execute(names_q).fetchall()
                    name_map = {str(r[0] or ""): str(r[1] or "") for r in name_rows}
                else:
                    name_map = {}

                for code, st in proc_stats.items():
                    raw_name = name_map.get(str(code) or "", "")
                    st["procedure_name"] = raw_name if raw_name else code

                # Build top 10 lists
                all_stats = list(proc_stats.values())
                top_procs_by_count = sorted(
                    all_stats, key=lambda x: x["total_count_3m"], reverse=True
                )[:10]
                top_procs_by_cost = sorted(
                    all_stats, key=lambda x: x["total_cost_3m"], reverse=True
                )[:10]
        else:
            proc_month_labels = []
            top_procs_by_count = []
            top_procs_by_cost = []

        # 11) High-concentration clients: provider's share of client debit note total (> 15%)
        #     Only consider clients that have used this provider in the last 6 months (encounterdatefrom).
        #     Provider's amount = claims (encounterdatefrom in contract) + unclaimed PA (requestdate in contract)
        #     so it matches Client Analysis "Top 20 Providers" total cost per provider.
        high_concentration_clients = []
        concentration_threshold = 0.15
        try:
            high_conc_q = f"""
                WITH contract_periods AS (
                    SELECT 
                        gc.groupid,
                        gc.groupname,
                        gc.startdate AS contract_start,
                        gc.enddate AS contract_end
                    FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
                    WHERE gc.iscurrent = 1
                ),
                provider_clients_last6 AS (
                    SELECT DISTINCT c.nhisgroupid
                    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                    WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                      AND c.encounterdatefrom >= DATE '{m_start}' AND c.encounterdatefrom <= DATE '{m_end}'
                      AND c.approvedamount > 0
                      AND c.nhisgroupid IS NOT NULL
                ),
                contracts_for_provider_clients AS (
                    SELECT
                        cp.groupid,
                        cp.groupname,
                        cp.contract_start,
                        cp.contract_end
                    FROM contract_periods cp
                    JOIN provider_clients_last6 pcl
                      ON CAST(pcl.nhisgroupid AS VARCHAR) = CAST(cp.groupid AS VARCHAR)
                ),
                claimed_pa_per_client AS (
                    SELECT DISTINCT
                        cpc.groupid,
                        CAST(c.panumber AS BIGINT) AS panumber_bigint
                    FROM contracts_for_provider_clients cpc
                    INNER JOIN "AI DRIVEN DATA"."CLAIMS DATA" c
                      ON CAST(c.nhisgroupid AS VARCHAR) = CAST(cpc.groupid AS VARCHAR)
                     AND c.panumber IS NOT NULL
                     AND c.encounterdatefrom >= CAST(cpc.contract_start AS DATE)
                     AND c.encounterdatefrom <= CAST(cpc.contract_end AS DATE)
                ),
                provider_unclaimed_pa AS (
                    SELECT
                        cpc.groupid,
                        COALESCE(SUM(pa.granted), 0) AS provider_unclaimed_pa_amount
                    FROM contracts_for_provider_clients cpc
                    INNER JOIN "AI DRIVEN DATA"."PA DATA" pa
                      ON TRIM(COALESCE(pa.groupname, '')) = TRIM(COALESCE(cpc.groupname, ''))
                     AND pa.requestdate >= CAST(cpc.contract_start AS TIMESTAMP)
                     AND pa.requestdate <= CAST(cpc.contract_end AS TIMESTAMP)
                     AND pa.panumber IS NOT NULL
                     AND pa.granted > 0
                     AND CAST(pa.panumber AS BIGINT) IS NOT NULL
                     AND ({_provider_id_eq_literal('pa.providerid', sid)})
                    LEFT JOIN claimed_pa_per_client cp
                      ON cp.groupid = cpc.groupid AND CAST(pa.panumber AS BIGINT) = cp.panumber_bigint
                    WHERE cp.panumber_bigint IS NULL
                    GROUP BY cpc.groupid
                ),
                debit_totals AS (
                    SELECT
                        cpc.groupid,
                        COALESCE(SUM(dn.Amount), 0) AS total_debit_amount
                    FROM contracts_for_provider_clients cpc
                    LEFT JOIN "AI DRIVEN DATA"."DEBIT_NOTE" dn
                      ON TRIM(COALESCE(cpc.groupname, '')) = TRIM(COALESCE(dn.CompanyName, ''))
                     AND dn."From" >= CAST(cpc.contract_start AS DATE)
                     AND dn."From" <= CAST(cpc.contract_end AS DATE)
                     AND (dn.Description IS NULL OR LOWER(TRIM(CAST(dn.Description AS VARCHAR))) NOT LIKE '%tpa%')
                    GROUP BY cpc.groupid
                ),
                provider_claims_for_contract AS (
                    SELECT
                        cpc.groupid,
                        COALESCE(SUM(c.approvedamount), 0) AS provider_claims_amount
                    FROM contracts_for_provider_clients cpc
                    LEFT JOIN "AI DRIVEN DATA"."CLAIMS DATA" c
                      ON CAST(c.nhisgroupid AS VARCHAR) = CAST(cpc.groupid AS VARCHAR)
                     AND ({_provider_id_eq_literal('c.nhisproviderid', sid)})
                     AND c.encounterdatefrom >= CAST(cpc.contract_start AS DATE)
                     AND c.encounterdatefrom <= CAST(cpc.contract_end AS DATE)
                     AND c.approvedamount > 0
                    GROUP BY cpc.groupid
                )
                SELECT
                    cpc.groupname,
                    cpc.contract_start,
                    cpc.contract_end,
                    dt.total_debit_amount,
                    pc.provider_claims_amount,
                    COALESCE(pu.provider_unclaimed_pa_amount, 0) AS provider_unclaimed_pa_amount,
                    pc.provider_claims_amount + COALESCE(pu.provider_unclaimed_pa_amount, 0) AS provider_total_contract,
                    CASE 
                        WHEN dt.total_debit_amount > 0 
                        THEN (pc.provider_claims_amount + COALESCE(pu.provider_unclaimed_pa_amount, 0)) / dt.total_debit_amount
                        ELSE NULL
                    END AS provider_share
                FROM contracts_for_provider_clients cpc
                LEFT JOIN debit_totals dt ON cpc.groupid = dt.groupid
                LEFT JOIN provider_claims_for_contract pc ON cpc.groupid = pc.groupid
                LEFT JOIN provider_unclaimed_pa pu ON cpc.groupid = pu.groupid
                WHERE dt.total_debit_amount > 0
            """
            rows = conn.execute(high_conc_q).fetchall()
            for r in rows:
                groupname = str(r[0] or "").strip()
                contract_start = r[1]
                contract_end = r[2]
                total_debit_amount = float(r[3] or 0)
                provider_claims_amount = float(r[4] or 0)
                provider_unclaimed_pa_amount = float(r[5] or 0)
                provider_total_contract = float(r[6] or 0)
                provider_share = float(r[7] or 0) if r[7] is not None else 0.0
                if total_debit_amount > 0 and provider_share >= concentration_threshold:
                    high_concentration_clients.append(
                        {
                            "client_name": groupname or "—",
                            "contract_start": contract_start.isoformat() if hasattr(contract_start, "isoformat") else str(contract_start),
                            "contract_end": contract_end.isoformat() if hasattr(contract_end, "isoformat") else str(contract_end),
                            "total_debit_amount": total_debit_amount,
                            "provider_claims_amount": provider_claims_amount,
                            "provider_unclaimed_pa_amount": provider_unclaimed_pa_amount,
                            "provider_total_contract": provider_total_contract,
                            "provider_share": round(provider_share, 4),
                        }
                    )
        except Exception as _hc_err:
            logger.warning(f"High-concentration client calculation failed for provider {provider_id}: {_hc_err}")

        return {
            "success": True,
            "data": {
                "provider_id": provider_id,
                "total_pa_granted_this_year": total_pa_granted_this_year,
                "total_claimed_pa_cost": total_claimed_pa_cost,
                "total_unclaimed": total_unclaimed,
                "total_claims_this_year_datesubmitted": total_claims_this_year_datesubmitted,
                "panumber_count_by_month_last_6": panumber_by_month,
                "claims_cost_by_month_last_6": cost_by_month,
                "claims_avg_last_6": claims_avg_6,
                "claims_encounter_avg_last_6": claims_encounter_avg_6,
                "pa_avg_last_6": pa_avg_6,
                "top5_companies_by_cost_last_6": top5_by_cost,
                "top5_companies_by_enrollee_count_last_6": top5_by_enrollee_count,
                "cer_by_month_last_10": cer_by_month,
                "cer_avg_last_10": cer_avg_last_10,
                "cer_avg_last_10_datesubmitted": cer_avg_last_10_datesubmitted,
                "cer_avg_last_10_encounter": cer_avg_last_10_encounter,
                "vpe_by_month_last_10": vpe_by_month,
                "vpe_avg_last_10": vpe_avg_last_10,
                "cpv_avg_last_10": cpv_avg_last_10,
                "acv_avg_last_10": acv_avg_last_10,
                "cpe_avg_last_10": cpe_avg_last_10,
                "procedure_month_labels_last_3": proc_month_labels,
                "top_procedures_by_count_last_3": top_procs_by_count,
                "top_procedures_by_cost_last_3": top_procs_by_cost,
                "high_concentration_clients_last_6": high_concentration_clients,
                "high_concentration_threshold": concentration_threshold,
            },
        }
    except Exception as e:
        logger.error(f"Error fetching provider analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/critical-alerts/monthly-scoreboard")
async def get_provider_critical_alerts_monthly():
    """
    Critical Alert scoreboard for providers for the current calendar month.
    
    For each provider (by encounterdatefrom month):
      - VPE (Visits per Enrollee)
      - CPE (Cost per Enrollee) = total claim cost / unique enrollees
      - CPV (Cost per Visit) = total claim cost / total visits
      - Repeat diagnosis rate = % of enrollees with at least one diagnosis repeated ≥2 times in the month
      - Short interval visits rate = % of enrollees with ≥1 pair of visits ≤14 days apart
    
    Scoring rules per provider (per band peer group):
      1. VPE > peer_mean + 1.5*SD     → +1
      2. CPE > peer mean + 2*SD       → +2
      3. CPV > peer mean + 2*SD       → +2
      4. Repeat diagnosis rate > 30%  → +1
      5. Short interval rate > 10%    → +1
    
    Returns providers whose total score (sum of the 5 components) is ≥ 4,
    with per-metric details so the UI can show a breakdown.
    """
    try:
        conn = get_db_connection()
        now = datetime.now()
        y, m = now.year, now.month
        month_start, month_end = _first_last_day_of_month(y, m)
        eid = _enrollee_id_col()

        # Build scoreboard in one DuckDB query
        query = f"""
        WITH month_bounds AS (
            SELECT DATE '{month_start.isoformat()}' AS start_d,
                   DATE '{month_end.isoformat()}'   AS end_d
        ),
        claims_month AS (
            SELECT
                TRIM(CAST(c.nhisproviderid AS VARCHAR)) AS provider_id,
                {eid} AS enrollee_id,
                DATE(c.encounterdatefrom) AS enc_date,
                c.diagnosiscode,
                COALESCE(c.approvedamount, 0) AS approvedamount,
                c.panumber
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c, month_bounds mb
            WHERE c.encounterdatefrom IS NOT NULL
              AND c.encounterdatefrom >= mb.start_d
              AND c.encounterdatefrom <= mb.end_d
              AND {eid} IS NOT NULL
        ),
        provider_base AS (
            SELECT
                provider_id,
                COUNT(DISTINCT enrollee_id) AS unique_enrollees,
                COUNT(
                    DISTINCT
                    CASE
                        WHEN enc_date IS NULL THEN NULL
                        WHEN panumber IS NOT NULL
                             THEN 'P|' || CAST(panumber AS VARCHAR)
                        ELSE 'N|' || enrollee_id || '|' || CAST(enc_date AS VARCHAR)
                    END
                ) AS total_visits,
                COUNT(*) AS claim_lines,
                SUM(approvedamount) AS total_amount
            FROM claims_month
            GROUP BY provider_id
        ),
        provider_metrics AS (
            SELECT
                pb.*,
                CASE
                    WHEN unique_enrollees > 0 THEN CAST(total_visits AS DOUBLE) / unique_enrollees
                    ELSE 0
                END AS vpe,
                CASE
                    WHEN unique_enrollees > 0 THEN total_amount / unique_enrollees
                    ELSE 0
                END AS cpe,
                CASE
                    WHEN total_visits > 0 THEN total_amount / total_visits
                    ELSE 0
                END AS cpv
            FROM provider_base pb
        ),
        provider_with_band AS (
            SELECT
                pm.*,
                COALESCE(
                    NULLIF(TRIM(CAST(p.bands AS VARCHAR)), ''),
                    'Unspecified'
                ) AS provider_band
            FROM provider_metrics pm
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
              ON TRIM(CAST(p.providerid AS VARCHAR)) = pm.provider_id
        ),
        peer_stats AS (
            SELECT
                provider_band,
                AVG(cpe) AS avg_cpe,
                STDDEV_SAMP(cpe) AS std_cpe,
                AVG(cpv) AS avg_cpv,
                STDDEV_SAMP(cpv) AS std_cpv,
                AVG(vpe) AS avg_vpe,
                STDDEV_SAMP(vpe) AS std_vpe
            FROM provider_with_band
            WHERE unique_enrollees > 0
              AND total_visits > 0
            GROUP BY provider_band
        ),
        -- Repeat diagnosis rate per enrollee per provider
        repeat_dx_per_enrollee AS (
            SELECT
                provider_id,
                enrollee_id,
                MAX(CASE WHEN cnt_per_dx >= 2 THEN 1 ELSE 0 END) AS has_repeat_dx
            FROM (
                SELECT
                    provider_id,
                    enrollee_id,
                    diagnosiscode,
                    COUNT(*) AS cnt_per_dx
                FROM claims_month
                GROUP BY provider_id, enrollee_id, diagnosiscode
            ) t
            GROUP BY provider_id, enrollee_id
        ),
        repeat_dx_provider AS (
            SELECT
                provider_id,
                SUM(has_repeat_dx) AS repeat_enrollees,
                COUNT(*) AS total_enrollees,
                CASE
                    WHEN COUNT(*) > 0 THEN CAST(SUM(has_repeat_dx) AS DOUBLE) / COUNT(*)
                    ELSE 0
                END AS repeat_rate
            FROM repeat_dx_per_enrollee
            GROUP BY provider_id
        ),
        -- Short interval visits: enrollee has ≥ 2 visits ≤ 14 days apart
        visit_dates AS (
            SELECT DISTINCT
                provider_id,
                enrollee_id,
                enc_date
            FROM claims_month
            WHERE enc_date IS NOT NULL
        ),
        short_interval_flags AS (
            SELECT
                provider_id,
                enrollee_id,
                MAX(
                    CASE
                        WHEN next_date IS NOT NULL AND next_date <= enc_date + INTERVAL 14 DAY
                        THEN 1 ELSE 0
                    END
                ) AS has_short_interval
            FROM (
                SELECT
                    provider_id,
                    enrollee_id,
                    enc_date,
                    LEAD(enc_date) OVER (
                        PARTITION BY provider_id, enrollee_id
                        ORDER BY enc_date
                    ) AS next_date
                FROM visit_dates
            ) v
            GROUP BY provider_id, enrollee_id
        ),
        short_interval_provider AS (
            SELECT
                provider_id,
                SUM(has_short_interval) AS short_interval_enrollees,
                COUNT(*) AS total_enrollees,
                CASE
                    WHEN COUNT(*) > 0 THEN CAST(SUM(has_short_interval) AS DOUBLE) / COUNT(*)
                    ELSE 0
                END AS short_interval_rate
            FROM short_interval_flags
            GROUP BY provider_id
        ),
        scored AS (
            SELECT
                pm.provider_id,
                pm.unique_enrollees,
                pm.total_visits,
                pm.total_amount,
                pm.vpe,
                pm.cpe,
                pm.cpv,
                pm.provider_band,
                COALESCE(rd.repeat_rate, 0) AS repeat_rate,
                COALESCE(si.short_interval_rate, 0) AS short_interval_rate,
                CASE
                    WHEN ps.std_vpe IS NOT NULL AND ps.std_vpe > 0
                         AND pm.vpe > ps.avg_vpe + 1.5 * ps.std_vpe
                    THEN 1 ELSE 0
                END AS score_vpe,
                CASE
                    WHEN ps.std_cpe IS NOT NULL AND ps.std_cpe > 0
                         AND pm.cpe > ps.avg_cpe + 2 * ps.std_cpe
                    THEN 2 ELSE 0
                END AS score_cpe,
                CASE
                    WHEN ps.std_cpv IS NOT NULL AND ps.std_cpv > 0
                         AND pm.cpv > ps.avg_cpv + 2 * ps.std_cpv
                    THEN 2 ELSE 0
                END AS score_cpv,
                CASE
                    WHEN COALESCE(rd.repeat_rate, 0) > 0.30 THEN 1
                    ELSE 0
                END AS score_repeat_dx,
                CASE
                    WHEN COALESCE(si.short_interval_rate, 0) > 0.10 THEN 1
                    ELSE 0
                END AS score_short_interval
            FROM provider_with_band pm
            LEFT JOIN peer_stats ps ON pm.provider_band = ps.provider_band
            LEFT JOIN repeat_dx_provider rd ON pm.provider_id = rd.provider_id
            LEFT JOIN short_interval_provider si ON pm.provider_id = si.provider_id
        ),
        totals AS (
            SELECT
                s.*,
                (score_vpe + score_cpe + score_cpv + score_repeat_dx + score_short_interval) AS total_score
            FROM scored s
        )
        SELECT
            t.provider_id,
            TRIM(CAST(p.providername AS VARCHAR)) AS provider_name,
            t.unique_enrollees,
            t.total_visits,
            t.total_amount,
            t.vpe,
            t.cpe,
            t.cpv,
            t.repeat_rate,
            t.short_interval_rate,
            t.score_vpe,
            t.score_cpe,
            t.score_cpv,
            t.score_repeat_dx,
            t.score_short_interval,
            t.total_score,
            t.provider_band
        FROM totals t
        LEFT JOIN \"AI DRIVEN DATA\".\"PROVIDERS\" p
          ON TRIM(CAST(p.providerid AS VARCHAR)) = t.provider_id
        WHERE t.total_score >= 4
        ORDER BY t.total_score DESC, t.total_amount DESC;
        """

        rows = conn.execute(query).fetchall()
        conn.close()

        scoreboard = []
        for r in rows:
            scoreboard.append({
                "provider_id": r[0],
                "provider_name": (r[1] or "").strip() or f"Provider {r[0]}",
                "unique_enrollees": int(r[2] or 0),
                "total_visits": int(r[3] or 0),
                "total_amount": float(r[4] or 0),
                "vpe": float(r[5] or 0),
                "cpe": float(r[6] or 0),
                "cpv": float(r[7] or 0),
                "repeat_rate": float(r[8] or 0),
                "short_interval_rate": float(r[9] or 0),
                "score_vpe": int(r[10] or 0),
                "score_cpe": int(r[11] or 0),
                "score_cpv": int(r[12] or 0),
                "score_repeat_dx": int(r[13] or 0),
                "score_short_interval": int(r[14] or 0),
                "total_score": int(r[15] or 0),
            })

        return {
            "success": True,
            "year": y,
            "month": m,
            "month_start": month_start.isoformat(),
            "month_end": month_end.isoformat(),
            "providers": scoreboard,
        }
    except Exception as e:
        logger.error(f"Error computing provider critical alerts scoreboard: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/critical-alerts/monthly-threshold-history")
async def get_provider_critical_alert_threshold_history():
    """
    For each of the last 6 full calendar months, return all (provider, month)
    pairs where the Critical Alert total score for that month is >= 4.
    
    Uses encounterdatefrom and band-based peer normalization for VPE, CPE, CPV
    (same scoring rules as the monthly scoreboard).
    """
    try:
        conn = get_db_connection()
        eid = _enrollee_id_col()

        # Last 6 full calendar months (most recent first)
        months6 = _last_n_calendar_months(6)
        if not months6:
            conn.close()
            return {"success": True, "entries": []}

        entries = []

        for (y, m, start_d, end_d) in months6:
            # Reuse the monthly scoreboard logic, parameterized by start/end dates
            query = f"""
            WITH month_bounds AS (
                SELECT DATE '{start_d.isoformat()}' AS start_d,
                       DATE '{end_d.isoformat()}'   AS end_d
            ),
            claims_month AS (
                SELECT
                    TRIM(CAST(c.nhisproviderid AS VARCHAR)) AS provider_id,
                    {eid} AS enrollee_id,
                    DATE(c.encounterdatefrom) AS enc_date,
                    c.diagnosiscode,
                    COALESCE(c.approvedamount, 0) AS approvedamount,
                    c.panumber
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c, month_bounds mb
                WHERE c.encounterdatefrom IS NOT NULL
                  AND c.encounterdatefrom >= mb.start_d
                  AND c.encounterdatefrom <= mb.end_d
                  AND {eid} IS NOT NULL
            ),
            provider_base AS (
                SELECT
                    provider_id,
                    COUNT(DISTINCT enrollee_id) AS unique_enrollees,
                    COUNT(
                        DISTINCT
                        CASE
                            WHEN enc_date IS NULL THEN NULL
                            WHEN panumber IS NOT NULL
                                 THEN 'P|' || CAST(panumber AS VARCHAR)
                            ELSE 'N|' || enrollee_id || '|' || CAST(enc_date AS VARCHAR)
                        END
                    ) AS total_visits,
                    COUNT(*) AS claim_lines,
                    SUM(approvedamount) AS total_amount
                FROM claims_month
                GROUP BY provider_id
            ),
            provider_metrics AS (
                SELECT
                    pb.*,
                    CASE
                        WHEN unique_enrollees > 0 THEN CAST(total_visits AS DOUBLE) / unique_enrollees
                        ELSE 0
                    END AS vpe,
                    CASE
                        WHEN unique_enrollees > 0 THEN total_amount / unique_enrollees
                        ELSE 0
                    END AS cpe,
                    CASE
                        WHEN total_visits > 0 THEN total_amount / total_visits
                        ELSE 0
                    END AS cpv
                FROM provider_base pb
            ),
            provider_with_band AS (
                SELECT
                    pm.*,
                    COALESCE(
                        NULLIF(TRIM(CAST(p.bands AS VARCHAR)), ''),
                        'Unspecified'
                    ) AS provider_band
                FROM provider_metrics pm
                LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
                  ON TRIM(CAST(p.providerid AS VARCHAR)) = pm.provider_id
            ),
            peer_stats AS (
                SELECT
                    provider_band,
                    AVG(cpe) AS avg_cpe,
                    STDDEV_SAMP(cpe) AS std_cpe,
                    AVG(cpv) AS avg_cpv,
                    STDDEV_SAMP(cpv) AS std_cpv,
                    AVG(vpe) AS avg_vpe,
                    STDDEV_SAMP(vpe) AS std_vpe
                FROM provider_with_band
                WHERE unique_enrollees > 0
                  AND total_visits > 0
                GROUP BY provider_band
            ),
            repeat_dx_per_enrollee AS (
                SELECT
                    provider_id,
                    enrollee_id,
                    MAX(CASE WHEN cnt_per_dx >= 2 THEN 1 ELSE 0 END) AS has_repeat_dx
                FROM (
                    SELECT
                        provider_id,
                        enrollee_id,
                        diagnosiscode,
                        COUNT(*) AS cnt_per_dx
                    FROM claims_month
                    GROUP BY provider_id, enrollee_id, diagnosiscode
                ) t
                GROUP BY provider_id, enrollee_id
            ),
            repeat_dx_provider AS (
                SELECT
                    provider_id,
                    SUM(has_repeat_dx) AS repeat_enrollees,
                    COUNT(*) AS total_enrollees,
                    CASE
                        WHEN COUNT(*) > 0 THEN CAST(SUM(has_repeat_dx) AS DOUBLE) / COUNT(*)
                        ELSE 0
                    END AS repeat_rate
                FROM repeat_dx_per_enrollee
                GROUP BY provider_id
            ),
            visit_dates AS (
                SELECT DISTINCT
                    provider_id,
                    enrollee_id,
                    enc_date
                FROM claims_month
                WHERE enc_date IS NOT NULL
            ),
            short_interval_flags AS (
                SELECT
                    provider_id,
                    enrollee_id,
                    MAX(
                        CASE
                            WHEN next_date IS NOT NULL AND next_date <= enc_date + INTERVAL 14 DAY
                            THEN 1 ELSE 0
                        END
                    ) AS has_short_interval
                FROM (
                    SELECT
                        provider_id,
                        enrollee_id,
                        enc_date,
                        LEAD(enc_date) OVER (
                            PARTITION BY provider_id, enrollee_id
                            ORDER BY enc_date
                        ) AS next_date
                    FROM visit_dates
                ) v
                GROUP BY provider_id, enrollee_id
            ),
            short_interval_provider AS (
                SELECT
                    provider_id,
                    SUM(has_short_interval) AS short_interval_enrollees,
                    COUNT(*) AS total_enrollees,
                    CASE
                        WHEN COUNT(*) > 0 THEN CAST(SUM(has_short_interval) AS DOUBLE) / COUNT(*)
                        ELSE 0
                    END AS short_interval_rate
                FROM short_interval_flags
                GROUP BY provider_id
            ),
            scored AS (
                SELECT
                    pm.provider_id,
                    pm.unique_enrollees,
                    pm.total_visits,
                    pm.total_amount,
                    pm.vpe,
                    pm.cpe,
                    pm.cpv,
                    pm.provider_band,
                    COALESCE(rd.repeat_rate, 0) AS repeat_rate,
                    COALESCE(si.short_interval_rate, 0) AS short_interval_rate,
                    CASE
                        WHEN ps.std_vpe IS NOT NULL AND ps.std_vpe > 0
                             AND pm.vpe > ps.avg_vpe + 1.5 * ps.std_vpe
                        THEN 1 ELSE 0
                    END AS score_vpe,
                    CASE
                        WHEN ps.std_cpe IS NOT NULL AND ps.std_cpe > 0
                             AND pm.cpe > ps.avg_cpe + 2 * ps.std_cpe
                        THEN 2 ELSE 0
                    END AS score_cpe,
                    CASE
                        WHEN ps.std_cpv IS NOT NULL AND ps.std_cpv > 0
                             AND pm.cpv > ps.avg_cpv + 2 * ps.std_cpv
                        THEN 2 ELSE 0
                    END AS score_cpv,
                    CASE
                        WHEN COALESCE(rd.repeat_rate, 0) > 0.30 THEN 1
                        ELSE 0
                    END AS score_repeat_dx,
                    CASE
                        WHEN COALESCE(si.short_interval_rate, 0) > 0.10 THEN 1
                        ELSE 0
                    END AS score_short_interval
                FROM provider_with_band pm
                LEFT JOIN peer_stats ps ON pm.provider_band = ps.provider_band
                LEFT JOIN repeat_dx_provider rd ON pm.provider_id = rd.provider_id
                LEFT JOIN short_interval_provider si ON pm.provider_id = si.provider_id
            ),
            totals AS (
                SELECT
                    s.*,
                    (score_vpe + score_cpe + score_cpv + score_repeat_dx + score_short_interval) AS total_score
                FROM scored s
            )
            SELECT
                t.provider_id,
                TRIM(CAST(p.providername AS VARCHAR)) AS provider_name,
                t.provider_band,
                t.total_score
            FROM totals t
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
              ON TRIM(CAST(p.providerid AS VARCHAR)) = t.provider_id
            WHERE t.total_score >= 4
            ORDER BY t.total_score DESC, t.total_amount DESC;
            """

            rows = conn.execute(query).fetchall()
            for r in rows:
                entries.append({
                    "provider_id": r[0],
                    "provider_name": (r[1] or "").strip() or f"Provider {r[0]}",
                    "provider_band": r[2] or "Unspecified",
                    "year": y,
                    "month": m,
                    "total_score": int(r[3] or 0),
                })

        conn.close()

        return {
            "success": True,
            "entries": entries,
        }
    except Exception as e:
        logger.error(f"Error computing provider critical alert threshold history: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/critical-alerts/{provider_id}/6-month-breakdown")
async def get_provider_critical_alert_breakdown(provider_id: str):
    """
    Detailed Critical Alert breakdown for a single provider over the last 6 full calendar months.
    
    Uses encounterdatefrom as the date column for all metrics.
    
    Returns:
      - Total claims cost (6 months)
      - Total visits (6 months)
      - Total unique enrollees (6 months)
      - Provider band
      - Band / peer means & standard deviations for VPE and CPE
      - List of diagnoses that were repeated (>= 2 claims) in the 6‑month window
      - List of enrollees who had at least one pair of visits ≤ 14 days apart
      - Overall VPE, CPE, CPV for the provider over 6 months
    """
    try:
        conn = get_db_connection()
        sid = provider_id.replace("'", "''")
        eid = _enrollee_id_col()

        # Last 6 full calendar months (helper returns most recent first)
        months6 = _last_n_calendar_months(6)
        if not months6:
            conn.close()
            return {
                "success": True,
                "providers": [],
            }
        overall_start = months6[-1][2]
        overall_end = months6[0][3]

        # Get provider band
        band_row = conn.execute(
            """
            SELECT COALESCE(NULLIF(TRIM(CAST(bands AS VARCHAR)), ''), 'Unspecified') AS band
            FROM "AI DRIVEN DATA"."PROVIDERS"
            WHERE TRIM(CAST(providerid AS VARCHAR)) = ?
            """,
            [provider_id],
        ).fetchone()
        provider_band = (band_row[0] if band_row else "Unspecified") or "Unspecified"

        # 6‑month claims window for this provider
        claims_6m_q = f"""
        SELECT
            {eid} AS enrollee_id,
            DATE(c.encounterdatefrom) AS enc_date,
            c.diagnosiscode,
            COALESCE(c.approvedamount, 0) AS approvedamount,
            c.panumber
        FROM "AI DRIVEN DATA"."CLAIMS DATA" c
        WHERE ({_provider_id_eq_literal('c.nhisproviderid', sid)})
          AND c.encounterdatefrom IS NOT NULL
          AND c.encounterdatefrom >= DATE '{overall_start.isoformat()}'
          AND c.encounterdatefrom <= DATE '{overall_end.isoformat()}'
          AND {eid} IS NOT NULL
        """
        claims_df = conn.execute(claims_6m_q).fetchdf()

        if claims_df.empty:
            conn.close()
            return {
                "success": True,
                "provider_id": provider_id,
                "provider_band": provider_band,
                "window_start": overall_start.isoformat(),
                "window_end": overall_end.isoformat(),
                "total_amount": 0.0,
                "total_visits": 0,
                "unique_enrollees": 0,
                "vpe": 0.0,
                "cpe": 0.0,
                "cpv": 0.0,
                "peer_mean_vpe": 0.0,
                "peer_sd_vpe": 0.0,
                "peer_mean_cpe": 0.0,
                "peer_sd_cpe": 0.0,
                "peer_mean_cpv": 0.0,
                "peer_sd_cpv": 0.0,
                "repeated_diagnoses": [],
                "short_interval_enrollees": [],
            }

        # Compute visits & totals for this provider over 6 months using DuckDB
        conn.register("claims_tmp", claims_df)
        base_row = conn.execute(
            """
            WITH visits AS (
                SELECT
                    enrollee_id,
                    enc_date,
                    CASE
                        WHEN panumber IS NOT NULL THEN 'P|' || CAST(panumber AS VARCHAR)
                        ELSE 'N|' || enrollee_id || '|' || CAST(enc_date AS VARCHAR)
                    END AS visit_key,
                    approvedamount
                FROM claims_tmp
            )
            SELECT
                COUNT(DISTINCT v.enrollee_id) AS unique_enrollees,
                COUNT(DISTINCT v.visit_key) AS total_visits,
                COUNT(*) AS claim_lines,
                SUM(v.approvedamount) AS total_amount
            FROM visits v
            """
        ).fetchone()
        unique_enrollees = int(base_row[0] or 0)
        total_visits = int(base_row[1] or 0)
        total_amount = float(base_row[3] or 0.0)

        vpe = (total_visits / unique_enrollees) if unique_enrollees > 0 else 0.0
        cpe = (total_amount / unique_enrollees) if unique_enrollees > 0 else 0.0
        cpv = (total_amount / total_visits) if total_visits > 0 else 0.0

        # Peer stats (same 6‑month window, same band) across all providers
        peer_q = f"""
        WITH months6 AS (
            SELECT DATE '{overall_start.isoformat()}' AS start_d,
                   DATE '{overall_end.isoformat()}'   AS end_d
        ),
        all_claims AS (
            SELECT
                TRIM(CAST(c.nhisproviderid AS VARCHAR)) AS provider_id,
                {_enrollee_id_col()} AS enrollee_id,
                DATE(c.encounterdatefrom) AS enc_date,
                COALESCE(c.approvedamount, 0) AS approvedamount,
                c.panumber
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c, months6 mb
            WHERE c.encounterdatefrom IS NOT NULL
              AND c.encounterdatefrom >= mb.start_d
              AND c.encounterdatefrom <= mb.end_d
              AND {_enrollee_id_col()} IS NOT NULL
        ),
        visits AS (
            SELECT
                a.provider_id,
                a.enrollee_id,
                a.enc_date,
                CASE
                    WHEN a.panumber IS NOT NULL THEN 'P|' || CAST(a.panumber AS VARCHAR)
                    ELSE 'N|' || a.enrollee_id || '|' || CAST(a.enc_date AS VARCHAR)
                END AS visit_key,
                a.approvedamount
            FROM all_claims a
        ),
        provider_base AS (
            SELECT
                v.provider_id,
                COUNT(DISTINCT v.enrollee_id) AS unique_enrollees,
                COUNT(DISTINCT v.visit_key) AS total_visits,
                SUM(v.approvedamount) AS total_amount
            FROM visits v
            GROUP BY v.provider_id
        ),
        provider_with_band AS (
            SELECT
                pb.*,
                COALESCE(
                    NULLIF(TRIM(CAST(p.bands AS VARCHAR)), ''),
                    'Unspecified'
                ) AS provider_band
            FROM provider_base pb
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
              ON TRIM(CAST(p.providerid AS VARCHAR)) = pb.provider_id
        ),
        metrics AS (
            SELECT
                provider_id,
                provider_band,
                CASE
                    WHEN unique_enrollees > 0 THEN CAST(total_visits AS DOUBLE) / unique_enrollees
                    ELSE 0
                END AS vpe,
                CASE
                    WHEN unique_enrollees > 0 THEN total_amount / unique_enrollees
                    ELSE 0
                END AS cpe,
                CASE
                    WHEN total_visits > 0 THEN total_amount / total_visits
                    ELSE 0
                END AS cpv
            FROM provider_with_band
            WHERE unique_enrollees > 0
              AND total_visits > 0
        )
        SELECT
            AVG(vpe) AS avg_vpe,
            STDDEV_SAMP(vpe) AS std_vpe,
            AVG(cpe) AS avg_cpe,
            STDDEV_SAMP(cpe) AS std_cpe,
            AVG(cpv) AS avg_cpv,
            STDDEV_SAMP(cpv) AS std_cpv
        FROM metrics
        WHERE provider_band = ?
        """
        peer_row = conn.execute(peer_q, [provider_band]).fetchone()
        peer_mean_vpe = float(peer_row[0] or 0.0)
        peer_sd_vpe = float(peer_row[1] or 0.0)
        peer_mean_cpe = float(peer_row[2] or 0.0)
        peer_sd_cpe = float(peer_row[3] or 0.0)
        peer_mean_cpv = float(peer_row[4] or 0.0)
        peer_sd_cpv = float(peer_row[5] or 0.0)

        # Repeated diagnoses per month (within-month repeats, last 6 months)
        repeated_dx_q = """
        SELECT
            EXTRACT(YEAR FROM c.enc_date) AS year,
            EXTRACT(MONTH FROM c.enc_date) AS month,
            c.diagnosiscode,
            COALESCE(d.diagnosisdesc, c.diagnosiscode, 'Unknown') AS diagnosis_name,
            COUNT(*) AS cnt
        FROM claims_tmp c
        LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d
          ON c.diagnosiscode = d.diagnosiscode
        WHERE c.diagnosiscode IS NOT NULL
        GROUP BY
            EXTRACT(YEAR FROM c.enc_date),
            EXTRACT(MONTH FROM c.enc_date),
            c.diagnosiscode,
            d.diagnosisdesc
        HAVING COUNT(*) >= 2
        ORDER BY year, month, cnt DESC
        """
        repeated_rows = conn.execute(repeated_dx_q).fetchall()
        repeated_diagnoses = [
            {
                "year": int(r[0] or 0),
                "month": int(r[1] or 0),
                "diagnosis_code": r[2],
                "diagnosis_name": r[3],
                "count": int(r[4] or 0),
            }
            for r in repeated_rows
        ]

        # Enrollees with at least one pair of visits ≤ 14 days apart (6‑month window)
        short_interval_q = """
        WITH visits AS (
            SELECT DISTINCT
                enrollee_id,
                enc_date
            FROM claims_tmp
            WHERE enc_date IS NOT NULL
        ),
        seq AS (
            SELECT
                enrollee_id,
                enc_date,
                LEAD(enc_date) OVER (
                    PARTITION BY enrollee_id
                    ORDER BY enc_date
                ) AS next_date
            FROM visits
        ),
        flags AS (
            SELECT DISTINCT enrollee_id
            FROM seq
            WHERE next_date IS NOT NULL
              AND next_date <= enc_date + INTERVAL 14 DAY
        )
        SELECT enrollee_id FROM flags
        """
        short_rows = conn.execute(short_interval_q).fetchall()
        short_interval_enrollees = [r[0] for r in short_rows]

        conn.close()

        return {
            "success": True,
            "provider_id": provider_id,
            "provider_band": provider_band,
            "window_start": overall_start.isoformat(),
            "window_end": overall_end.isoformat(),
            "total_amount": total_amount,
            "total_visits": total_visits,
            "unique_enrollees": unique_enrollees,
            "vpe": vpe,
            "cpe": cpe,
            "cpv": cpv,
            "peer_mean_vpe": peer_mean_vpe,
            "peer_sd_vpe": peer_sd_vpe,
            "peer_mean_cpe": peer_mean_cpe,
            "peer_sd_cpe": peer_sd_cpe,
            "peer_mean_cpv": peer_mean_cpv,
            "peer_sd_cpv": peer_sd_cpv,
            "repeated_diagnoses": repeated_diagnoses,
            "short_interval_enrollees": short_interval_enrollees,
        }
    except Exception as e:
        logger.error(f"Error computing provider critical alert breakdown: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
