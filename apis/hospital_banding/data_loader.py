"""
Data loading and DuckDB queries for Hospital Banding API.
All queries extracted from adv_hosp_band2.py and adv_hosp_claims_band.py.
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = os.getenv(
    "DUCKDB_PATH",
    "/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb",
)
TARIFF_PATH = os.getenv(
    "STANDARD_TARIFF_PATH",
    "/Users/kenechukwuchukwuka/Downloads/DLT/REALITY TARIFF_Sheet1.csv",
)


def get_conn(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=read_only)


def load_standard_tariff() -> pd.DataFrame:
    """
    Load the standard band-threshold tariff (REALITY TARIFF_Sheet1.csv).
    The CSV has a blank header row at row 0; actual headers are at row 1.
    Columns: procedurecode, procedure name, band_d, band_c, band_b, band_a, band_special
    """
    path = Path(TARIFF_PATH)
    if not path.exists():
        raise FileNotFoundError(f"Standard tariff not found: {TARIFF_PATH}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, skiprows=1)
    else:
        df = pd.read_excel(path)

    required = ["procedurecode", "band_a", "band_b", "band_c", "band_d", "band_special"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Standard tariff missing columns: {missing}")

    # Rename 'procedure name' -> 'proceduredesc' for consistency
    if "procedure name" in df.columns:
        df = df.rename(columns={"procedure name": "proceduredesc"})

    return df


def load_claims_stats(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load P25/P50/P75/P90/P95 statistics for every procedure from CLAIMS DATA.
    Used for: (a) frequency weighting in banding, (b) fraud risk detection.
    """
    query = """
    WITH raw AS (
        SELECT
            LOWER(TRIM(code))  AS procedurecode,
            chargeamount
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE code IS NOT NULL
          AND chargeamount > 0
    ),
    pcts AS (
        SELECT
            procedurecode,
            APPROX_QUANTILE(chargeamount, 0.01) AS p01,
            APPROX_QUANTILE(chargeamount, 0.90) AS p90,
            APPROX_QUANTILE(chargeamount, 0.95) AS p95,
            APPROX_QUANTILE(chargeamount, 0.99) AS p99,
            COUNT(*)                             AS total_count
        FROM raw
        GROUP BY procedurecode
    )
    SELECT
        r.procedurecode,
        p.p90,
        p.p95,
        AVG(r.chargeamount)    AS mean,
        STDDEV(r.chargeamount) AS std,
        COUNT(*)               AS count
    FROM raw r
    JOIN pcts p ON r.procedurecode = p.procedurecode
    WHERE r.chargeamount >= p.p01
      AND r.chargeamount <= p.p99
    GROUP BY r.procedurecode, p.p90, p.p95
    HAVING COUNT(*) >= 5
    ORDER BY count DESC
    """
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        logger.warning(f"Could not load claims stats: {e}")
        return pd.DataFrame()


def load_quality_metrics(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load provider quality scores derived from:
    - 30-day readmission rate (weighted ×2)
    - High-cost outlier rate (procedures above P95 for that code)
    - Claim denial rate
    Quality score = 100 − penalties (higher is better, range 0–100).
    Only providers with ≥50 claims since 2024-01-01 are included.
    """
    query = """
    WITH readmission_check AS (
        SELECT
            nhisproviderid,
            enrollee_id,
            diagnosiscode,
            datesubmitted,
            LAG(datesubmitted) OVER (
                PARTITION BY nhisproviderid, enrollee_id, diagnosiscode
                ORDER BY datesubmitted
            ) AS prev_visit
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE nhisproviderid IS NOT NULL
          AND datesubmitted >= DATE '2024-01-01'
          AND enrollee_id    IS NOT NULL
          AND diagnosiscode  IS NOT NULL
    ),
    readmissions AS (
        SELECT
            nhisproviderid,
            COUNT(DISTINCT CASE
                WHEN prev_visit IS NOT NULL
                 AND date_diff('day', prev_visit, datesubmitted) <= 30
                THEN enrollee_id
            END) AS readmission_count
        FROM readmission_check
        GROUP BY nhisproviderid
    ),
    outcomes AS (
        SELECT
            c.nhisproviderid,
            p.providername,
            COUNT(DISTINCT c.enrollee_id)                       AS patient_count,
            COUNT(*)                                            AS claim_count,
            AVG(c.chargeamount)                                 AS avg_charge,
            COUNT(CASE WHEN c.chargeamount > (
                SELECT APPROX_QUANTILE(chargeamount, 0.95)
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c2
                WHERE c2.code = c.code
            ) THEN 1 END)                                       AS high_cost_outlier_count,
            SUM(CASE WHEN c.deniedamount > 0 THEN 1 ELSE 0 END)::FLOAT
                / NULLIF(COUNT(*), 0) * 100                     AS denial_rate
        FROM "AI DRIVEN DATA"."CLAIMS DATA" c
        LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
               ON c.nhisproviderid = p.providerid
        WHERE c.nhisproviderid IS NOT NULL
          AND c.datesubmitted >= DATE '2024-01-01'
          AND (p.providername IS NULL OR LOWER(p.providername) NOT LIKE '%nhis%')
        GROUP BY c.nhisproviderid, p.providername
        HAVING COUNT(*) >= 50
    )
    SELECT
        o.*,
        COALESCE(r.readmission_count, 0) AS readmission_count,
        GREATEST(0, 100
            - (COALESCE(r.readmission_count, 0)::FLOAT / NULLIF(o.patient_count, 0) * 100 * 2)
            - (o.high_cost_outlier_count::FLOAT / NULLIF(o.claim_count, 0) * 100)
            - (o.denial_rate * 0.5)
        ) AS quality_score
    FROM outcomes o
    LEFT JOIN readmissions r ON o.nhisproviderid = r.nhisproviderid
    ORDER BY quality_score DESC
    """
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        logger.warning(f"Could not load quality metrics: {e}")
        return pd.DataFrame()


def get_providers_list(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    All providers that have at least one active tariff entry in the DB.
    Returns: providerid, providername, current_band
    """
    query = """
    SELECT DISTINCT
        CAST(p.providerid AS VARCHAR)                          AS providerid,
        TRIM(p.providername)                                   AS providername,
        COALESCE(NULLIF(TRIM(p.bands), ''), 'Unspecified')    AS current_band
    FROM "AI DRIVEN DATA"."PROVIDERS" p
    INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt ON p.protariffid = pt.protariffid
    INNER JOIN "AI DRIVEN DATA"."TARIFF" t           ON pt.tariffid    = t.tariffid
    WHERE t.tariffamount  > 0
      AND t.procedurecode IS NOT NULL
    ORDER BY p.providername
    """
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        logger.error(f"Could not load providers list: {e}")
        return pd.DataFrame()


def get_provider_official_tariff(
    conn: duckdb.DuckDBPyConnection,
    provider_id: str,
) -> Optional[pd.DataFrame]:
    """
    Fetch the provider's published tariff from the DB (procedurecode, tariffamount).
    This is what the provider *says* they charge (official published rate).
    """
    query = """
    SELECT
        LOWER(TRIM(t.procedurecode))  AS procedurecode,
        CAST(t.tariffamount AS DOUBLE) AS tariffamount
    FROM "AI DRIVEN DATA"."PROVIDERS" p
    INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt ON p.protariffid = pt.protariffid
    INNER JOIN "AI DRIVEN DATA"."TARIFF" t           ON pt.tariffid    = t.tariffid
    WHERE CAST(p.providerid AS VARCHAR) = ?
      AND t.tariffamount  > 0
      AND t.procedurecode IS NOT NULL
    ORDER BY t.procedurecode
    """
    try:
        df = conn.execute(query, [str(provider_id).strip()]).fetchdf()
        if df.empty:
            return None
        df["procedurecode"] = (
            df["procedurecode"].astype(str).str.strip().str.lower().str.replace(" ", "")
        )
        return df
    except Exception as e:
        logger.error(f"Failed to get official tariff for provider {provider_id}: {e}")
        return None


def build_reality_tariff(
    conn: duckdb.DuckDBPyConnection,
    provider_id: str,
    standard_df: pd.DataFrame,
    lookback_months: int = 6,
) -> pd.DataFrame:
    """
    Build a hybrid tariff for a provider that reflects actual billing behaviour:
      - For procedures WITH claims in the lookback window  → use AVG(approvedamount)
      - For procedures WITHOUT claims                       → use official published tariff
    This exposes the gap between what a provider publishes and what they actually bill.

    Returns DataFrame with columns:
      procedurecode, proceduredesc, reality_price, price_source (CLAIMS|TARIFF),
      claims_count, official_tariff_price, price_difference, price_difference_pct
    """
    cutoff = (datetime.now() - timedelta(days=lookback_months * 30)).strftime("%Y-%m-%d")

    def _norm(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v).strip().lower().replace(" ", "")

    try:
        # Step 1: claims-based avg approved amount for this provider
        claims_df = conn.execute(
            f"""
            SELECT
                LOWER(TRIM(c.code)) AS procedurecode,
                COUNT(*)            AS claims_count,
                AVG(c.approvedamount) AS avg_approved
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE CAST(c.nhisproviderid AS VARCHAR) = '{str(provider_id)}'
              AND c.datesubmitted >= DATE '{cutoff}'
              AND c.code          IS NOT NULL
              AND c.approvedamount > 0
            GROUP BY LOWER(TRIM(c.code))
            HAVING COUNT(*) >= 1
            """
        ).fetchdf()

        # Step 2: official tariff prices for this provider
        tariff_id_df = conn.execute(
            f"""
            SELECT DISTINCT pt.tariffid
            FROM "AI DRIVEN DATA"."PROVIDERS" p
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt ON p.protariffid = pt.protariffid
            WHERE CAST(p.providerid AS VARCHAR) = '{str(provider_id)}'
            LIMIT 1
            """
        ).fetchdf()

        if tariff_id_df.empty:
            return pd.DataFrame()

        tariff_id = int(tariff_id_df["tariffid"].iloc[0])

        official_df = conn.execute(
            f"""
            SELECT
                LOWER(TRIM(t.procedurecode)) AS procedurecode,
                t.tariffamount               AS official_price
            FROM "AI DRIVEN DATA"."TARIFF" t
            WHERE t.tariffid     = {tariff_id}
              AND t.tariffamount > 0
            """
        ).fetchdf()

    except Exception as e:
        logger.error(f"Reality tariff query failed for {provider_id}: {e}")
        return pd.DataFrame()

    # Build lookup dicts
    claims_dict = (
        {_norm(r["procedurecode"]): r for _, r in claims_df.iterrows()}
        if not claims_df.empty else {}
    )
    official_dict = (
        {_norm(r["procedurecode"]): float(r["official_price"]) for _, r in official_df.iterrows()}
        if not official_df.empty else {}
    )

    # Step 3: for each standard-tariff procedure, pick price source
    hybrid = []
    for _, std_row in standard_df.iterrows():
        proc_code = _norm(std_row["procedurecode"])
        proc_desc = std_row.get("proceduredesc", "N/A")

        if proc_code in claims_dict:
            cr = claims_dict[proc_code]
            avg = float(cr["avg_approved"])
            cnt = int(cr["claims_count"])
            off = official_dict.get(proc_code)
            diff = (avg - off) if off else None
            diff_pct = ((diff / off) * 100) if (off and diff is not None) else None

            hybrid.append({
                "procedurecode":       proc_code,
                "proceduredesc":       proc_desc,
                "reality_price":       avg,
                "price_source":        "CLAIMS",
                "claims_count":        cnt,
                "official_tariff_price": off,
                "price_difference":    diff,
                "price_difference_pct": diff_pct,
            })

        elif proc_code in official_dict:
            off = official_dict[proc_code]
            hybrid.append({
                "procedurecode":       proc_code,
                "proceduredesc":       proc_desc,
                "reality_price":       off,
                "price_source":        "TARIFF",
                "claims_count":        0,
                "official_tariff_price": off,
                "price_difference":    0.0,
                "price_difference_pct": 0.0,
            })

    return pd.DataFrame(hybrid) if hybrid else pd.DataFrame()
