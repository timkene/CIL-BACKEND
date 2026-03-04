"""
Fraud Scoring API - Database Layer
=====================================
All DuckDB queries.  Every function accepts an open duckdb.Connection.

Visit definition (enforced throughout):
  - Rows WITH  real panumber → visit_id = panumber        (1 PA = 1 visit)
  - Rows WITHOUT panumber    → visit_id = enrollee+date   (1 date = 1 visit)
"""

import duckdb
import pandas as pd
from typing import Optional, Tuple
from provider_fraud_config import DB_SCHEMA, DRUG_CODE_PREFIXES, MIN_PEER_ENROLLEES


# ─── helpers ──────────────────────────────────────────────────────────────────
def _drug_case(alias: str = "drug_cost") -> str:
    """SQL CASE expression that sums drug-coded procedure lines."""
    conditions = " OR ".join(
        f"code::VARCHAR LIKE '{p}%'" for p in DRUG_CODE_PREFIXES
    )
    return f"SUM(CASE WHEN {conditions} THEN approvedamount ELSE 0 END) AS {alias}"


def _visit_id_expr() -> str:
    """
    Returns a SQL expression for a unified visit identifier per row.
    Real panumber  → use panumber
    No panumber    → concat enrollee_id + '|' + encounterdatefrom::VARCHAR
    'No panumber' means NULL, '0', or '0.0'
    """
    return """
        CASE
            WHEN panumber IS NOT NULL
                 AND TRIM(panumber::VARCHAR) != ''
                 AND panumber::VARCHAR NOT IN ('0','0.0')
            THEN panumber::VARCHAR
            ELSE enrollee_id::VARCHAR || '|' || encounterdatefrom::VARCHAR
        END
    """


# ─── provider lookup ──────────────────────────────────────────────────────────
def resolve_provider(
    conn: duckdb.DuckDBPyConnection,
    provider_id: Optional[str],
    provider_name: Optional[str],
) -> Optional[dict]:
    """
    Resolve provider_id or provider_name → {provider_id, provider_name, band, state}.
    ID takes priority over name.
    """
    schema = DB_SCHEMA
    if provider_id:
        sql = f"""
            SELECT DISTINCT providerid, providername, bands, statename
            FROM "{schema}"."PROVIDERS"
            WHERE providerid::VARCHAR = ?
            LIMIT 1
        """
        rows = conn.execute(sql, [str(provider_id)]).fetchdf()
    else:
        sql = f"""
            SELECT DISTINCT providerid, providername, bands, statename
            FROM "{schema}"."PROVIDERS"
            WHERE LOWER(providername) LIKE LOWER(?)
              AND isvisible = true
            ORDER BY providername
            LIMIT 1
        """
        rows = conn.execute(sql, [f"%{provider_name}%"]).fetchdf()

    if rows.empty:
        return None

    row = rows.iloc[0]
    return {
        "provider_id":   str(row["providerid"]),
        "provider_name": row["providername"],
        "band":          row["bands"] or "Unknown",
        "state":         row.get("statename"),
    }


# ─── raw metrics for a single provider ───────────────────────────────────────
def get_provider_raw_metrics(
    conn: duckdb.DuckDBPyConnection,
    provider_id: str,
    start_date: str,
    end_date: str,
) -> Optional[pd.DataFrame]:
    """
    Returns a single-row DataFrame with:
    unique_enrollees, total_cost, drug_cost,
    pa_visits, no_pa_visits, total_visits
    """
    schema   = DB_SCHEMA
    drug_sql = _drug_case()
    sql = f"""
        WITH claims AS (
            SELECT
                enrollee_id,
                panumber::VARCHAR                                       AS panumber,
                encounterdatefrom,
                approvedamount,
                code::VARCHAR                                           AS code,
                CASE
                    WHEN panumber IS NOT NULL
                         AND TRIM(panumber::VARCHAR) != ''
                         AND panumber::VARCHAR NOT IN ('0','0.0')
                    THEN 'real'
                    ELSE 'no_pa'
                END                                                     AS pa_type
            FROM "{schema}"."CLAIMS DATA"
            WHERE nhisproviderid = ?
              AND encounterdatefrom BETWEEN ? AND ?
        )
        SELECT
            COUNT(DISTINCT enrollee_id)                                AS unique_enrollees,
            SUM(approvedamount)                                        AS total_cost,
            {drug_sql},
            COUNT(DISTINCT CASE WHEN pa_type='real'  THEN panumber  END) AS pa_visits,
            COUNT(DISTINCT CASE WHEN pa_type='no_pa'
                                THEN enrollee_id||'|'||encounterdatefrom::VARCHAR
                           END)                                        AS no_pa_visits
        FROM claims
    """
    return conn.execute(sql, [provider_id, start_date, end_date]).fetchdf()


# ─── peer-band benchmarks (Tukey fence) ───────────────────────────────────────
def get_band_benchmarks(
    conn: duckdb.DuckDBPyConnection,
    band: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    For every active, old-format hospital in `band`,
    compute CPE / CPV / VPE / drug_ratio,
    then return Q1, median, Q3, IQR, Tukey threshold for each.
    Only hospitals with ≥ MIN_PEER_ENROLLEES unique enrollees are included.
    """
    schema   = DB_SCHEMA
    drug_sql = _drug_case()
    min_e    = MIN_PEER_ENROLLEES

    sql = f"""
        WITH band_providers AS (
            SELECT DISTINCT providerid::VARCHAR AS providerid
            FROM "{schema}"."PROVIDERS"
            WHERE bands = ?
              AND isvisible = true
              AND providerid NOT LIKE '000%'
        ),
        claims AS (
            SELECT
                c.nhisproviderid,
                c.enrollee_id,
                c.panumber::VARCHAR                             AS panumber,
                c.encounterdatefrom,
                c.approvedamount,
                c.code::VARCHAR                                 AS code
            FROM "{schema}"."CLAIMS DATA" c
            INNER JOIN band_providers p ON c.nhisproviderid = p.providerid
            WHERE c.encounterdatefrom BETWEEN ? AND ?
        ),
        prov_metrics AS (
            SELECT
                nhisproviderid,
                COUNT(DISTINCT enrollee_id)                     AS unique_enrollees,
                SUM(approvedamount)                             AS total_cost,
                {drug_sql},
                COUNT(DISTINCT CASE
                    WHEN panumber IS NOT NULL
                         AND TRIM(panumber) != ''
                         AND panumber NOT IN ('0','0.0')
                    THEN panumber END)
                +
                COUNT(DISTINCT CASE
                    WHEN panumber IS NULL
                         OR TRIM(panumber) = ''
                         OR panumber IN ('0','0.0')
                    THEN enrollee_id||'|'||encounterdatefrom::VARCHAR
                    END)                                        AS total_visits
            FROM claims
            GROUP BY nhisproviderid
            HAVING COUNT(DISTINCT enrollee_id) >= {min_e}
        ),
        calc AS (
            SELECT
                nhisproviderid,
                total_cost / unique_enrollees                   AS cpe,
                total_cost / NULLIF(total_visits, 0)            AS cpv,
                total_visits::DECIMAL / unique_enrollees        AS vpe,
                drug_cost  / NULLIF(total_cost, 0) * 100        AS drug_ratio
            FROM prov_metrics
        )
        SELECT
            COUNT(*)                                                        AS peer_count,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cpe)              AS cpe_q1,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY cpe)              AS cpe_median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cpe)              AS cpe_q3,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cpv)              AS cpv_q1,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY cpv)              AS cpv_median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cpv)              AS cpv_q3,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vpe)              AS vpe_q1,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY vpe)              AS vpe_median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vpe)              AS vpe_q3,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY drug_ratio)       AS drug_ratio_median
        FROM calc
    """
    return conn.execute(sql, [band, start_date, end_date]).fetchdf()


# ─── behavioral metrics ───────────────────────────────────────────────────────
def get_dx_repeat_metrics(
    conn: duckdb.DuckDBPyConnection,
    provider_id: str,
    start_date: str,
    end_date: str,
    window_days: int = 14,
) -> pd.DataFrame:
    """
    Diagnosis Repeat Rate.
    Correct logic:  same enrollee + same diagnosis + DIFFERENT visit_id within N days.
    visit_id = panumber (if real) or enrollee+date (if no PA).
    Returns: repeated_pairs, total_pairs, repeat_rate_pct, top_diagnoses (JSON-like col).
    """
    schema = DB_SCHEMA
    sql = f"""
        WITH claims AS (
            SELECT
                enrollee_id::VARCHAR                                    AS enrollee_id,
                diagnosiscode::VARCHAR                                  AS diagnosiscode,
                encounterdatefrom,
                CASE
                    WHEN panumber IS NOT NULL
                         AND TRIM(panumber::VARCHAR) != ''
                         AND panumber::VARCHAR NOT IN ('0','0.0')
                    THEN panumber::VARCHAR
                    ELSE enrollee_id::VARCHAR||'|'||encounterdatefrom::VARCHAR
                END                                                     AS visit_id
            FROM "{schema}"."CLAIMS DATA"
            WHERE nhisproviderid = ?
              AND encounterdatefrom BETWEEN ? AND ?
              AND diagnosiscode IS NOT NULL
        ),
        -- collapse to 1 row per (enrollee, diagnosis, visit)
        visit_dx AS (
            SELECT DISTINCT enrollee_id, diagnosiscode, visit_id, encounterdatefrom
            FROM claims
        ),
        -- pairs: same enrollee, same dx, DIFFERENT visit, within window
        pairs AS (
            SELECT
                a.enrollee_id,
                a.diagnosiscode,
                a.visit_id AS visit_a,
                b.visit_id AS visit_b
            FROM visit_dx a
            JOIN visit_dx b
              ON  a.enrollee_id    = b.enrollee_id
              AND a.diagnosiscode  = b.diagnosiscode
              AND a.visit_id      != b.visit_id
              AND b.encounterdatefrom > a.encounterdatefrom
              AND b.encounterdatefrom <= a.encounterdatefrom + INTERVAL '{window_days} days'
        ),
        repeated_summary AS (
            SELECT
                diagnosiscode,
                COUNT(*)                                                AS repeat_count
            FROM pairs
            GROUP BY diagnosiscode
            ORDER BY repeat_count DESC
        ),
        totals AS (
            SELECT
                COUNT(DISTINCT enrollee_id||'|'||diagnosiscode)         AS total_pairs
            FROM visit_dx
        ),
        repeated_total AS (
            SELECT COUNT(DISTINCT enrollee_id||'|'||diagnosiscode)      AS repeated_pairs
            FROM pairs
        )
        SELECT
            (SELECT repeated_pairs FROM repeated_total)                 AS repeated_pairs,
            (SELECT total_pairs    FROM totals)                         AS total_pairs,
            ROUND(
                100.0 *
                (SELECT repeated_pairs FROM repeated_total) /
                NULLIF((SELECT total_pairs FROM totals), 0),
            2)                                                          AS repeat_rate_pct
    """
    summary = conn.execute(sql, [provider_id, start_date, end_date]).fetchdf()

    # Top repeated diagnoses separately
    top_sql = f"""
        WITH claims AS (
            SELECT
                enrollee_id::VARCHAR                                    AS enrollee_id,
                diagnosiscode::VARCHAR                                  AS diagnosiscode,
                encounterdatefrom,
                CASE
                    WHEN panumber IS NOT NULL
                         AND TRIM(panumber::VARCHAR) != ''
                         AND panumber::VARCHAR NOT IN ('0','0.0')
                    THEN panumber::VARCHAR
                    ELSE enrollee_id::VARCHAR||'|'||encounterdatefrom::VARCHAR
                END                                                     AS visit_id
            FROM "{schema}"."CLAIMS DATA"
            WHERE nhisproviderid = ?
              AND encounterdatefrom BETWEEN ? AND ?
              AND diagnosiscode IS NOT NULL
        ),
        visit_dx AS (
            SELECT DISTINCT enrollee_id, diagnosiscode, visit_id, encounterdatefrom FROM claims
        ),
        pairs AS (
            SELECT a.diagnosiscode
            FROM visit_dx a
            JOIN visit_dx b
              ON  a.enrollee_id   = b.enrollee_id
              AND a.diagnosiscode = b.diagnosiscode
              AND a.visit_id     != b.visit_id
              AND b.encounterdatefrom > a.encounterdatefrom
              AND b.encounterdatefrom <= a.encounterdatefrom + INTERVAL '{window_days} days'
        )
        SELECT
            p.diagnosiscode,
            COALESCE(d.diagnosisdesc, 'Unknown') AS diagnosisdesc,
            COUNT(*)                              AS repeat_count
        FROM pairs p
        LEFT JOIN "{schema}"."DIAGNOSIS" d ON p.diagnosiscode = d.diagnosiscode
        GROUP BY p.diagnosiscode, d.diagnosisdesc
        ORDER BY repeat_count DESC
        LIMIT 8
    """
    top_dx = conn.execute(top_sql, [provider_id, start_date, end_date]).fetchdf()

    return summary, top_dx


def get_short_interval_metrics(
    conn: duckdb.DuckDBPyConnection,
    provider_id: str,
    start_date: str,
    end_date: str,
    window_days: int = 14,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Short Visit Interval.
    Correct logic:
      - For PA rows:    visit date = encounterdatefrom of distinct panumber
      - For no-PA rows: visit date = encounterdatefrom of distinct enrollee+date
    Compute avg gap between consecutive visits per enrollee.
    Flag if avg gap < window_days.
    Returns: (summary_df, bucket_df)
    """
    schema = DB_SCHEMA
    sql = f"""
        WITH claims AS (
            SELECT
                enrollee_id::VARCHAR                                    AS enrollee_id,
                encounterdatefrom,
                CASE
                    WHEN panumber IS NOT NULL
                         AND TRIM(panumber::VARCHAR) != ''
                         AND panumber::VARCHAR NOT IN ('0','0.0')
                    THEN panumber::VARCHAR
                    ELSE enrollee_id::VARCHAR||'|'||encounterdatefrom::VARCHAR
                END                                                     AS visit_id
            FROM "{schema}"."CLAIMS DATA"
            WHERE nhisproviderid = ?
              AND encounterdatefrom BETWEEN ? AND ?
        ),
        -- one row per unique visit per enrollee
        visits AS (
            SELECT DISTINCT enrollee_id, visit_id, encounterdatefrom
            FROM claims
        ),
        -- lag to get previous visit date per enrollee
        with_prev AS (
            SELECT
                enrollee_id,
                encounterdatefrom                                       AS visit_date,
                LAG(encounterdatefrom)
                    OVER (PARTITION BY enrollee_id ORDER BY encounterdatefrom) AS prev_date
            FROM visits
        ),
        gaps AS (
            SELECT
                enrollee_id,
                DATEDIFF('day', prev_date, visit_date)                  AS gap_days
            FROM with_prev
            WHERE prev_date IS NOT NULL
        ),
        enrollee_avg AS (
            SELECT enrollee_id, AVG(gap_days) AS avg_gap
            FROM gaps
            WHERE gap_days > 0
            GROUP BY enrollee_id
        )
        SELECT
            COUNT(*)                                                    AS multi_visit_enrollees,
            COUNT(CASE WHEN avg_gap < {window_days} THEN 1 END)         AS short_interval_enrollees,
            ROUND(
                100.0 *
                COUNT(CASE WHEN avg_gap < {window_days} THEN 1 END) /
                NULLIF(COUNT(*), 0),
            1)                                                          AS short_interval_pct,
            ROUND(AVG(CASE WHEN avg_gap < {window_days} THEN avg_gap END), 2)
                                                                        AS avg_gap_short
        FROM enrollee_avg
    """
    summary = conn.execute(sql, [provider_id, start_date, end_date]).fetchdf()

    bucket_sql = f"""
        WITH claims AS (
            SELECT
                enrollee_id::VARCHAR                                    AS enrollee_id,
                encounterdatefrom,
                CASE
                    WHEN panumber IS NOT NULL
                         AND TRIM(panumber::VARCHAR) != ''
                         AND panumber::VARCHAR NOT IN ('0','0.0')
                    THEN panumber::VARCHAR
                    ELSE enrollee_id::VARCHAR||'|'||encounterdatefrom::VARCHAR
                END                                                     AS visit_id
            FROM "{schema}"."CLAIMS DATA"
            WHERE nhisproviderid = ?
              AND encounterdatefrom BETWEEN ? AND ?
        ),
        visits AS (
            SELECT DISTINCT enrollee_id, visit_id, encounterdatefrom FROM claims
        ),
        with_prev AS (
            SELECT enrollee_id,
                   encounterdatefrom,
                   LAG(encounterdatefrom)
                       OVER (PARTITION BY enrollee_id ORDER BY encounterdatefrom) AS prev_date
            FROM visits
        ),
        gaps AS (
            SELECT enrollee_id,
                   DATEDIFF('day', prev_date, encounterdatefrom) AS gap_days
            FROM with_prev WHERE prev_date IS NOT NULL
        ),
        enrollee_avg AS (
            SELECT enrollee_id, AVG(gap_days) AS avg_gap
            FROM gaps WHERE gap_days > 0
            GROUP BY enrollee_id
        )
        SELECT
            CASE
                WHEN avg_gap <  3  THEN '<3 days'
                WHEN avg_gap <  7  THEN '3-7 days'
                WHEN avg_gap < 14  THEN '7-14 days'
                ELSE '14+ days'
            END                                                         AS bucket,
            COUNT(*)                                                    AS enrollee_count,
            ROUND(AVG(avg_gap), 1)                                      AS mean_gap_days
        FROM enrollee_avg
        GROUP BY bucket
        ORDER BY mean_gap_days
    """
    buckets = conn.execute(bucket_sql, [provider_id, start_date, end_date]).fetchdf()

    return summary, buckets