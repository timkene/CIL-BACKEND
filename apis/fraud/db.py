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
from .config import DB_SCHEMA, DRUG_CODE_PREFIXES, MIN_PEER_ENROLLEES


# ─── helpers ──────────────────────────────────────────────────────────────────
def _normalize_pid(pid: str) -> str:
    """Strip leading zeros from numeric provider IDs.
    PROVIDERS table stores '000022164'; CLAIMS DATA nhisproviderid stores '22164'.
    """
    try:
        return str(int(pid))
    except (ValueError, TypeError):
        return pid


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
    pa_visits, no_pa_visits, unclaimed_pa_visits, total_visits

    total_cost = claims (encounterdatefrom in period) + unclaimed PA (PA granted
    where panumber has no matching claim encounterdatefrom in the same period).
    This matches the company-wide total medical cost definition.
    """
    schema      = DB_SCHEMA
    drug_sql    = _drug_case()
    orig_pid    = provider_id                  # PA DATA uses '000022164' format
    provider_id = _normalize_pid(provider_id)  # CLAIMS DATA uses '22164' format
    sql = f"""
        WITH claims AS (
            SELECT
                enrollee_id::VARCHAR                                    AS enrollee_id,
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
        ),
        unclaimed_pa AS (
            -- PA authorized in period that never produced a claim (encounterdatefrom in period)
            SELECT
                pa."IID"::VARCHAR                                       AS enrollee_id,
                pa.panumber::VARCHAR                                    AS panumber,
                CAST(pa.requestdate AS DATE)                            AS encounterdatefrom,
                pa.granted                                              AS approvedamount
            FROM "{schema}"."PA DATA" pa
            WHERE pa.providerid::VARCHAR = ?
              AND pa.pastatus = 'AUTHORIZED'
              AND pa.panumber IS NOT NULL
              AND CAST(pa.requestdate AS DATE) BETWEEN ? AND ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM "{schema}"."CLAIMS DATA" cd
                  WHERE TRY_CAST(cd.panumber AS BIGINT) = TRY_CAST(pa.panumber AS BIGINT)
                    AND CAST(cd.encounterdatefrom AS DATE) BETWEEN ? AND ?
              )
        ),
        all_activity AS (
            SELECT enrollee_id, panumber, encounterdatefrom, approvedamount, code, pa_type
            FROM claims
            UNION ALL
            SELECT enrollee_id, panumber, encounterdatefrom, approvedamount,
                   NULL AS code,
                   'unclaimed_pa' AS pa_type
            FROM unclaimed_pa
        )
        SELECT
            COUNT(DISTINCT enrollee_id)                                AS unique_enrollees,
            SUM(approvedamount)                                        AS total_cost,
            {drug_sql},
            COUNT(DISTINCT CASE WHEN pa_type='real'         THEN panumber END) AS pa_visits,
            COUNT(DISTINCT CASE WHEN pa_type='no_pa'
                                THEN enrollee_id||'|'||encounterdatefrom::VARCHAR
                           END)                                        AS no_pa_visits,
            COUNT(DISTINCT CASE WHEN pa_type='unclaimed_pa' THEN panumber END) AS unclaimed_pa_visits
        FROM all_activity
    """
    return conn.execute(
        sql,
        [provider_id, start_date, end_date,   # claims filter
         orig_pid, start_date, end_date,       # unclaimed_pa providerid + requestdate
         start_date, end_date],                # NOT EXISTS encounterdatefrom
    ).fetchdf()


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
            -- Both old-format (000...) and regular IDs in the band
            SELECT DISTINCT
                providerid::VARCHAR                             AS providerid,
                LPAD(
                    LTRIM(providerid::VARCHAR, '0'),
                    1, '0'
                )                                               AS providerid_short,
                providerid::VARCHAR                             AS providerid_orig
            FROM "{schema}"."PROVIDERS"
            WHERE bands = ?
              AND isvisible = true
        ),
        claims AS (
            SELECT
                c.nhisproviderid,
                c.enrollee_id::VARCHAR                          AS enrollee_id,
                c.panumber::VARCHAR                             AS panumber,
                c.encounterdatefrom,
                c.approvedamount,
                c.code::VARCHAR                                 AS code
            FROM "{schema}"."CLAIMS DATA" c
            WHERE c.encounterdatefrom BETWEEN ? AND ?
              AND EXISTS (
                  SELECT 1 FROM band_providers bp
                  WHERE TRY_CAST(c.nhisproviderid AS BIGINT) = TRY_CAST(bp.providerid AS BIGINT)
              )
        ),
        unclaimed_pa AS (
            -- PA authorized in period not matched to a claim in the same period
            SELECT
                bp.providerid                                   AS nhisproviderid,
                pa."IID"::VARCHAR                               AS enrollee_id,
                pa.panumber::VARCHAR                            AS panumber,
                CAST(pa.requestdate AS DATE)                    AS encounterdatefrom,
                pa.granted                                      AS approvedamount,
                NULL::VARCHAR                                   AS code
            FROM "{schema}"."PA DATA" pa
            INNER JOIN band_providers bp
                ON pa.providerid::VARCHAR = bp.providerid_orig
            WHERE pa.pastatus = 'AUTHORIZED'
              AND pa.panumber IS NOT NULL
              AND CAST(pa.requestdate AS DATE) BETWEEN ? AND ?
              AND NOT EXISTS (
                  SELECT 1 FROM "{schema}"."CLAIMS DATA" cd
                  WHERE TRY_CAST(cd.panumber AS BIGINT) = TRY_CAST(pa.panumber AS BIGINT)
                    AND CAST(cd.encounterdatefrom AS DATE) BETWEEN ? AND ?
              )
        ),
        all_activity AS (
            SELECT nhisproviderid, enrollee_id, panumber, encounterdatefrom, approvedamount, code
            FROM claims
            UNION ALL
            SELECT nhisproviderid, enrollee_id, panumber, encounterdatefrom, approvedamount, code
            FROM unclaimed_pa
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
            FROM all_activity
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
    return conn.execute(sql, [band, start_date, end_date,        # band_providers + claims
                               start_date, end_date,              # unclaimed_pa requestdate
                               start_date, end_date]).fetchdf()   # NOT EXISTS encounterdatefrom


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
    provider_id = _normalize_pid(provider_id)
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


def get_network_cpe_benchmark(
    conn: duckdb.DuckDBPyConnection,
    group_id: str,
    provider_ids: list,
    contract_start: str,
    contract_end: str,
) -> pd.DataFrame:
    """
    Network CPE benchmark: for each provider, compare their CPE for this specific
    group vs their CPE across all other groups they serve in the same period.

    total_cost = claims (encounterdatefrom) + unclaimed PA (requestdate, no matching claim).

    Returns columns:
        provider_id, provider_name, cpe_this_group, cpe_network,
        groups_served, cpe_ratio
    Only providers appearing in the input list are evaluated.
    HAVING clause enforces >= 3 groups served (set in query; callers assign INSUFFICIENT_DATA otherwise).
    """
    schema = DB_SCHEMA
    if not provider_ids:
        return pd.DataFrame()

    # Normalize IDs: CLAIMS DATA nhisproviderid stores '22164', not '000022164'
    # Unclaimed PA filter uses CAST(TRY_CAST(providerid AS BIGINT) AS VARCHAR) which also strips zeros
    provider_ids = [_normalize_pid(pid) for pid in provider_ids]
    ids_str = ", ".join(f"'{pid}'" for pid in provider_ids)

    sql = f"""
        WITH claims AS (
            SELECT
                cd.nhisproviderid                                       AS provider_id,
                cd.nhisgroupid                                          AS group_id,
                cd.enrollee_id::VARCHAR                                 AS enrollee_id,
                CAST(cd.approvedamount AS FLOAT)                        AS amount,
                COALESCE(
                    CAST(TRY_CAST(cd.panumber AS BIGINT) AS VARCHAR),
                    cd.enrollee_id || '|' || CAST(cd.encounterdatefrom AS VARCHAR)
                )                                                       AS visit_key
            FROM "{schema}"."CLAIMS DATA" cd
            WHERE cd.nhisproviderid IN ({ids_str})
              AND CAST(cd.encounterdatefrom AS DATE) BETWEEN '{contract_start}' AND '{contract_end}'
        ),
        unclaimed_pa AS (
            -- Unclaimed PA: authorized, in period, no matching claim in period
            -- Use nhisgroupid from PA DATA groupname → nhisgroupid via CLAIMS DATA lookup
            -- Simpler: tag with group_id from the CLAIMS DATA nhisgroupid if available,
            -- otherwise use the PA request group.  We attach the PA to the group it was
            -- requested for by matching enrollee+group on the claims side.
            SELECT
                CAST(TRY_CAST(pa.providerid AS BIGINT) AS VARCHAR)      AS provider_id,
                pa.groupname                                             AS group_label,
                pa."IID"::VARCHAR                                        AS enrollee_id,
                CAST(pa.granted AS FLOAT)                               AS amount,
                pa.panumber::VARCHAR                                     AS visit_key,
                -- Derive nhisgroupid: look it up from CLAIMS DATA for this enrollee
                COALESCE(
                    (SELECT cd2.nhisgroupid
                     FROM "{schema}"."CLAIMS DATA" cd2
                     WHERE cd2.enrollee_id = pa."IID"::VARCHAR
                     LIMIT 1),
                    NULL
                )                                                        AS group_id
            FROM "{schema}"."PA DATA" pa
            WHERE pa.pastatus = 'AUTHORIZED'
              AND pa.panumber IS NOT NULL
              AND CAST(pa.requestdate AS DATE) BETWEEN '{contract_start}' AND '{contract_end}'
              AND CAST(TRY_CAST(pa.providerid AS BIGINT) AS VARCHAR) IN ({ids_str})
              AND NOT EXISTS (
                  SELECT 1 FROM "{schema}"."CLAIMS DATA" cd
                  WHERE TRY_CAST(cd.panumber AS BIGINT) = TRY_CAST(pa.panumber AS BIGINT)
                    AND CAST(cd.encounterdatefrom AS DATE) BETWEEN '{contract_start}' AND '{contract_end}'
              )
        ),
        all_activity AS (
            SELECT provider_id, group_id, enrollee_id, amount, visit_key FROM claims
            UNION ALL
            SELECT provider_id, group_id, enrollee_id, amount, visit_key FROM unclaimed_pa
        ),
        agg AS (
            SELECT
                provider_id,
                SUM(CASE WHEN group_id = '{group_id}' THEN amount END)  AS cost_this_group,
                SUM(CASE WHEN group_id != '{group_id}' THEN amount END) AS cost_network,
                COUNT(DISTINCT CASE WHEN group_id = '{group_id}' THEN visit_key END)
                                                                        AS enc_this_group,
                COUNT(DISTINCT CASE WHEN group_id != '{group_id}' THEN visit_key END)
                                                                        AS enc_network,
                COUNT(DISTINCT group_id)                                AS groups_served
            FROM all_activity
            GROUP BY provider_id
            HAVING COUNT(DISTINCT group_id) >= 3
        )
        SELECT
            a.provider_id,
            p.providername,
            ROUND(a.cost_this_group / NULLIF(a.enc_this_group, 0), 0)  AS cpe_this_group,
            ROUND(a.cost_network    / NULLIF(a.enc_network,    0), 0)  AS cpe_network,
            a.groups_served,
            a.enc_this_group                                            AS encounters_this_group,
            ROUND(
                (a.cost_this_group / NULLIF(a.enc_this_group, 0)) /
                NULLIF(a.cost_network / NULLIF(a.enc_network, 0), 0),
            2)                                                          AS cpe_ratio
        FROM agg a
        LEFT JOIN "{schema}"."PROVIDERS" p
            ON TRY_CAST(a.provider_id AS BIGINT) = TRY_CAST(p.providerid AS BIGINT)
    """
    return conn.execute(sql).fetchdf()


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
      - Unclaimed PA:   visit date = requestdate (PA authorized but never claimed)
    Compute avg gap between consecutive visits per enrollee.
    Flag if avg gap < window_days.
    Returns: (summary_df, bucket_df)
    """
    schema   = DB_SCHEMA
    orig_pid = provider_id
    provider_id = _normalize_pid(provider_id)
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
        unclaimed_pa AS (
            SELECT
                pa."IID"::VARCHAR                                       AS enrollee_id,
                CAST(pa.requestdate AS DATE)                            AS encounterdatefrom,
                pa.panumber::VARCHAR                                    AS visit_id
            FROM "{schema}"."PA DATA" pa
            WHERE pa.providerid::VARCHAR = ?
              AND pa.pastatus = 'AUTHORIZED'
              AND pa.panumber IS NOT NULL
              AND CAST(pa.requestdate AS DATE) BETWEEN ? AND ?
              AND NOT EXISTS (
                  SELECT 1 FROM "{schema}"."CLAIMS DATA" cd
                  WHERE TRY_CAST(cd.panumber AS BIGINT) = TRY_CAST(pa.panumber AS BIGINT)
                    AND CAST(cd.encounterdatefrom AS DATE) BETWEEN ? AND ?
              )
        ),
        all_visits_raw AS (
            SELECT enrollee_id, visit_id, encounterdatefrom FROM claims
            UNION ALL
            SELECT enrollee_id, visit_id, encounterdatefrom FROM unclaimed_pa
        ),
        -- one row per unique visit per enrollee
        visits AS (
            SELECT DISTINCT enrollee_id, visit_id, encounterdatefrom
            FROM all_visits_raw
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
    params = [provider_id, start_date, end_date,        # claims
              orig_pid, start_date, end_date,            # unclaimed_pa providerid + requestdate
              start_date, end_date]                      # NOT EXISTS encounterdatefrom
    summary = conn.execute(sql, params).fetchdf()

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
        unclaimed_pa AS (
            SELECT
                pa."IID"::VARCHAR                                       AS enrollee_id,
                CAST(pa.requestdate AS DATE)                            AS encounterdatefrom,
                pa.panumber::VARCHAR                                    AS visit_id
            FROM "{schema}"."PA DATA" pa
            WHERE pa.providerid::VARCHAR = ?
              AND pa.pastatus = 'AUTHORIZED'
              AND pa.panumber IS NOT NULL
              AND CAST(pa.requestdate AS DATE) BETWEEN ? AND ?
              AND NOT EXISTS (
                  SELECT 1 FROM "{schema}"."CLAIMS DATA" cd
                  WHERE TRY_CAST(cd.panumber AS BIGINT) = TRY_CAST(pa.panumber AS BIGINT)
                    AND CAST(cd.encounterdatefrom AS DATE) BETWEEN ? AND ?
              )
        ),
        all_visits_raw AS (
            SELECT enrollee_id, visit_id, encounterdatefrom FROM claims
            UNION ALL
            SELECT enrollee_id, visit_id, encounterdatefrom FROM unclaimed_pa
        ),
        visits AS (
            SELECT DISTINCT enrollee_id, visit_id, encounterdatefrom FROM all_visits_raw
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
    buckets = conn.execute(bucket_sql, params).fetchdf()

    return summary, buckets