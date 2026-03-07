"""
service.py
──────────
All DuckDB queries for the MLR summary endpoint.

Methodology (Nigerian HMO / actuarial standards):
  • Actual MLR    = (Claims by encounterdatefrom + Unclaimed PA) / Total Debit
  • Claims-Paid MLR = Claims by datesubmitted / Total Debit
  • Total Medical Cost = Claims Paid + Unclaimed PA  (no double-count)
  • Unclaimed PA  = AUTHORIZED PAs in period with NO matching claim panumber
  • PMPM denominator = enrolled member count × contract months
  • Visit count   = distinct panumber (real PA) OR enrollee_id+date (walk-in)
  • MLR > 75 %   = LOSS  (25 % overhead: 15 % admin + 10 % commission)
"""

from datetime import date
from dateutil.relativedelta import relativedelta
from .database import get_db
from .models import MLRSummaryResponse, ProviderRow, EnrolleeRow, ProcedureRow
from .config import MLR_BREAK_EVEN, MLR_WARNING


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _pct(value: float, total: float) -> float:
    return round((value / total * 100), 2) if total else 0.0

def _mlr_status(mlr: float) -> str:
    if mlr > MLR_BREAK_EVEN:
        return "LOSS"
    if mlr > MLR_WARNING:
        return "WARNING"
    return "PROFITABLE"

def _contract_months(start: date, end: date) -> int:
    """Full contracted period in months. Used for premium PMPM."""
    delta = relativedelta(end, start)
    return max(1, delta.years * 12 + delta.months + 1)


def _elapsed_months(start: date, end: date) -> int:
    """
    Months elapsed from start up to today (or end_date if contract has finished).
    Used for medical cost PMPM run-rate — normalises actual spend by actual
    elapsed time, not the full contracted period.
    """
    today = date.today()
    effective_end = min(today, end)
    if effective_end <= start:
        return 1
    delta = relativedelta(effective_end, start)
    return max(1, delta.years * 12 + delta.months + 1)

def _safe_div(num: float, den: float) -> float:
    return round(num / den, 6) if den else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Individual query functions
# ─────────────────────────────────────────────────────────────────────────────
def _total_debit(con, client: str, start: date, end: date) -> float:
    """
    Sum of all debit note amounts for the client within the contract period.
    Anchors on the debit note's 'From' date — it must fall within the contract
    period (From >= start AND From <= end). This correctly excludes add-on notes
    from a previous contract that merely overlap the current period by a few days.
    Excludes TPA rows (Description ILIKE '%tpa%').
    """
    q = """
        SELECT COALESCE(SUM("Amount"), 0)
        FROM   "AI DRIVEN DATA"."DEBIT_NOTE"
        WHERE  "CompanyName" ILIKE ?
          AND  CAST("From" AS DATE) >= ?
          AND  CAST("From" AS DATE) <= ?
          AND  ("Description" IS NULL OR "Description" NOT ILIKE '%tpa%')
    """
    return float(con.execute(q, [f"%{client}%", start, end]).fetchone()[0])


def _claims_actual(con, client: str, start: date, end: date) -> float:
    """Claims cost anchored on encounterdatefrom (actuarial / Actual MLR)."""
    q = """
        SELECT COALESCE(SUM(c.approvedamount), 0)
        FROM   "AI DRIVEN DATA"."CLAIMS DATA" c
        JOIN   "AI DRIVEN DATA"."GROUPS"       g
               ON TRY_CAST(c.nhisgroupid AS BIGINT) = g.groupid
        WHERE  g.groupname ILIKE ?
          AND  c.encounterdatefrom >= ?
          AND  c.encounterdatefrom <= ?
    """
    return float(con.execute(q, [f"%{client}%", start, end]).fetchone()[0])


def _claims_paid(con, client: str, start: date, end: date) -> float:
    """Claims cost anchored on datesubmitted (financial / Cash MLR)."""
    q = """
        SELECT COALESCE(SUM(c.approvedamount), 0)
        FROM   "AI DRIVEN DATA"."CLAIMS DATA" c
        JOIN   "AI DRIVEN DATA"."GROUPS"       g
               ON TRY_CAST(c.nhisgroupid AS BIGINT) = g.groupid
        WHERE  g.groupname ILIKE ?
          AND  CAST(c.datesubmitted AS DATE) >= ?
          AND  CAST(c.datesubmitted AS DATE) <= ?
    """
    return float(con.execute(q, [f"%{client}%", start, end]).fetchone()[0])


def _unclaimed_pa(con, client: str, start: date, end: date) -> float:
    """
    AUTHORIZED PAs in the contract period that have NO matching claim.

    Matching logic:
      Claims side  → CAST(CAST(panumber AS BIGINT) AS VARCHAR)
      PA side      → panumber (VARCHAR, but may contain '123.0' artefacts)
      We normalise both to integer-string before comparing.

    Excludes PAs with NULL / '0' / '0.0' / '' panumbers (walk-in rows).
    """
    q = """
        WITH claimed_pa AS (
            SELECT DISTINCT
                CAST(CAST(c.panumber AS BIGINT) AS VARCHAR) AS norm_pa
            FROM  "AI DRIVEN DATA"."CLAIMS DATA" c
            JOIN  "AI DRIVEN DATA"."GROUPS"       g
                  ON TRY_CAST(c.nhisgroupid AS BIGINT) = g.groupid
            WHERE g.groupname ILIKE ?
              AND c.encounterdatefrom >= ?
              AND c.encounterdatefrom <= ?
              AND c.panumber IS NOT NULL
              AND c.panumber != 0
        )
        SELECT COALESCE(SUM(pa.granted), 0)
        FROM   "AI DRIVEN DATA"."PA DATA" pa
        WHERE  pa.groupname  ILIKE ?
          AND  CAST(pa.requestdate AS DATE) >= ?
          AND  CAST(pa.requestdate AS DATE) <= ?
          AND  pa.pastatus = 'AUTHORIZED'
          AND  pa.panumber IS NOT NULL
          AND  pa.panumber NOT IN ('0', '0.0', '')
          AND  TRY_CAST(pa.panumber AS BIGINT) IS NOT NULL
          AND  CAST(TRY_CAST(pa.panumber AS BIGINT) AS VARCHAR)
                   NOT IN (SELECT norm_pa FROM claimed_pa)
    """
    params = [f"%{client}%", start, end, f"%{client}%", start, end]
    return float(con.execute(q, params).fetchone()[0])


def _enrolled_members(con, client: str, start: date, end: date) -> int:
    """
    Count unique members enrolled during the contract period.
    Uses MEMBERS table joined on enrollee_id (confirmed correct key).
    A member is 'enrolled' if:
      - registrationdate <= end_date  (joined before or during period)
      - terminationdate IS NULL OR terminationdate >= start_date  (still active at some point)
    """
    q = """
        SELECT COUNT(DISTINCT ms.enrollee_id)
        FROM   "AI DRIVEN DATA"."MEMBERS" ms
        JOIN   "AI DRIVEN DATA"."GROUPS"  g
               ON CAST(ms.groupid AS INTEGER) = CAST(g.groupid AS INTEGER)
        WHERE  g.groupname ILIKE ?
          AND  CAST(ms.registrationdate AS DATE) <= ?
          AND  (ms.terminationdate IS NULL
                OR CAST(ms.terminationdate AS DATE) >= ?)
    """
    return int(con.execute(q, [f"%{client}%", end, start]).fetchone()[0])


def _utilized_members(con, client: str, start: date, end: date) -> int:
    """
    Count unique members who have at least one claim within the contract period.
    Anchored on encounterdatefrom (consistent with actual MLR methodology).
    """
    q = """
        SELECT COUNT(DISTINCT c.enrollee_id)
        FROM   "AI DRIVEN DATA"."CLAIMS DATA" c
        JOIN   "AI DRIVEN DATA"."GROUPS"       g
               ON TRY_CAST(c.nhisgroupid AS BIGINT) = g.groupid
        WHERE  g.groupname ILIKE ?
          AND  c.encounterdatefrom >= ?
          AND  c.encounterdatefrom <= ?
          AND  c.enrollee_id IS NOT NULL
    """
    return int(con.execute(q, [f"%{client}%", start, end]).fetchone()[0])


# ─────────────────────────────────────────────────────────────────────────────
# Top-10 queries
# ─────────────────────────────────────────────────────────────────────────────
def _top_providers(con, client: str, start: date, end: date) -> list:
    """
    Top 10 providers by TOTAL MEDICAL COST (claims + unclaimed PA) AND by visit count.
    Unclaimed PA is attributed per provider using the same logic as the summary total.
    Visit = distinct panumber (real PA) OR enrollee_id + date (walk-in).
    """
    q = """
        WITH claimed_pa AS (
            SELECT DISTINCT
                CAST(CAST(c.panumber AS BIGINT) AS VARCHAR) AS norm_pa
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            JOIN "AI DRIVEN DATA"."GROUPS" g
                 ON TRY_CAST(c.nhisgroupid AS BIGINT) = g.groupid
            WHERE g.groupname ILIKE ?
              AND c.encounterdatefrom >= ?
              AND c.encounterdatefrom <= ?
              AND c.panumber IS NOT NULL
              AND c.panumber != 0
        ),

        claims_by_provider AS (
            SELECT
                COALESCE(p.providerid, CAST(c.nhisproviderid AS VARCHAR)) AS provider_id,
                COALESCE(p.providername, 'Unknown')                       AS provider_name,
                COUNT(DISTINCT
                    CASE
                        WHEN c.panumber IS NOT NULL AND c.panumber != 0
                        THEN CAST(CAST(c.panumber AS BIGINT) AS VARCHAR)
                        ELSE c.enrollee_id || '_' || CAST(c.encounterdatefrom AS VARCHAR)
                    END
                )                                                          AS visit_count,
                COUNT(*)                                                   AS claim_rows,
                COALESCE(SUM(c.approvedamount), 0)                        AS claims_cost
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            JOIN "AI DRIVEN DATA"."GROUPS" g
                 ON TRY_CAST(c.nhisgroupid AS BIGINT) = g.groupid
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
                 ON TRY_CAST(c.nhisproviderid AS BIGINT) = TRY_CAST(p.providerid AS BIGINT)
            WHERE g.groupname ILIKE ?
              AND c.encounterdatefrom >= ?
              AND c.encounterdatefrom <= ?
            GROUP BY provider_id, provider_name
        ),

        unclaimed_pa_by_provider AS (
            SELECT
                pa.providerid                AS provider_id,
                COALESCE(SUM(pa.granted), 0) AS pa_cost
            FROM "AI DRIVEN DATA"."PA DATA" pa
            WHERE pa.groupname ILIKE ?
              AND CAST(pa.requestdate AS DATE) >= ?
              AND CAST(pa.requestdate AS DATE) <= ?
              AND pa.pastatus = 'AUTHORIZED'
              AND pa.panumber IS NOT NULL
              AND pa.panumber NOT IN ('0', '0.0', '')
              AND TRY_CAST(pa.panumber AS BIGINT) IS NOT NULL
              AND CAST(TRY_CAST(pa.panumber AS BIGINT) AS VARCHAR)
                      NOT IN (SELECT norm_pa FROM claimed_pa)
            GROUP BY pa.providerid
        ),

        combined AS (
            SELECT
                COALESCE(c.provider_id,   u.provider_id)  AS provider_id,
                COALESCE(c.provider_name, 'Unknown')       AS provider_name,
                COALESCE(c.visit_count,   0)               AS visit_count,
                COALESCE(c.claim_rows,    0)               AS claim_rows,
                COALESCE(c.claims_cost,   0)               AS claims_cost,
                COALESCE(u.pa_cost,       0)               AS pa_cost,
                COALESCE(c.claims_cost, 0) + COALESCE(u.pa_cost, 0) AS total_cost
            FROM claims_by_provider c
            FULL OUTER JOIN unclaimed_pa_by_provider u
                 ON c.provider_id = u.provider_id
            WHERE COALESCE(c.claims_cost, 0) + COALESCE(u.pa_cost, 0) > 0
        )

        SELECT provider_id, provider_name, visit_count, claim_rows, total_cost
        FROM combined
    """
    params = [
        f"%{client}%", start, end,   # claimed_pa CTE
        f"%{client}%", start, end,   # claims_by_provider CTE
        f"%{client}%", start, end,   # unclaimed_pa_by_provider CTE
    ]
    rows = con.execute(q, params).fetchall()
    total_cost_all = sum(r[4] for r in rows) or 1

    by_cost  = sorted(rows, key=lambda r: r[4], reverse=True)[:10]
    by_count = sorted(rows, key=lambda r: r[2], reverse=True)[:10]

    def to_row(i, r):
        return ProviderRow(
            rank=i+1, provider_id=r[0], provider_name=r[1],
            visit_count=r[2], claim_rows=r[3], total_cost=round(r[4], 2),
            pct_of_total=_pct(r[4], total_cost_all)
        )

    return (
        [to_row(i, r) for i, r in enumerate(by_cost)],
        [to_row(i, r) for i, r in enumerate(by_count)]
    )


def _top_enrollees(con, client: str, start: date, end: date) -> list:
    """
    Top 10 enrollees by cost AND by visit count.
    Name lookup via MEMBER table (has firstname/lastname) on legacycode = enrollee_id.
    """
    q = """
        SELECT
            c.enrollee_id,
            COALESCE(
                TRIM(m.firstname) || ' ' || TRIM(m.lastname),
                c.enrollee_id
            )                                             AS enrollee_name,
            COUNT(DISTINCT
                CASE
                    WHEN c.panumber IS NOT NULL AND c.panumber != 0
                    THEN CAST(CAST(c.panumber AS BIGINT) AS VARCHAR)
                    ELSE c.enrollee_id || '_' || CAST(c.encounterdatefrom AS VARCHAR)
                END
            )                                             AS visit_count,
            COUNT(*)                                      AS claim_rows,
            COALESCE(SUM(c.approvedamount), 0)            AS total_cost
        FROM  "AI DRIVEN DATA"."CLAIMS DATA" c
        JOIN  "AI DRIVEN DATA"."GROUPS"       g
              ON TRY_CAST(c.nhisgroupid AS BIGINT) = g.groupid
        LEFT JOIN "AI DRIVEN DATA"."MEMBER"   m
              ON c.enrollee_id = m.legacycode
        WHERE g.groupname ILIKE ?
          AND c.encounterdatefrom >= ?
          AND c.encounterdatefrom <= ?
        GROUP BY c.enrollee_id, enrollee_name
    """
    rows = con.execute(q, [f"%{client}%", start, end]).fetchall()
    total_cost = sum(r[4] for r in rows) or 1

    by_cost  = sorted(rows, key=lambda r: r[4], reverse=True)[:10]
    by_count = sorted(rows, key=lambda r: r[2], reverse=True)[:10]

    def to_row(i, r):
        return EnrolleeRow(
            rank=i+1, enrollee_id=r[0], enrollee_name=r[1],
            visit_count=r[2], claim_rows=r[3], total_cost=round(r[4], 2),
            pct_of_total=_pct(r[4], total_cost)
        )

    return (
        [to_row(i, r) for i, r in enumerate(by_cost)],
        [to_row(i, r) for i, r in enumerate(by_count)]
    )


def _top_procedures(con, client: str, start: date, end: date) -> list:
    """Top 10 procedures by cost AND by count. Joins PROCEDURE DATA for desc."""
    q = """
        SELECT
            c.code                                        AS procedure_code,
            COALESCE(pd.proceduredesc, c.code)            AS procedure_desc,
            COUNT(*)                                      AS claim_count,
            COALESCE(SUM(c.approvedamount), 0)            AS total_cost
        FROM  "AI DRIVEN DATA"."CLAIMS DATA"   c
        JOIN  "AI DRIVEN DATA"."GROUPS"         g
              ON TRY_CAST(c.nhisgroupid AS BIGINT) = g.groupid
        LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd
              ON UPPER(c.code) = UPPER(pd.procedurecode)
        WHERE g.groupname ILIKE ?
          AND c.encounterdatefrom >= ?
          AND c.encounterdatefrom <= ?
          AND c.code IS NOT NULL
        GROUP BY c.code, procedure_desc
    """
    rows = con.execute(q, [f"%{client}%", start, end]).fetchall()
    total_cost = sum(r[3] for r in rows) or 1

    by_cost  = sorted(rows, key=lambda r: r[3], reverse=True)[:10]
    by_count = sorted(rows, key=lambda r: r[2], reverse=True)[:10]

    def to_row(i, r):
        return ProcedureRow(
            rank=i+1, procedure_code=r[0], procedure_desc=r[1],
            claim_count=r[2], total_cost=round(r[3], 2),
            pct_of_total=_pct(r[3], total_cost)
        )

    return (
        [to_row(i, r) for i, r in enumerate(by_cost)],
        [to_row(i, r) for i, r in enumerate(by_count)]
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main service entry point
# ─────────────────────────────────────────────────────────────────────────────
def compute_mlr_summary(
    client_name: str,
    start_date:  date,
    end_date:    date
) -> MLRSummaryResponse:

    with get_db() as con:
        # ── Financials ──────────────────────────────────────────────────────
        total_debit       = _total_debit(con, client_name, start_date, end_date)
        claims_actual_amt = _claims_actual(con, client_name, start_date, end_date)
        claims_paid_amt   = _claims_paid(con, client_name, start_date, end_date)
        unclaimed_pa_amt  = _unclaimed_pa(con, client_name, start_date, end_date)

        total_actual_cost = claims_actual_amt + unclaimed_pa_amt

        # ── MLR ─────────────────────────────────────────────────────────────
        actual_mlr      = _safe_div(total_actual_cost, total_debit)
        claims_paid_mlr = _safe_div(claims_paid_amt,   total_debit)

        # ── PMPM ────────────────────────────────────────────────────────────
        enrolled      = _enrolled_members(con, client_name, start_date, end_date)
        contract_mths = _contract_months(start_date, end_date)
        elapsed_mths  = _elapsed_months(start_date, end_date)
        medical_mm    = max(enrolled * elapsed_mths, 1)
        premium_mm    = max(enrolled * contract_mths, 1)

        actual_pmpm  = _safe_div(total_actual_cost, medical_mm)
        paid_pmpm    = _safe_div(claims_paid_amt,   medical_mm)
        premium_pmpm = _safe_div(total_debit,       premium_mm)

        # ── Utilization ─────────────────────────────────────────────────────
        utilized        = _utilized_members(con, client_name, start_date, end_date)
        utilization_pct = round(_pct(utilized, enrolled), 2)

        # ── Top-10 ──────────────────────────────────────────────────────────
        prov_by_cost, prov_by_count = _top_providers(con, client_name, start_date, end_date)
        enrl_by_cost, enrl_by_count = _top_enrollees(con, client_name, start_date, end_date)
        proc_by_cost, proc_by_count = _top_procedures(con, client_name, start_date, end_date)

    return MLRSummaryResponse(
        client_name  = client_name,
        start_date   = start_date,
        end_date     = end_date,

        total_debit_amount         = round(total_debit, 2),

        actual_claims_cost         = round(claims_actual_amt, 2),
        unclaimed_pa_cost          = round(unclaimed_pa_amt, 2),
        total_actual_medical_cost  = round(total_actual_cost, 2),
        claims_paid_cost           = round(claims_paid_amt, 2),

        actual_mlr          = round(actual_mlr, 4),
        actual_mlr_pct      = f"{actual_mlr * 100:.2f} %",
        claims_paid_mlr     = round(claims_paid_mlr, 4),
        claims_paid_mlr_pct = f"{claims_paid_mlr * 100:.2f} %",
        mlr_status          = _mlr_status(actual_mlr),

        enrolled_members              = enrolled,
        utilized_members              = utilized,
        member_utilization_pct        = utilization_pct,
        contract_months               = contract_mths,
        elapsed_months                = elapsed_mths,
        member_months                 = medical_mm,
        actual_medical_cost_pmpm      = round(actual_pmpm, 2),
        claims_paid_medical_cost_pmpm = round(paid_pmpm, 2),
        premium_pmpm                  = round(premium_pmpm, 2),

        top_10_providers_by_cost   = prov_by_cost,
        top_10_providers_by_count  = prov_by_count,
        top_10_enrollees_by_cost   = enrl_by_cost,
        top_10_enrollees_by_count  = enrl_by_count,
        top_10_procedures_by_cost  = proc_by_cost,
        top_10_procedures_by_count = proc_by_count,
    )
