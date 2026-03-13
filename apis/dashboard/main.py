"""
Clearline Executive Dashboard API
===================================
Provides all data needed for the company-level performance dashboard.

Routes
------
GET /health                                 — DB connectivity + column introspection
GET /dashboard/summary-cards                — 8 KPI cards
GET /dashboard/monthly-trends               — 6 month-by-month series (current vs last year)
GET /dashboard/contract-summary             — Previous debit / current debit / cash per active group
GET /dashboard/top-clients/pa               — Top 20 clients by PA cost (YTD)
GET /dashboard/top-providers/pa             — Top 20 providers by PA cost (YTD)
GET /dashboard/top-clients/claims-encounter — Top 20 clients by claims (encounterdatefrom, YTD)
GET /dashboard/top-clients/claims-submitted — Top 20 clients by claims (datesubmitted, YTD)
GET /dashboard/cash-breakdown               — Monthly salary/palliative + expense/commission

Run (from project root):
    uvicorn apis.dashboard.main:app --reload --port 8003

Key decisions
-------------
- "YTD 2026"  = Jan 1 2026 to today
- "SPLY"      = Jan 1 2025 to same calendar date in 2025
- MLR         = total claims / total cash collected × 100  (company-wide aggregate)
- Debit       = DEBIT_NOTE, "From" within contract period, TPA rows excluded
- Cash        = CLIENT_CASH_RECEIVED
- Salary/Pal  = SALARY_AND_PALLIATIVE  (master table; year-partitioned siblings exist)
- Expense/Com = EXPENSE_AND_COMMISSION (master table; year-partitioned siblings exist)
"""

import os
from typing import List, Optional, Dict, Any
from datetime import date
from collections import defaultdict

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv(
    "DUCKDB_PATH",
    "/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb"
)
SCHEMA = "AI DRIVEN DATA"

TODAY      = date.today()
YEAR_START = date(TODAY.year, 1, 1)
SPLY_START = date(TODAY.year - 1, 1, 1)
SPLY_END   = date(TODAY.year - 1, TODAY.month, TODAY.day)

MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Clearline Executive Dashboard API",
    description="Company-level KPIs, trends, contract table, top-20s, and cash breakdown.",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# Populated at startup — serves column introspection at /health
_introspection: Dict[str, Any] = {}


def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)


def _introspect(conn, table_name: str) -> dict:
    try:
        cols = conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{SCHEMA}' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()
        if cols:
            return {"status": "found", "columns": [{"name": c[0], "type": c[1]} for c in cols]}
        return {"status": "not_found"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.on_event("startup")
def startup_introspect():
    global _introspection
    conn = get_conn()
    try:
        for t in ["SALARY_AND_PALLIATIVE", "EXPENSE_AND_COMMISSION",
                  "CLIENT_CASH_RECEIVED", "DEBIT_NOTE", "PA DATA", "CLAIMS DATA"]:
            _introspection[t] = _introspect(conn, t)
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/health", tags=["Health"])
def health():
    try:
        conn = get_conn()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "db_error",
        "service": "clearline-executive-dashboard",
        "ytd_window":  {"from": str(YEAR_START), "to": str(TODAY)},
        "sply_window": {"from": str(SPLY_START), "to": str(SPLY_END)},
        "table_columns": _introspection,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — SUMMARY CARDS
# ══════════════════════════════════════════════════════════════════════════════

class SummaryCards(BaseModel):
    cash_collected_ytd: float
    claims_paid_encounter_ytd: float
    claims_paid_submitted_ytd: float
    cash_collected_sply: float
    claims_paid_encounter_sply: float
    claims_paid_submitted_sply: float
    mlr_encounter_pct: float
    mlr_submitted_pct: float
    ytd_from: str
    ytd_to: str
    sply_from: str
    sply_to: str


@app.get(
    "/dashboard/summary-cards",
    response_model=SummaryCards,
    summary="8 KPI summary cards — YTD vs same period last year",
    tags=["1 — Summary Cards"],
)
def get_summary_cards():
    conn = get_conn()
    try:
        def q(sql): return conn.execute(sql).fetchone()[0] or 0.0

        cash_ytd  = q(f"""SELECT COALESCE(SUM(Amount),0) FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
                         WHERE Date >= DATE '{YEAR_START}' AND Date <= DATE '{TODAY}'""")
        enc_ytd   = q(f"""SELECT COALESCE(SUM(CAST(approvedamount AS DOUBLE)),0) FROM "{SCHEMA}"."CLAIMS DATA"
                         WHERE CAST(encounterdatefrom AS DATE) >= DATE '{YEAR_START}'
                           AND CAST(encounterdatefrom AS DATE) <= DATE '{TODAY}'""")
        sub_ytd   = q(f"""SELECT COALESCE(SUM(CAST(approvedamount AS DOUBLE)),0) FROM "{SCHEMA}"."CLAIMS DATA"
                         WHERE CAST(datesubmitted AS DATE) >= DATE '{YEAR_START}'
                           AND CAST(datesubmitted AS DATE) <= DATE '{TODAY}'""")
        cash_sply = q(f"""SELECT COALESCE(SUM(Amount),0) FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
                         WHERE Date >= DATE '{SPLY_START}' AND Date <= DATE '{SPLY_END}'""")
        enc_sply  = q(f"""SELECT COALESCE(SUM(CAST(approvedamount AS DOUBLE)),0) FROM "{SCHEMA}"."CLAIMS DATA"
                         WHERE CAST(encounterdatefrom AS DATE) >= DATE '{SPLY_START}'
                           AND CAST(encounterdatefrom AS DATE) <= DATE '{SPLY_END}'""")
        sub_sply  = q(f"""SELECT COALESCE(SUM(CAST(approvedamount AS DOUBLE)),0) FROM "{SCHEMA}"."CLAIMS DATA"
                         WHERE CAST(datesubmitted AS DATE) >= DATE '{SPLY_START}'
                           AND CAST(datesubmitted AS DATE) <= DATE '{SPLY_END}'""")
    finally:
        conn.close()

    return SummaryCards(
        cash_collected_ytd=round(cash_ytd, 2),
        claims_paid_encounter_ytd=round(enc_ytd, 2),
        claims_paid_submitted_ytd=round(sub_ytd, 2),
        cash_collected_sply=round(cash_sply, 2),
        claims_paid_encounter_sply=round(enc_sply, 2),
        claims_paid_submitted_sply=round(sub_sply, 2),
        mlr_encounter_pct=round(enc_ytd / cash_ytd * 100, 2) if cash_ytd else 0.0,
        mlr_submitted_pct=round(sub_ytd / cash_ytd * 100, 2) if cash_ytd else 0.0,
        ytd_from=str(YEAR_START), ytd_to=str(TODAY),
        sply_from=str(SPLY_START), sply_to=str(SPLY_END),
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — MONTHLY TRENDS
# ══════════════════════════════════════════════════════════════════════════════

class MonthDataPoint(BaseModel):
    month: int
    month_label: str
    current_year: float
    last_year: float


class CumulativeMLRPoint(BaseModel):
    month: int
    month_label: str
    cumulative_mlr_pct: float


class MonthlyTrends(BaseModel):
    cash_collection:          List[MonthDataPoint]
    claims_by_encounter:      List[MonthDataPoint]
    claims_by_submitted:      List[MonthDataPoint]
    pa_cost:                  List[MonthDataPoint]
    cumulative_mlr_encounter: List[CumulativeMLRPoint]
    cumulative_mlr_submitted: List[CumulativeMLRPoint]
    current_year: int
    last_year: int


def _monthly(conn, table, amt_col, date_col, year, cast_date=False) -> dict:
    d = f"CAST({date_col} AS DATE)" if cast_date else date_col
    rows = conn.execute(f"""
        SELECT MONTH({d}), COALESCE(SUM(CAST({amt_col} AS DOUBLE)),0)
        FROM "{SCHEMA}"."{table}"
        WHERE YEAR({d}) = {year}
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    return {r[0]: r[1] for r in rows}


def _series(cy, ly) -> List[MonthDataPoint]:
    return [MonthDataPoint(month=m, month_label=MONTH_LABELS[m-1],
                           current_year=round(cy.get(m,0.),2),
                           last_year=round(ly.get(m,0.),2))
            for m in range(1,13)]


def _cum_mlr(claims, cash) -> List[CumulativeMLRPoint]:
    cc = ck = 0.
    out = []
    for m in range(1,13):
        cc += claims.get(m, 0.)
        ck += cash.get(m, 0.)
        out.append(CumulativeMLRPoint(month=m, month_label=MONTH_LABELS[m-1],
                                      cumulative_mlr_pct=round(cc/ck*100,2) if ck else 0.))
    return out


@app.get(
    "/dashboard/monthly-trends",
    response_model=MonthlyTrends,
    summary="Month-by-month series current year vs last year — line/bar charts",
    tags=["2 — Monthly Trends"],
)
def get_monthly_trends():
    cy, ly = TODAY.year, TODAY.year - 1
    conn = get_conn()
    try:
        cash_cy = _monthly(conn, "CLIENT_CASH_RECEIVED", "Amount", "Date", cy)
        cash_ly = _monthly(conn, "CLIENT_CASH_RECEIVED", "Amount", "Date", ly)
        enc_cy  = _monthly(conn, "CLAIMS DATA", "approvedamount", "encounterdatefrom", cy, True)
        enc_ly  = _monthly(conn, "CLAIMS DATA", "approvedamount", "encounterdatefrom", ly, True)
        sub_cy  = _monthly(conn, "CLAIMS DATA", "approvedamount", "datesubmitted", cy, True)
        sub_ly  = _monthly(conn, "CLAIMS DATA", "approvedamount", "datesubmitted", ly, True)
        pa_cy   = _monthly(conn, "PA DATA", "granted", "requestdate", cy, True)
        pa_ly   = _monthly(conn, "PA DATA", "granted", "requestdate", ly, True)
    finally:
        conn.close()

    return MonthlyTrends(
        cash_collection=_series(cash_cy, cash_ly),
        claims_by_encounter=_series(enc_cy, enc_ly),
        claims_by_submitted=_series(sub_cy, sub_ly),
        pa_cost=_series(pa_cy, pa_ly),
        cumulative_mlr_encounter=_cum_mlr(enc_cy, cash_cy),
        cumulative_mlr_submitted=_cum_mlr(sub_cy, cash_cy),
        current_year=cy, last_year=ly,
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — CONTRACT & CASH TABLE
# ══════════════════════════════════════════════════════════════════════════════

class ContractRow(BaseModel):
    group_name: str
    previous_contract_start: Optional[str]
    previous_contract_end: Optional[str]
    previous_debit: Optional[float]
    current_contract_start: str
    current_contract_end: str
    current_debit: Optional[float]
    cash_collected_current: float
    cash_vs_debit_pct: Optional[float]


@app.get(
    "/dashboard/contract-summary",
    response_model=List[ContractRow],
    summary="Per active group: previous debit, current debit, cash collected",
    tags=["3 — Contract Summary"],
)
def get_contract_summary():
    conn = get_conn()
    try:
        active = conn.execute(f"""
            SELECT groupid, groupname,
                   CAST(startdate AS DATE), CAST(enddate AS DATE)
            FROM "{SCHEMA}"."GROUP_CONTRACT"
            WHERE iscurrent = TRUE
            ORDER BY groupname
        """).fetchall()

        results = []
        for (gid, gname, cur_start, cur_end) in active:
            s = gname.replace("'", "''")

            # Previous contract period
            prev = conn.execute(f"""
                SELECT CAST(startdate AS DATE), CAST(enddate AS DATE)
                FROM "{SCHEMA}"."GROUP_CONTRACT"
                WHERE groupid = {gid} AND iscurrent = FALSE
                  AND CAST(enddate AS DATE) < DATE '{cur_start}'
                ORDER BY enddate DESC LIMIT 1
            """).fetchone()

            prev_start = prev[0] if prev else None
            prev_end   = prev[1] if prev else None

            # Current debit (exclude TPA)
            cur_debit = conn.execute(f"""
                SELECT COALESCE(SUM(CAST("Amount" AS DOUBLE)), 0)
                FROM "{SCHEMA}"."DEBIT_NOTE"
                WHERE UPPER("CompanyName") = UPPER('{s}')
                  AND CAST("From" AS DATE) >= DATE '{cur_start}'
                  AND CAST("From" AS DATE) <= DATE '{cur_end}'
                  AND LOWER(COALESCE("Description",'')) NOT LIKE '%tpa%'
            """).fetchone()[0]

            # Previous debit
            prev_debit = None
            if prev_start and prev_end:
                prev_debit = conn.execute(f"""
                    SELECT COALESCE(SUM(CAST("Amount" AS DOUBLE)), 0)
                    FROM "{SCHEMA}"."DEBIT_NOTE"
                    WHERE UPPER("CompanyName") = UPPER('{s}')
                      AND CAST("From" AS DATE) >= DATE '{prev_start}'
                      AND CAST("From" AS DATE) <= DATE '{prev_end}'
                      AND LOWER(COALESCE("Description",'')) NOT LIKE '%tpa%'
                """).fetchone()[0]

            # Cash within current contract period
            cash = conn.execute(f"""
                SELECT COALESCE(SUM(Amount), 0)
                FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
                WHERE UPPER(groupname) = UPPER('{s}')
                  AND Date >= DATE '{cur_start}'
                  AND Date <= DATE '{cur_end}'
            """).fetchone()[0]

            results.append(ContractRow(
                group_name=gname,
                previous_contract_start=str(prev_start) if prev_start else None,
                previous_contract_end=str(prev_end) if prev_end else None,
                previous_debit=round(float(prev_debit), 2) if prev_debit else None,
                current_contract_start=str(cur_start),
                current_contract_end=str(cur_end),
                current_debit=round(float(cur_debit), 2) if cur_debit else None,
                cash_collected_current=round(float(cash), 2),
                cash_vs_debit_pct=round(cash/cur_debit*100, 2) if cur_debit else None,
            ))
    finally:
        conn.close()
    return results


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — TOP 20 TABLES
# ══════════════════════════════════════════════════════════════════════════════

class TopClientRow(BaseModel):
    rank: int
    group_name: str
    amount_ytd: float


class TopProviderRow(BaseModel):
    rank: int
    provider_id: str
    provider_name: str
    provider_band: Optional[str]
    amount_ytd: float


@app.get("/dashboard/top-clients/pa", response_model=List[TopClientRow],
         summary="Top 20 clients by PA cost YTD", tags=["4 — Top 20 Tables"])
def top_clients_pa():
    conn = get_conn()
    try:
        rows = conn.execute(f"""
            SELECT ROW_NUMBER() OVER (ORDER BY SUM(CAST(granted AS DOUBLE)) DESC),
                   groupname,
                   ROUND(SUM(CAST(granted AS DOUBLE)), 2)
            FROM "{SCHEMA}"."PA DATA"
            WHERE CAST(requestdate AS DATE) >= DATE '{YEAR_START}'
              AND CAST(requestdate AS DATE) <= DATE '{TODAY}'
              AND groupname IS NOT NULL AND TRIM(groupname) != ''
            GROUP BY groupname ORDER BY 3 DESC LIMIT 20
        """).fetchall()
    finally:
        conn.close()
    return [TopClientRow(rank=r[0], group_name=r[1], amount_ytd=r[2]) for r in rows]


@app.get("/dashboard/top-providers/pa", response_model=List[TopProviderRow],
         summary="Top 20 providers by PA cost YTD", tags=["4 — Top 20 Tables"])
def top_providers_pa():
    conn = get_conn()
    try:
        rows = conn.execute(f"""
            SELECT
                ROW_NUMBER() OVER (ORDER BY SUM(CAST(pa.granted AS DOUBLE)) DESC),
                CAST(pa.providerid AS VARCHAR),
                COALESCE(p.providername, '(Not in master list)'),
                p.bands,
                ROUND(SUM(CAST(pa.granted AS DOUBLE)), 2)
            FROM "{SCHEMA}"."PA DATA" pa
            LEFT JOIN "{SCHEMA}"."PROVIDERS" p
                ON TRY_CAST(pa.providerid AS BIGINT) = TRY_CAST(p.providerid AS BIGINT)
            WHERE CAST(pa.requestdate AS DATE) >= DATE '{YEAR_START}'
              AND CAST(pa.requestdate AS DATE) <= DATE '{TODAY}'
              AND pa.providerid IS NOT NULL
            GROUP BY pa.providerid, p.providername, p.bands
            ORDER BY 5 DESC LIMIT 20
        """).fetchall()
    finally:
        conn.close()
    return [TopProviderRow(rank=r[0], provider_id=str(r[1]),
                           provider_name=r[2], provider_band=r[3], amount_ytd=r[4])
            for r in rows]


@app.get("/dashboard/top-clients/claims-encounter", response_model=List[TopClientRow],
         summary="Top 20 clients by claims (encounterdatefrom) YTD", tags=["4 — Top 20 Tables"])
def top_clients_claims_encounter():
    conn = get_conn()
    try:
        rows = conn.execute(f"""
            SELECT
                ROW_NUMBER() OVER (ORDER BY SUM(CAST(cd.approvedamount AS DOUBLE)) DESC),
                COALESCE(g.groupname, cd.nhisgroupid),
                ROUND(SUM(CAST(cd.approvedamount AS DOUBLE)), 2)
            FROM "{SCHEMA}"."CLAIMS DATA" cd
            LEFT JOIN "{SCHEMA}"."GROUPS" g
                ON TRY_CAST(cd.nhisgroupid AS BIGINT) = g.groupid
            WHERE CAST(cd.encounterdatefrom AS DATE) >= DATE '{YEAR_START}'
              AND CAST(cd.encounterdatefrom AS DATE) <= DATE '{TODAY}'
              AND cd.nhisgroupid IS NOT NULL AND TRIM(cd.nhisgroupid) != ''
            GROUP BY 2 ORDER BY 3 DESC LIMIT 20
        """).fetchall()
    finally:
        conn.close()
    return [TopClientRow(rank=r[0], group_name=str(r[1]), amount_ytd=r[2]) for r in rows]


@app.get("/dashboard/top-clients/claims-submitted", response_model=List[TopClientRow],
         summary="Top 20 clients by claims (datesubmitted) YTD", tags=["4 — Top 20 Tables"])
def top_clients_claims_submitted():
    conn = get_conn()
    try:
        rows = conn.execute(f"""
            SELECT
                ROW_NUMBER() OVER (ORDER BY SUM(CAST(cd.approvedamount AS DOUBLE)) DESC),
                COALESCE(g.groupname, cd.nhisgroupid),
                ROUND(SUM(CAST(cd.approvedamount AS DOUBLE)), 2)
            FROM "{SCHEMA}"."CLAIMS DATA" cd
            LEFT JOIN "{SCHEMA}"."GROUPS" g
                ON TRY_CAST(cd.nhisgroupid AS BIGINT) = g.groupid
            WHERE CAST(cd.datesubmitted AS DATE) >= DATE '{YEAR_START}'
              AND CAST(cd.datesubmitted AS DATE) <= DATE '{TODAY}'
              AND cd.nhisgroupid IS NOT NULL AND TRIM(cd.nhisgroupid) != ''
            GROUP BY 2 ORDER BY 3 DESC LIMIT 20
        """).fetchall()
    finally:
        conn.close()
    return [TopClientRow(rank=r[0], group_name=str(r[1]), amount_ytd=r[2]) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — CASH OVERHEAD BREAKDOWN
# ══════════════════════════════════════════════════════════════════════════════

class CashBreakdownPoint(BaseModel):
    year: int
    month: int
    month_label: str
    total_cash: float
    salary_palliative: float
    expense: float
    commission: float
    salary_palliative_pct: float
    expense_pct: float
    commission_pct: float
    remaining: float
    remaining_pct: float


def _find_col(col_list: list, candidates: list) -> Optional[str]:
    """Case-insensitive column name detection."""
    lower_map = {c["name"].lower(): c["name"] for c in col_list}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


@app.get(
    "/dashboard/cash-breakdown",
    response_model=List[CashBreakdownPoint],
    summary="Monthly salary/palliative + expense/commission as % of cash — stacked bar",
    tags=["5 — Cash Breakdown"],
)
def get_cash_breakdown():
    """
    Sources:
      SALARY_AND_PALLIATIVE  — all rows treated as salary+palliative costs
      EXPENSE_AND_COMMISSION — rows split by category column:
                               rows containing 'commiss' → commission
                               all other rows            → expense

    If column names differ from expected, call /health to see actual column names,
    then update via the SALARY_DATE_COL / SALARY_AMT_COL etc. env vars (or just rename columns).

    Returns both current year and last year months for YoY comparison on the stacked bar.
    """
    sal_info = _introspection.get("SALARY_AND_PALLIATIVE", {})
    exp_info = _introspection.get("EXPENSE_AND_COMMISSION", {})

    for name, info in [("SALARY_AND_PALLIATIVE", sal_info),
                        ("EXPENSE_AND_COMMISSION", exp_info)]:
        if info.get("status") != "found":
            raise HTTPException(503, detail=(
                f"Table '{name}' not found or not introspected. "
                f"Call /health to see introspection results."
            ))

    DATE_CANDS   = ["date", "month", "period", "pay_date", "payment_date",
                    "transaction_date", "year_month", "created_date", "postingdate"]
    AMOUNT_CANDS = ["amount", "value", "total", "naira_amount", "sum", "payment", "debit"]
    CAT_CANDS    = ["category", "type", "expense_type", "description", "item",
                    "account", "gl_account", "name", "narration"]

    sal_cols = sal_info["columns"]
    exp_cols = exp_info["columns"]

    sal_date = os.getenv("SAL_DATE_COL")   or _find_col(sal_cols, DATE_CANDS)
    sal_amt  = os.getenv("SAL_AMT_COL")    or _find_col(sal_cols, AMOUNT_CANDS)
    exp_date = os.getenv("EXP_DATE_COL")   or _find_col(exp_cols, DATE_CANDS)
    exp_amt  = os.getenv("EXP_AMT_COL")    or _find_col(exp_cols, AMOUNT_CANDS)
    exp_cat  = os.getenv("EXP_CAT_COL")    or _find_col(exp_cols, CAT_CANDS)

    if not sal_date or not sal_amt:
        raise HTTPException(503, detail=(
            f"Cannot detect date/amount in SALARY_AND_PALLIATIVE. "
            f"Columns: {[c['name'] for c in sal_cols]}. "
            f"Set SAL_DATE_COL and SAL_AMT_COL env vars."
        ))
    if not exp_date or not exp_amt:
        raise HTTPException(503, detail=(
            f"Cannot detect date/amount in EXPENSE_AND_COMMISSION. "
            f"Columns: {[c['name'] for c in exp_cols]}. "
            f"Set EXP_DATE_COL and EXP_AMT_COL env vars."
        ))

    years = f"IN ({TODAY.year}, {TODAY.year - 1})"
    conn = get_conn()
    try:
        # Salary & palliative — sum all rows by month
        sal_rows = conn.execute(f"""
            SELECT YEAR(CAST("{sal_date}" AS DATE)),
                   MONTH(CAST("{sal_date}" AS DATE)),
                   COALESCE(SUM(CAST("{sal_amt}" AS DOUBLE)), 0)
            FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE"
            WHERE YEAR(CAST("{sal_date}" AS DATE)) {years}
            GROUP BY 1, 2 ORDER BY 1, 2
        """).fetchall()

        # Expense & commission — split by category if possible
        if exp_cat:
            exp_rows = conn.execute(f"""
                SELECT YEAR(CAST("{exp_date}" AS DATE)),
                       MONTH(CAST("{exp_date}" AS DATE)),
                       LOWER(TRIM(COALESCE("{exp_cat}", 'expense'))),
                       COALESCE(SUM(CAST("{exp_amt}" AS DOUBLE)), 0)
                FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION"
                WHERE YEAR(CAST("{exp_date}" AS DATE)) {years}
                GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
            """).fetchall()
        else:
            # No category column — everything is expense
            exp_rows = conn.execute(f"""
                SELECT YEAR(CAST("{exp_date}" AS DATE)),
                       MONTH(CAST("{exp_date}" AS DATE)),
                       'expense' AS cat,
                       COALESCE(SUM(CAST("{exp_amt}" AS DOUBLE)), 0)
                FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION"
                WHERE YEAR(CAST("{exp_date}" AS DATE)) {years}
                GROUP BY 1, 2 ORDER BY 1, 2
            """).fetchall()

        # Cash by month (denominator)
        cash_rows = conn.execute(f"""
            SELECT YEAR(Date), MONTH(Date), COALESCE(SUM(Amount), 0)
            FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
            WHERE YEAR(Date) {years}
            GROUP BY 1, 2 ORDER BY 1, 2
        """).fetchall()
    finally:
        conn.close()

    # ── Aggregate ─────────────────────────────────────────────────────────────
    monthly: dict = defaultdict(lambda: {"sal_pal": 0., "expense": 0., "commission": 0.})

    for (yr, mo, amt) in sal_rows:
        monthly[(yr, mo)]["sal_pal"] += amt

    for row in exp_rows:
        yr, mo, cat, amt = row
        if "commiss" in cat:
            monthly[(yr, mo)]["commission"] += amt
        else:
            monthly[(yr, mo)]["expense"] += amt

    cash_map = {(r[0], r[1]): r[2] for r in cash_rows}

    result = []
    for (yr, mo) in sorted(monthly.keys()):
        d    = monthly[(yr, mo)]
        cash = cash_map.get((yr, mo), 0.)
        den  = cash if cash > 0 else 1.

        sal = round(d["sal_pal"], 2)
        exp = round(d["expense"], 2)
        com = round(d["commission"], 2)
        rem = round(cash - sal - exp - com, 2)

        result.append(CashBreakdownPoint(
            year=yr, month=mo, month_label=MONTH_LABELS[mo-1],
            total_cash=round(cash, 2),
            salary_palliative=sal, expense=exp, commission=com,
            salary_palliative_pct=round(sal/den*100, 2),
            expense_pct=          round(exp/den*100, 2),
            commission_pct=       round(com/den*100, 2),
            remaining=rem,
            remaining_pct=        round(rem/den*100, 2),
        ))

    return result