"""
Daily Executive Dashboard Batch Script
=======================================
Queries MotherDuck and populates 5 Supabase tables for the Vercel dashboard.

Tables:
  dashboard_kpi_cards         — 1 row of 8 KPI values
  dashboard_monthly_trends    — rows per (series, year, month)
  dashboard_contract_summary  — 1 row per active group
  dashboard_top20             — rows per (category, rank)
  dashboard_cash_breakdown    — rows per (year, month)

Run locally:
  MOTHERDUCK_TOKEN=... SUPABASE_URL=... SUPABASE_KEY=... python scripts/daily_executive_dashboard.py
"""

import os
from datetime import date
from collections import defaultdict

import duckdb
from supabase import create_client

# ── Config ─────────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.environ["MOTHERDUCK_TOKEN"]
SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_KEY     = os.environ["SUPABASE_KEY"]
SCHEMA = "AI DRIVEN DATA"

TODAY      = date.today()
YEAR_START = date(TODAY.year, 1, 1)
SPLY_START = date(TODAY.year - 1, 1, 1)
SPLY_END   = date(TODAY.year - 1, TODAY.month, TODAY.day)
CY = TODAY.year
PY = TODAY.year - 1

MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]


# ── Helpers ────────────────────────────────────────────────────────────────────
def q1(con, sql):
    return con.execute(sql).fetchone()[0] or 0.0


def monthly_by_year(con, table, amt_col, date_col, year, cast_date=False):
    d = f"CAST({date_col} AS DATE)" if cast_date else date_col
    rows = con.execute(f"""
        SELECT MONTH({d}), COALESCE(SUM(CAST({amt_col} AS DOUBLE)), 0)
        FROM "{SCHEMA}"."{table}"
        WHERE YEAR({d}) = {year}
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    return {r[0]: r[1] for r in rows}


def cumulative_mlr(claims_map, cash_map):
    cc = ck = 0.0
    result = {}
    for m in range(1, 13):
        cc += claims_map.get(m, 0.0)
        ck += cash_map.get(m, 0.0)
        result[m] = round(cc / ck * 100, 2) if ck else 0.0
    return result


def find_col(col_names, candidates):
    lower = {c.lower(): c for c in col_names}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def delete_all(sb, table):
    sb.table(table).delete().neq("id", 0).execute()


def insert_batched(sb, table, rows, batch=500):
    for i in range(0, len(rows), batch):
        sb.table(table).insert(rows[i:i+batch]).execute()


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"[{TODAY}] Starting executive dashboard batch...")
    con = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")
    sb  = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── 1. KPI CARDS ─────────────────────────────────────────────────────────
    print("1/5  KPI cards...")
    cash_ytd  = q1(con, f"""SELECT COALESCE(SUM(Amount),0) FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
                            WHERE Date >= DATE '{YEAR_START}' AND Date <= DATE '{TODAY}'""")
    enc_ytd   = q1(con, f"""SELECT COALESCE(SUM(CAST(approvedamount AS DOUBLE)),0) FROM "{SCHEMA}"."CLAIMS DATA"
                            WHERE CAST(encounterdatefrom AS DATE) >= DATE '{YEAR_START}'
                              AND CAST(encounterdatefrom AS DATE) <= DATE '{TODAY}'""")
    sub_ytd   = q1(con, f"""SELECT COALESCE(SUM(CAST(approvedamount AS DOUBLE)),0) FROM "{SCHEMA}"."CLAIMS DATA"
                            WHERE CAST(datesubmitted AS DATE) >= DATE '{YEAR_START}'
                              AND CAST(datesubmitted AS DATE) <= DATE '{TODAY}'""")
    cash_sply = q1(con, f"""SELECT COALESCE(SUM(Amount),0) FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
                            WHERE Date >= DATE '{SPLY_START}' AND Date <= DATE '{SPLY_END}'""")
    enc_sply  = q1(con, f"""SELECT COALESCE(SUM(CAST(approvedamount AS DOUBLE)),0) FROM "{SCHEMA}"."CLAIMS DATA"
                            WHERE CAST(encounterdatefrom AS DATE) >= DATE '{SPLY_START}'
                              AND CAST(encounterdatefrom AS DATE) <= DATE '{SPLY_END}'""")
    sub_sply  = q1(con, f"""SELECT COALESCE(SUM(CAST(approvedamount AS DOUBLE)),0) FROM "{SCHEMA}"."CLAIMS DATA"
                            WHERE CAST(datesubmitted AS DATE) >= DATE '{SPLY_START}'
                              AND CAST(datesubmitted AS DATE) <= DATE '{SPLY_END}'""")

    sb.table("dashboard_kpi_cards").upsert([{
        "id": 1,
        "cash_collected_ytd":          round(cash_ytd, 2),
        "claims_paid_encounter_ytd":   round(enc_ytd, 2),
        "claims_paid_submitted_ytd":   round(sub_ytd, 2),
        "cash_collected_sply":         round(cash_sply, 2),
        "claims_paid_encounter_sply":  round(enc_sply, 2),
        "claims_paid_submitted_sply":  round(sub_sply, 2),
        "mlr_encounter_pct":           round(enc_ytd / cash_ytd * 100, 2) if cash_ytd else 0.0,
        "mlr_submitted_pct":           round(sub_ytd / cash_ytd * 100, 2) if cash_ytd else 0.0,
        "ytd_from":    str(YEAR_START),
        "ytd_to":      str(TODAY),
        "sply_from":   str(SPLY_START),
        "sply_to":     str(SPLY_END),
        "current_year": CY,
        "last_year":    PY,
    }], on_conflict="id").execute()
    print(f"     Cash YTD: {cash_ytd:,.0f}  MLR(enc): {round(enc_ytd/cash_ytd*100,1) if cash_ytd else 0}%")

    # ── 2. MONTHLY TRENDS ────────────────────────────────────────────────────
    print("2/5  Monthly trends...")
    cash_cy = monthly_by_year(con, "CLIENT_CASH_RECEIVED", "Amount", "Date", CY)
    cash_py = monthly_by_year(con, "CLIENT_CASH_RECEIVED", "Amount", "Date", PY)
    enc_cy  = monthly_by_year(con, "CLAIMS DATA", "approvedamount", "encounterdatefrom", CY, True)
    enc_py  = monthly_by_year(con, "CLAIMS DATA", "approvedamount", "encounterdatefrom", PY, True)
    sub_cy  = monthly_by_year(con, "CLAIMS DATA", "approvedamount", "datesubmitted", CY, True)
    sub_py  = monthly_by_year(con, "CLAIMS DATA", "approvedamount", "datesubmitted", PY, True)
    pa_cy   = monthly_by_year(con, "PA DATA", "granted", "requestdate", CY, True)
    pa_py   = monthly_by_year(con, "PA DATA", "granted", "requestdate", PY, True)
    mlr_enc_cy = cumulative_mlr(enc_cy, cash_cy)
    mlr_sub_cy = cumulative_mlr(sub_cy, cash_cy)

    trend_rows = []
    for series, year_map in [
        ("cash",             {CY: cash_cy, PY: cash_py}),
        ("claims_encounter", {CY: enc_cy,  PY: enc_py}),
        ("claims_submitted", {CY: sub_cy,  PY: sub_py}),
        ("pa_cost",          {CY: pa_cy,   PY: pa_py}),
        ("mlr_encounter",    {CY: mlr_enc_cy}),
        ("mlr_submitted",    {CY: mlr_sub_cy}),
    ]:
        for year, month_map in year_map.items():
            for m in range(1, 13):
                trend_rows.append({
                    "series": series, "year": year, "month": m,
                    "month_label": MONTH_LABELS[m - 1],
                    "value": round(month_map.get(m, 0.0), 2),
                })

    delete_all(sb, "dashboard_monthly_trends")
    insert_batched(sb, "dashboard_monthly_trends", trend_rows)
    print(f"     {len(trend_rows)} trend rows saved")

    # ── 3. CONTRACT SUMMARY ──────────────────────────────────────────────────
    print("3/5  Contract summary...")
    active = con.execute(f"""
        SELECT groupid, groupname, CAST(startdate AS DATE), CAST(enddate AS DATE)
        FROM "{SCHEMA}"."GROUP_CONTRACT"
        WHERE iscurrent = TRUE
        ORDER BY groupname
    """).fetchall()

    contract_rows = []
    for (gid, gname, cur_start, cur_end) in active:
        s = gname.replace("'", "''")

        prev = con.execute(f"""
            SELECT CAST(startdate AS DATE), CAST(enddate AS DATE)
            FROM "{SCHEMA}"."GROUP_CONTRACT"
            WHERE groupid = {gid} AND iscurrent = FALSE
              AND CAST(enddate AS DATE) < DATE '{cur_start}'
            ORDER BY enddate DESC LIMIT 1
        """).fetchone()

        cur_debit = con.execute(f"""
            SELECT COALESCE(SUM(CAST("Amount" AS DOUBLE)), 0)
            FROM "{SCHEMA}"."DEBIT_NOTE"
            WHERE UPPER("CompanyName") = UPPER('{s}')
              AND CAST("From" AS DATE) >= DATE '{cur_start}'
              AND CAST("From" AS DATE) <= DATE '{cur_end}'
              AND LOWER(COALESCE("Description",'')) NOT LIKE '%tpa%'
        """).fetchone()[0]

        prev_start = prev[0] if prev else None
        prev_end   = prev[1] if prev else None
        prev_debit = None
        if prev_start and prev_end:
            prev_debit = con.execute(f"""
                SELECT COALESCE(SUM(CAST("Amount" AS DOUBLE)), 0)
                FROM "{SCHEMA}"."DEBIT_NOTE"
                WHERE UPPER("CompanyName") = UPPER('{s}')
                  AND CAST("From" AS DATE) >= DATE '{prev_start}'
                  AND CAST("From" AS DATE) <= DATE '{prev_end}'
                  AND LOWER(COALESCE("Description",'')) NOT LIKE '%tpa%'
            """).fetchone()[0]

        cash = con.execute(f"""
            SELECT COALESCE(SUM(Amount), 0)
            FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
            WHERE UPPER(groupname) = UPPER('{s}')
              AND Date >= DATE '{cur_start}'
              AND Date <= DATE '{cur_end}'
        """).fetchone()[0]

        contract_rows.append({
            "group_name":              gname.strip(),
            "previous_contract_start": str(prev_start) if prev_start else None,
            "previous_contract_end":   str(prev_end)   if prev_end   else None,
            "previous_debit":          round(float(prev_debit), 2) if prev_debit else None,
            "current_contract_start":  str(cur_start),
            "current_contract_end":    str(cur_end),
            "current_debit":           round(float(cur_debit), 2) if cur_debit else None,
            "cash_collected_current":  round(float(cash), 2),
            "cash_vs_debit_pct":       round(cash / cur_debit * 100, 2) if cur_debit else None,
        })

    delete_all(sb, "dashboard_contract_summary")
    insert_batched(sb, "dashboard_contract_summary", contract_rows)
    print(f"     {len(contract_rows)} groups saved")

    # ── 4. TOP 20 ────────────────────────────────────────────────────────────
    print("4/5  Top 20s...")
    top20_rows = []

    for rank, gname, amt in con.execute(f"""
        SELECT ROW_NUMBER() OVER (ORDER BY SUM(CAST(granted AS DOUBLE)) DESC),
               groupname, ROUND(SUM(CAST(granted AS DOUBLE)), 2)
        FROM "{SCHEMA}"."PA DATA"
        WHERE CAST(requestdate AS DATE) >= DATE '{YEAR_START}'
          AND CAST(requestdate AS DATE) <= DATE '{TODAY}'
          AND groupname IS NOT NULL AND TRIM(groupname) != ''
        GROUP BY groupname ORDER BY 3 DESC LIMIT 20
    """).fetchall():
        top20_rows.append({"category": "pa_clients", "rank": rank,
                           "name": gname, "provider_id": None, "provider_band": None, "amount_ytd": amt})

    for rank, pid, pname, band, amt in con.execute(f"""
        SELECT ROW_NUMBER() OVER (ORDER BY SUM(CAST(pa.granted AS DOUBLE)) DESC),
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
    """).fetchall():
        top20_rows.append({"category": "pa_providers", "rank": rank,
                           "name": pname, "provider_id": str(pid), "provider_band": band, "amount_ytd": amt})

    for rank, gname, amt in con.execute(f"""
        SELECT ROW_NUMBER() OVER (ORDER BY SUM(CAST(cd.approvedamount AS DOUBLE)) DESC),
               COALESCE(g.groupname, cd.nhisgroupid),
               ROUND(SUM(CAST(cd.approvedamount AS DOUBLE)), 2)
        FROM "{SCHEMA}"."CLAIMS DATA" cd
        LEFT JOIN "{SCHEMA}"."GROUPS" g ON TRY_CAST(cd.nhisgroupid AS BIGINT) = g.groupid
        WHERE CAST(cd.encounterdatefrom AS DATE) >= DATE '{YEAR_START}'
          AND CAST(cd.encounterdatefrom AS DATE) <= DATE '{TODAY}'
          AND cd.nhisgroupid IS NOT NULL AND TRIM(cd.nhisgroupid) != ''
        GROUP BY 2 ORDER BY 3 DESC LIMIT 20
    """).fetchall():
        top20_rows.append({"category": "claims_enc_clients", "rank": rank,
                           "name": str(gname), "provider_id": None, "provider_band": None, "amount_ytd": amt})

    for rank, gname, amt in con.execute(f"""
        SELECT ROW_NUMBER() OVER (ORDER BY SUM(CAST(cd.approvedamount AS DOUBLE)) DESC),
               COALESCE(g.groupname, cd.nhisgroupid),
               ROUND(SUM(CAST(cd.approvedamount AS DOUBLE)), 2)
        FROM "{SCHEMA}"."CLAIMS DATA" cd
        LEFT JOIN "{SCHEMA}"."GROUPS" g ON TRY_CAST(cd.nhisgroupid AS BIGINT) = g.groupid
        WHERE CAST(cd.datesubmitted AS DATE) >= DATE '{YEAR_START}'
          AND CAST(cd.datesubmitted AS DATE) <= DATE '{TODAY}'
          AND cd.nhisgroupid IS NOT NULL AND TRIM(cd.nhisgroupid) != ''
        GROUP BY 2 ORDER BY 3 DESC LIMIT 20
    """).fetchall():
        top20_rows.append({"category": "claims_sub_clients", "rank": rank,
                           "name": str(gname), "provider_id": None, "provider_band": None, "amount_ytd": amt})

    delete_all(sb, "dashboard_top20")
    sb.table("dashboard_top20").insert(top20_rows).execute()
    print(f"     {len(top20_rows)} top-20 rows saved")

    # ── 5. CASH BREAKDOWN ────────────────────────────────────────────────────
    print("5/5  Cash breakdown...")
    DATE_CANDS   = ["date","month","period","pay_date","payment_date","transaction_date","created_date","postingdate"]
    AMOUNT_CANDS = ["amount","value","total","naira_amount","sum","payment","debit"]
    CAT_CANDS    = ["category","type","expense_type","description","item","account","gl_account","name","narration"]

    def get_cols(table):
        return [r[0] for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{SCHEMA}' AND table_name = '{table}'
        """).fetchall()]

    sal_cols = get_cols("SALARY_AND_PALLIATIVE")
    exp_cols = get_cols("EXPENSE_AND_COMMISSION")
    sal_date = find_col(sal_cols, DATE_CANDS)
    sal_amt  = find_col(sal_cols, AMOUNT_CANDS)
    exp_date = find_col(exp_cols, DATE_CANDS)
    exp_amt  = find_col(exp_cols, AMOUNT_CANDS)
    exp_cat  = find_col(exp_cols, CAT_CANDS)

    years_clause = f"IN ({CY}, {PY})"

    sal_rows = con.execute(f"""
        SELECT YEAR(CAST("{sal_date}" AS DATE)), MONTH(CAST("{sal_date}" AS DATE)),
               COALESCE(SUM(CAST("{sal_amt}" AS DOUBLE)), 0)
        FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE"
        WHERE YEAR(CAST("{sal_date}" AS DATE)) {years_clause}
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()

    if exp_cat:
        exp_rows = con.execute(f"""
            SELECT YEAR(CAST("{exp_date}" AS DATE)), MONTH(CAST("{exp_date}" AS DATE)),
                   LOWER(TRIM(COALESCE("{exp_cat}", 'expense'))),
                   COALESCE(SUM(CAST("{exp_amt}" AS DOUBLE)), 0)
            FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION"
            WHERE YEAR(CAST("{exp_date}" AS DATE)) {years_clause}
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
        """).fetchall()
    else:
        exp_rows = [(yr, mo, "expense", amt) for yr, mo, amt in con.execute(f"""
            SELECT YEAR(CAST("{exp_date}" AS DATE)), MONTH(CAST("{exp_date}" AS DATE)),
                   COALESCE(SUM(CAST("{exp_amt}" AS DOUBLE)), 0)
            FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION"
            WHERE YEAR(CAST("{exp_date}" AS DATE)) {years_clause}
            GROUP BY 1, 2 ORDER BY 1, 2
        """).fetchall()]

    cash_rows = con.execute(f"""
        SELECT YEAR(Date), MONTH(Date), COALESCE(SUM(Amount), 0)
        FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
        WHERE YEAR(Date) {years_clause}
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()

    monthly: dict = defaultdict(lambda: {"sal_pal": 0.0, "expense": 0.0, "commission": 0.0})
    for yr, mo, amt in sal_rows:
        monthly[(yr, mo)]["sal_pal"] += amt
    for yr, mo, cat, amt in exp_rows:
        monthly[(yr, mo)]["commission" if "commiss" in cat else "expense"] += amt
    cash_map = {(r[0], r[1]): r[2] for r in cash_rows}

    cb_rows = []
    for (yr, mo) in sorted(monthly.keys()):
        d    = monthly[(yr, mo)]
        cash = cash_map.get((yr, mo), 0.0)
        den  = cash if cash > 0 else 1.0
        sal  = round(d["sal_pal"], 2)
        exp  = round(d["expense"], 2)
        com  = round(d["commission"], 2)
        rem  = round(cash - sal - exp - com, 2)
        cb_rows.append({
            "year": yr, "month": mo, "month_label": MONTH_LABELS[mo - 1],
            "total_cash":             round(cash, 2),
            "salary_palliative":      sal,
            "expense":                exp,
            "commission":             com,
            "salary_palliative_pct":  round(sal / den * 100, 2),
            "expense_pct":            round(exp / den * 100, 2),
            "commission_pct":         round(com / den * 100, 2),
            "remaining":              rem,
            "remaining_pct":          round(rem / den * 100, 2),
        })

    delete_all(sb, "dashboard_cash_breakdown")
    sb.table("dashboard_cash_breakdown").insert(cb_rows).execute()
    print(f"     {len(cb_rows)} cash breakdown rows saved")

    con.close()
    print(f"[{TODAY}] Done.")


if __name__ == "__main__":
    main()
