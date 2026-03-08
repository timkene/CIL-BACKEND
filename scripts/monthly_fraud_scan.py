"""
monthly_fraud_scan.py
======================
Monthly batch: score ALL active providers for the previous calendar month.
Providers with alert_status ALERT or WATCHLIST are saved to Supabase.

Run schedule: 1st of every month via GitHub Actions.

Env vars required:
    MOTHERDUCK_TOKEN
    SUPABASE_URL
    SUPABASE_KEY

Supabase table (run once in SQL editor):
    CREATE TABLE provider_fraud_scores (
        id              BIGSERIAL PRIMARY KEY,
        scan_month      VARCHAR(7)   NOT NULL,   -- 'YYYY-MM'
        provider_id     TEXT         NOT NULL,
        provider_name   TEXT         NOT NULL,
        band            TEXT,
        state           TEXT,
        total_score     INT,
        max_score       INT          DEFAULT 10,
        alert_status    TEXT,        -- 'ALERT' | 'WATCHLIST' | 'CLEAR'
        total_cost      FLOAT,
        unique_enrollees INT,
        cpe             FLOAT,
        cpv             FLOAT,
        vpe             FLOAT,
        drug_ratio_pct  FLOAT,
        dx_repeat_pct   FLOAT,
        short_interval_pct FLOAT,
        network_signal  TEXT,
        cpe_ratio       FLOAT,
        groups_served   INT,
        scanned_at      TIMESTAMPTZ  DEFAULT NOW()
    );
    ALTER TABLE provider_fraud_scores ENABLE ROW LEVEL SECURITY;
    CREATE POLICY "Allow anon read" ON provider_fraud_scores FOR SELECT USING (true);
"""

import os
import sys
import traceback
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import duckdb
from supabase import create_client, Client

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")
MOTHERDUCK_DSN   = f"md:ai_driven_data?motherduck_token={MOTHERDUCK_TOKEN}"
SUPABASE_URL     = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY     = os.getenv("SUPABASE_KEY", "")

TABLE = "provider_fraud_scores"

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

from apis.fraud.direct import score_all_providers


def prev_month_range() -> tuple[str, str, str]:
    """Return (start_date, end_date, scan_month) for the previous calendar month."""
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month  = first_of_this_month - timedelta(days=1)
    first_of_prev_month = last_of_prev_month.replace(day=1)
    scan_month = first_of_prev_month.strftime("%Y-%m")
    return str(first_of_prev_month), str(last_of_prev_month), scan_month


def delete_previous_scan(scan_month: str) -> None:
    """Delete all rows for this scan_month before inserting fresh results."""
    sb.table(TABLE).delete().eq("scan_month", scan_month).execute()
    print(f"  Cleared previous scan data for {scan_month}")


def _clean(v):
    """Convert NaN/Inf/numpy floats to JSON-safe Python scalars."""
    import math
    # Handle numpy scalar types (float32, float64, int32, int64, etc.)
    try:
        import numpy as np
        if isinstance(v, (np.floating,)):
            if np.isnan(v) or np.isinf(v):
                return None
            return float(v)
        if isinstance(v, np.integer):
            return int(v)
    except ImportError:
        pass
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def save_results(scan_month: str, results: list) -> int:
    """Insert scored providers into Supabase. Returns count saved."""
    rows = []
    for r in results:
        alert = r.get("alert_status", "CLEAR")
        # Save all statuses — frontend can filter
        prov     = r.get("provider", {})
        raw      = r.get("raw_metrics", {})
        signals  = r.get("network_signals", [])
        net_sig  = signals[0] if signals else {}

        # Pull per-metric values from metric_scores
        metric_map = {m["metric"]: m["value"] for m in r.get("metric_scores", [])}

        rows.append({
            "scan_month":          scan_month,
            "provider_id":         prov.get("provider_id", ""),
            "provider_name":       prov.get("provider_name", ""),
            "band":                prov.get("band"),
            "state":               prov.get("state"),
            "total_score":         r.get("total_score"),
            "max_score":           r.get("max_score", 10),
            "alert_status":        alert,
            "total_cost":          _clean(raw.get("total_cost")),
            "unique_enrollees":    _clean(raw.get("unique_enrollees")),
            "cpe":                 _clean(raw.get("cpe")),
            "cpv":                 _clean(raw.get("cpv")),
            "vpe":                 _clean(raw.get("vpe")),
            "drug_ratio_pct":      _clean(raw.get("drug_ratio_pct")),
            "dx_repeat_pct":       _clean(metric_map.get("Dx Repeat Rate")),
            "short_interval_pct":  _clean(metric_map.get("Short Interval")),
            "network_signal":      net_sig.get("network_signal"),
            "cpe_ratio":           _clean(net_sig.get("cpe_ratio")),
            "groups_served":       _clean(net_sig.get("groups_served")),
        })

    if not rows:
        return 0

    # Batch insert in chunks of 100
    for i in range(0, len(rows), 100):
        sb.table(TABLE).insert(rows[i:i+100]).execute()

    return len(rows)


def main():
    start_date, end_date, scan_month = prev_month_range()
    print(f"\n=== Monthly Fraud Scan: {scan_month} ({start_date} → {end_date}) ===")

    print("\n[1/4] Connecting to MotherDuck ...")
    conn = duckdb.connect(MOTHERDUCK_DSN)

    print("\n[2/4] Scoring all active providers ...")
    results = score_all_providers(conn, start_date, end_date)
    conn.close()

    alert_count     = sum(1 for r in results if r.get("alert_status") == "ALERT")
    watchlist_count = sum(1 for r in results if r.get("alert_status") == "WATCHLIST")
    clear_count     = sum(1 for r in results if r.get("alert_status") == "CLEAR")
    print(f"\n  Scored {len(results)} providers: "
          f"{alert_count} ALERT, {watchlist_count} WATCHLIST, {clear_count} CLEAR")

    print("\n[3/4] Clearing previous scan data from Supabase ...")
    delete_previous_scan(scan_month)

    print("\n[4/4] Saving results to Supabase ...")
    saved = save_results(scan_month, results)
    print(f"  Saved {saved} rows to '{TABLE}'")

    print(f"\n=== Done. {alert_count} providers flagged for review. ===\n")


if __name__ == "__main__":
    main()
