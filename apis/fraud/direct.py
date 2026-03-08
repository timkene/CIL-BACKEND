"""
apis/fraud/direct.py
=====================
Direct (no-HTTP) fraud scoring — used by GitHub Actions scripts so they
don't need a running fraud API server.

Exposes:
    score_provider_direct(conn, provider_name, start_date, end_date, group_id=None)
        → dict  (same shape as the /fraud-score HTTP response)

    score_all_providers(conn, start_date, end_date, group_id=None, limit=None)
        → list[dict]
"""

import traceback
from datetime import date
from typing import Optional

import duckdb
import pandas as pd

from .config import (
    ALERT_THRESHOLD, WATCHLIST_THRESHOLD,
    NETWORK_CPE_MIN_GROUPS, NETWORK_CPE_RATIO_THRESHOLD,
    EXCLUDE_NAME_KEYWORDS,
)
from .db import (
    resolve_provider,
    get_provider_raw_metrics,
    get_band_benchmarks,
    get_dx_repeat_metrics,
    get_short_interval_metrics,
    get_network_cpe_benchmark,
    _normalize_pid,
)
from .scorer import build_peer_benchmarks, score_metrics, get_alert_status
from .models import NetworkSignal


# ─── single provider ──────────────────────────────────────────────────────────
def score_provider_direct(
    conn: duckdb.DuckDBPyConnection,
    provider_name: str,
    start_date: str,
    end_date: str,
    group_id: Optional[str] = None,
) -> Optional[dict]:
    """
    Score a single provider directly using DB functions.
    Returns a dict matching the /fraud-score HTTP response, or None on failure.
    """
    try:
        provider = resolve_provider(conn, None, provider_name)
        if not provider:
            return {"_status": "NOT_FOUND"}

        pid  = provider["provider_id"]
        band = provider["band"]

        raw_df = get_provider_raw_metrics(conn, pid, start_date, end_date)
        if raw_df.empty:
            return {"_status": "NOT_FOUND"}

        row = raw_df.iloc[0]
        unique_enrollees = int(row["unique_enrollees"] or 0)
        if unique_enrollees == 0:
            return {"_status": "NOT_FOUND"}

        total_cost = float(row["total_cost"] or 0)
        drug_cost  = float(row["drug_cost"]  or 0)
        pa_visits  = int(row["pa_visits"]    or 0)
        no_pa      = int(row["no_pa_visits"] or 0)
        total_visits = pa_visits + no_pa
        cpe = round(total_cost / unique_enrollees, 2) if unique_enrollees else 0
        cpv = round(total_cost / total_visits, 2)     if total_visits     else 0
        vpe = round(total_visits / unique_enrollees, 2) if unique_enrollees else 0
        drug_ratio_pct = round(drug_cost / total_cost * 100, 2) if total_cost else 0

        raw_dict = dict(
            total_cost=total_cost, unique_enrollees=unique_enrollees,
            total_visits=total_visits, pa_visits=pa_visits, no_pa_visits=no_pa,
            drug_cost=drug_cost, cpe=cpe, cpv=cpv, vpe=vpe,
            drug_ratio_pct=drug_ratio_pct,
        )

        # peer benchmarks
        bench_df  = get_band_benchmarks(conn, band, start_date, end_date)
        bench_row = bench_df.iloc[0] if not bench_df.empty else pd.Series(dtype=float)

        # behavioral
        dx_summary, _ = get_dx_repeat_metrics(conn, pid, start_date, end_date)
        dx_repeat_rate = float(dx_summary["repeat_rate_pct"].iloc[0] or 0) if not dx_summary.empty else 0

        short_summary, _ = get_short_interval_metrics(conn, pid, start_date, end_date)
        short_pct = float(short_summary["short_interval_pct"].iloc[0] or 0) if not short_summary.empty else 0

        metric_scores, total_score, warnings = score_metrics(
            raw_dict, bench_row, dx_repeat_rate, short_pct,
        )
        alert_status, alert_emoji = get_alert_status(total_score)

        # network benchmark
        network_signals = []
        if group_id:
            norm_pid = _normalize_pid(pid)
            net_df = get_network_cpe_benchmark(conn, group_id, [norm_pid], start_date, end_date)
            for _, nrow in net_df.iterrows():
                cpe_ratio     = float(nrow.get("cpe_ratio") or 0)
                groups_served = int(nrow.get("groups_served") or 0)
                signal = (
                    "GROUP_TARGETED" if groups_served >= NETWORK_CPE_MIN_GROUPS and cpe_ratio > NETWORK_CPE_RATIO_THRESHOLD
                    else "INSUFFICIENT_DATA" if groups_served < NETWORK_CPE_MIN_GROUPS or cpe_ratio == 0
                    else "CLEAN"
                )
                network_signals.append(dict(
                    provider_id=str(nrow.get("provider_id", pid)),
                    provider_name=str(nrow.get("providername") or provider["provider_name"]),
                    cpe_this_group=round(float(nrow.get("cpe_this_group") or 0), 2),
                    cpe_network=round(float(nrow.get("cpe_network") or 0), 2),
                    groups_served=groups_served,
                    encounters_this_group=int(nrow.get("encounters_this_group") or 0),
                    cpe_ratio=round(cpe_ratio, 2),
                    network_signal=signal,
                ))
            if net_df.empty:
                network_signals.append(dict(
                    provider_id=pid, provider_name=provider["provider_name"],
                    cpe_this_group=0, cpe_network=0, groups_served=0,
                    encounters_this_group=0, cpe_ratio=0,
                    network_signal="INSUFFICIENT_DATA",
                ))

        return dict(
            provider=provider,
            period=dict(start_date=start_date, end_date=end_date),
            raw_metrics=raw_dict,
            peer_benchmarks=[b.model_dump() for b in build_peer_benchmarks(bench_row, band)] if not bench_row.empty else [],
            metric_scores=[m.model_dump() for m in metric_scores],
            total_score=total_score,
            max_score=10,
            alert_status=alert_status,
            alert_emoji=alert_emoji,
            network_signals=network_signals,
            warnings=warnings,
        )

    except Exception:
        traceback.print_exc()
        return None


# ─── bulk: all providers with claims in a period ─────────────────────────────
def score_all_providers(
    conn: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
    group_id: Optional[str] = None,
    min_enrollees: int = 5,
    limit: Optional[int] = None,
) -> list:
    """
    Score every provider that had ≥ min_enrollees unique enrollees in the period.
    Skips pharmacies / labs / diagnostics (EXCLUDE_NAME_KEYWORDS).
    Returns list of result dicts — same shape as score_provider_direct().
    """
    from .config import DB_SCHEMA
    sql = f"""
        SELECT
            p.providerid,
            p.providername,
            COUNT(DISTINCT c.enrollee_id) AS unique_enrollees
        FROM "{DB_SCHEMA}"."CLAIMS DATA" c
        JOIN "{DB_SCHEMA}"."PROVIDERS" p
            ON TRY_CAST(c.nhisproviderid AS BIGINT) = TRY_CAST(p.providerid AS BIGINT)
        WHERE c.encounterdatefrom BETWEEN ? AND ?
          AND p.isvisible = true
        GROUP BY p.providerid, p.providername
        HAVING COUNT(DISTINCT c.enrollee_id) >= ?
        ORDER BY unique_enrollees DESC
        {"LIMIT " + str(limit) if limit else ""}
    """
    providers_df = conn.execute(sql, [start_date, end_date, min_enrollees]).fetchdf()

    # Filter out non-hospital provider types
    exclude = EXCLUDE_NAME_KEYWORDS
    providers_df = providers_df[
        ~providers_df["providername"].str.lower().str.contains(
            "|".join(exclude), na=False
        )
    ]

    results = []
    total = len(providers_df)
    for i, row in providers_df.iterrows():
        name = row["providername"]
        print(f"  [{i+1}/{total}] Scoring {name} ...", flush=True)
        result = score_provider_direct(conn, name, start_date, end_date, group_id)
        if result and result.get("_status") != "NOT_FOUND":
            results.append(result)

    return results
