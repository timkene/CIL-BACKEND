"""
Fraud Scoring API - Main FastAPI Application
=============================================
Endpoints:
  POST /fraud-score          → full fraud score for one provider
  GET  /provider/search      → resolve provider name/id to providerid + band
  GET  /health               → health check

Run with:
    uvicorn main:app --reload --port 8001
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from datetime import datetime
import duckdb

from .config   import DB_PATH
from .models   import (
    FraudScoreRequest, FraudScoreResponse, ErrorResponse,
    ProviderInfo, RawMetrics, BehavioralMetrics, DxRepeat,
    ShortIntervalBucket, NetworkSignal,
)
from .db       import (
    resolve_provider, get_provider_raw_metrics,
    get_band_benchmarks, get_dx_repeat_metrics,
    get_short_interval_metrics, get_network_cpe_benchmark,
)
from .scorer   import build_peer_benchmarks, score_metrics, get_alert_status
from .config   import NETWORK_CPE_MIN_GROUPS, NETWORK_CPE_RATIO_THRESHOLD
from .ai_analyst import get_ai_commentary


app = FastAPI(
    title       = "Clearline Provider Fraud Scoring API",
    description = "Automated fraud detection for HMO provider network using "
                  "claims data, peer benchmarking (Tukey fence), and AI medical judgment.",
    version     = "1.0.0",
)


def _open_db() -> duckdb.DuckDBPyConnection:
    """Open DuckDB in read-only mode."""
    try:
        return duckdb.connect(DB_PATH, read_only=True)
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database connection failed: {str(e)}. "
                   f"Close any other connections (Streamlit, Jupyter) first."
        )


# ─── Health check ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["Utility"])
def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


# ─── Provider search ──────────────────────────────────────────────────────────
@app.get("/provider/search", tags=["Utility"])
def search_provider(
    name: str = Query(None, description="Provider name (partial match)"),
    id:   str = Query(None, description="Provider ID"),
):
    """Resolve a provider name or ID → provider details + band."""
    if not name and not id:
        raise HTTPException(status_code=400, detail="Supply at least `name` or `id`.")
    conn = _open_db()
    try:
        result = resolve_provider(conn, provider_id=id, provider_name=name)
        if not result:
            raise HTTPException(status_code=404, detail="Provider not found.")
        return result
    finally:
        conn.close()


# ─── Main fraud score endpoint ────────────────────────────────────────────────
@app.post(
    "/fraud-score",
    response_model      = FraudScoreResponse,
    responses           = {400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
    tags                = ["Fraud Scoring"],
    summary             = "Compute full fraud score for a provider",
)
async def fraud_score(req: FraudScoreRequest):
    """
    Compute a full 10-point fraud score for a provider over a date range.

    Metrics scored:
    - VPE  (Visits per Enrollee)     — 1 pt
    - CPE  (Cost per Enrollee)       — 2 pts
    - CPV  (Cost per Visit)          — 2 pts
    - Drug Ratio                     — 2 pts  (>55%→2, >40%→1)
    - Diagnosis Repeat Rate          — 1 pt
    - Short Visit Interval           — 2 pts

    Thresholds for VPE/CPE/CPV use Tukey fence (Q3 + 1.5×IQR) across band peers.
    Alert status: CLEAR (<3), WATCHLIST (3–4), ALERT (≥5).

    Visit definition:
    - Real panumber present → panumber = 1 visit
    - No panumber           → enrollee_id + date = 1 visit
    """

    # ── Validate input ────────────────────────────────────────────────────────
    if not req.provider_id and not req.provider_name:
        raise HTTPException(status_code=400, detail="Supply provider_id or provider_name.")
    if req.start_date >= req.end_date:
        raise HTTPException(status_code=400, detail="start_date must be before end_date.")

    start_str = req.start_date.isoformat()
    end_str   = req.end_date.isoformat()

    conn = _open_db()
    try:
        # ── 1. Resolve provider ───────────────────────────────────────────────
        provider = resolve_provider(conn, req.provider_id, req.provider_name)
        if not provider:
            raise HTTPException(status_code=404, detail="Provider not found in PROVIDERS table.")

        pid  = provider["provider_id"]
        band = provider["band"]

        # ── 2. Raw metrics ────────────────────────────────────────────────────
        raw_df = get_provider_raw_metrics(conn, pid, start_str, end_str)
        if raw_df.empty or raw_df["unique_enrollees"].iloc[0] == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No claims found for provider {pid} between {start_str} and {end_str}."
            )

        r             = raw_df.iloc[0]
        pa_visits     = int(r["pa_visits"])
        no_pa_visits  = int(r["no_pa_visits"])
        total_visits  = pa_visits + no_pa_visits
        total_cost    = float(r["total_cost"] or 0)
        drug_cost     = float(r["drug_cost"]  or 0)
        unique_enr    = int(r["unique_enrollees"])

        cpe           = total_cost / unique_enr          if unique_enr  else 0
        cpv           = total_cost / total_visits        if total_visits else 0
        vpe           = total_visits / unique_enr        if unique_enr  else 0
        drug_ratio    = (drug_cost / total_cost * 100)   if total_cost  else 0

        raw_metrics = RawMetrics(
            total_cost       = round(total_cost, 2),
            unique_enrollees = unique_enr,
            total_visits     = total_visits,
            pa_visits        = pa_visits,
            no_pa_visits     = no_pa_visits,
            drug_cost        = round(drug_cost, 2),
            cpe              = round(cpe,        2),
            cpv              = round(cpv,        2),
            vpe              = round(vpe,        2),
            drug_ratio_pct   = round(drug_ratio, 2),
        )

        # ── 3. Peer benchmarks ────────────────────────────────────────────────
        bench_df  = get_band_benchmarks(conn, band, start_str, end_str)
        bench_row = bench_df.iloc[0] if not bench_df.empty else {}

        peer_benchmarks = build_peer_benchmarks(bench_row, band) if not bench_df.empty else []

        # ── 4. Behavioral metrics ─────────────────────────────────────────────
        dx_summary, top_dx_df   = get_dx_repeat_metrics(conn, pid, start_str, end_str)
        si_summary, si_buckets  = get_short_interval_metrics(conn, pid, start_str, end_str)

        dx_row = dx_summary.iloc[0] if not dx_summary.empty else {}
        si_row = si_summary.iloc[0] if not si_summary.empty else {}

        repeated_pairs  = int(dx_row.get("repeated_pairs", 0) or 0)
        total_pairs     = int(dx_row.get("total_pairs",    0) or 0)
        dx_repeat_rate  = float(dx_row.get("repeat_rate_pct", 0) or 0)

        short_enr       = int(si_row.get("short_interval_enrollees", 0) or 0)
        multi_visit_enr = int(si_row.get("multi_visit_enrollees",    0) or 0)
        short_pct       = float(si_row.get("short_interval_pct",     0) or 0)
        avg_gap_short   = si_row.get("avg_gap_short")
        avg_gap_short   = float(avg_gap_short) if avg_gap_short else None

        top_dx = [
            DxRepeat(
                diagnosis_code = row["diagnosiscode"],
                diagnosis_desc = str(row.get("diagnosisdesc", "") or ""),
                repeat_count   = int(row["repeat_count"]),
            )
            for _, row in top_dx_df.iterrows()
        ]

        si_bucket_list = [
            ShortIntervalBucket(
                bucket        = row["bucket"],
                enrollee_count= int(row["enrollee_count"]),
                mean_gap_days = float(row["mean_gap_days"]),
            )
            for _, row in si_buckets.iterrows()
        ]

        behavioral = BehavioralMetrics(
            dx_repeat_rate_pct         = dx_repeat_rate,
            repeated_dx_pairs          = repeated_pairs,
            total_dx_pairs             = total_pairs,
            top_repeated_diagnoses     = top_dx,
            short_interval_enrollees   = short_enr,
            multi_visit_enrollees      = multi_visit_enr,
            short_interval_pct         = short_pct,
            avg_gap_short_interval     = avg_gap_short,
            short_interval_breakdown   = si_bucket_list,
        )

        # ── 5. Score ──────────────────────────────────────────────────────────
        raw_dict = raw_metrics.model_dump()
        metric_scores, total_score, warnings = score_metrics(
            raw=raw_dict,
            bench_row    = bench_row if not bench_df.empty else {},
            dx_repeat_rate   = dx_repeat_rate,
            short_interval_pct= short_pct,
        )
        alert_status, alert_emoji = get_alert_status(total_score)

        # ── 6. Network CPE Benchmark (optional — requires group_id) ──────────
        network_signals: list[NetworkSignal] = []
        if req.group_id:
            try:
                net_df = get_network_cpe_benchmark(
                    conn, req.group_id, [pid], start_str, end_str
                )
                for _, row in net_df.iterrows():
                    cpe_ratio     = float(row.get("cpe_ratio")     or 0)
                    groups_served = int(row.get("groups_served")    or 0)
                    if groups_served < NETWORK_CPE_MIN_GROUPS or cpe_ratio == 0:
                        signal = "INSUFFICIENT_DATA"
                    elif cpe_ratio > NETWORK_CPE_RATIO_THRESHOLD:
                        signal = "GROUP_TARGETED"
                    else:
                        signal = "CLEAN"
                    network_signals.append(NetworkSignal(
                        provider_id           = str(row.get("provider_id", pid)),
                        provider_name         = str(row.get("providername") or ""),
                        cpe_this_group        = round(float(row.get("cpe_this_group") or 0), 2),
                        cpe_network           = round(float(row.get("cpe_network")    or 0), 2),
                        groups_served         = groups_served,
                        encounters_this_group = int(row.get("encounters_this_group") or 0),
                        cpe_ratio             = round(cpe_ratio, 2),
                        network_signal        = signal,
                    ))
                # If provider served <3 groups the HAVING clause filtered them out → add stub
                if net_df.empty:
                    network_signals.append(NetworkSignal(
                        provider_id    = pid,
                        provider_name  = provider["provider_name"],
                        cpe_this_group = 0,
                        cpe_network    = 0,
                        groups_served  = 0,
                        cpe_ratio      = 0,
                        network_signal = "INSUFFICIENT_DATA",
                    ))
            except Exception as net_err:
                warnings.append(f"Network CPE benchmark failed: {net_err}")

        # ── 7. AI Medical Commentary ──────────────────────────────────────────
        ai_commentary = None
        if req.include_ai:
            ai_commentary = await get_ai_commentary(
                provider_name        = provider["provider_name"],
                band                 = band,
                state                = provider.get("state"),
                raw_metrics          = raw_dict,
                peer_benchmarks      = [b.model_dump() for b in peer_benchmarks],
                top_dx_repeats       = [d.model_dump() for d in top_dx],
                short_interval_pct   = short_pct,
                short_interval_buckets=[b.model_dump() for b in si_bucket_list],
                metric_scores        = [s.model_dump() for s in metric_scores],
                total_score          = total_score,
                alert_status         = alert_status,
                period               = {"start_date": start_str, "end_date": end_str},
            )

        # ── 8. Build response ─────────────────────────────────────────────────
        return FraudScoreResponse(
            provider        = ProviderInfo(**provider),
            period          = {"start_date": start_str, "end_date": end_str},
            raw_metrics     = raw_metrics,
            peer_benchmarks = peer_benchmarks,
            metric_scores   = metric_scores,
            behavioral      = behavioral,
            total_score     = total_score,
            max_score       = 10,
            alert_status    = alert_status,
            alert_emoji     = alert_emoji,
            ai_commentary   = ai_commentary,
            network_signals = network_signals,
            warnings        = warnings,
            computed_at     = datetime.utcnow().isoformat(),
        )

    finally:
        conn.close()