"""
Fraud Scoring API - Scoring Engine
=====================================
Applies all 6 metrics and produces the final fraud score.
Uses Tukey fence (Q3 + 1.5×IQR) for CPE/CPV/VPE thresholds.
"""

import pandas as pd
from typing import List, Tuple, Dict
from .models import MetricScore, PeerBenchmark
from .config import (
    SCORE_WEIGHTS, ALERT_THRESHOLD, WATCHLIST_THRESHOLD,
    DRUG_RATIO_HIGH, DRUG_RATIO_MEDIUM, DX_REPEAT_THRESHOLD,
    SHORT_INTERVAL_DAYS,
)


def _tukey(q1: float, q3: float) -> Tuple[float, float]:
    """Returns (IQR, Tukey upper fence = Q3 + 1.5*IQR)."""
    iqr = q3 - q1
    return iqr, round(q3 + 1.5 * iqr, 2)


def build_peer_benchmarks(bench_row: pd.Series, band: str) -> List[PeerBenchmark]:
    """Convert the benchmark query row into a list of PeerBenchmark objects."""
    peer_count = int(bench_row["peer_count"])
    benchmarks = []
    for metric in ["cpe", "cpv", "vpe"]:
        q1     = float(bench_row[f"{metric}_q1"])
        median = float(bench_row[f"{metric}_median"])
        q3     = float(bench_row[f"{metric}_q3"])
        iqr, threshold = _tukey(q1, q3)
        benchmarks.append(PeerBenchmark(
            band=band, peer_count=peer_count, metric=metric.upper(),
            median=round(median, 2), q1=round(q1, 2), q3=round(q3, 2),
            iqr=round(iqr, 2), tukey_threshold=threshold,
        ))
    return benchmarks


def score_metrics(
    raw: Dict,
    bench_row: pd.Series,
    dx_repeat_rate: float,
    short_interval_pct: float,
) -> Tuple[List[MetricScore], int, List[str]]:
    """
    Score all 6 metrics.  Returns (metric_scores, total_score, warnings).
    raw keys: cpe, cpv, vpe, drug_ratio_pct
    """
    scores: List[MetricScore] = []
    total  = 0
    warnings = []

    peer_count = int(bench_row["peer_count"]) if not bench_row.empty else 0
    if peer_count < 15:
        warnings.append(
            f"Only {peer_count} Band peers qualify for benchmarking "
            f"(≥{15} preferred). Thresholds may be less stable."
        )

    # ── helper ────────────────────────────────────────────────────────────────
    def _add(metric: str, value: float, threshold: float,
             weight: int, detail: str, breached: bool = None):
        nonlocal total
        if breached is None:
            breached = value >= threshold
        pts = weight if breached else 0
        total += pts
        scores.append(MetricScore(
            metric=metric, value=round(value, 2),
            threshold=round(threshold, 2), breached=breached,
            score=pts, max_score=weight, detail=detail,
        ))

    # ── 1. VPE ────────────────────────────────────────────────────────────────
    if not bench_row.empty:
        _, vpe_thresh = _tukey(bench_row["vpe_q1"], bench_row["vpe_q3"])
        _add("VPE", raw["vpe"], vpe_thresh,
             SCORE_WEIGHTS["vpe"],
             f"Visits per Enrollee vs Band peer Tukey fence (Q3+1.5×IQR)")
    else:
        warnings.append("No peer data available for VPE benchmark.")

    # ── 2. CPE ────────────────────────────────────────────────────────────────
    if not bench_row.empty:
        _, cpe_thresh = _tukey(bench_row["cpe_q1"], bench_row["cpe_q3"])
        _add("CPE", raw["cpe"], cpe_thresh,
             SCORE_WEIGHTS["cpe"],
             f"Cost per Enrollee vs Band peer Tukey fence")
    else:
        warnings.append("No peer data available for CPE benchmark.")

    # ── 3. CPV ────────────────────────────────────────────────────────────────
    if not bench_row.empty:
        _, cpv_thresh = _tukey(bench_row["cpv_q1"], bench_row["cpv_q3"])
        _add("CPV", raw["cpv"], cpv_thresh,
             SCORE_WEIGHTS["cpv"],
             f"Cost per Visit vs Band peer Tukey fence")
    else:
        warnings.append("No peer data available for CPV benchmark.")

    # ── 4. Drug Ratio ─────────────────────────────────────────────────────────
    dr = raw["drug_ratio_pct"]
    if dr > DRUG_RATIO_HIGH:
        dr_score, dr_thresh, dr_breach = SCORE_WEIGHTS["drug_ratio"], DRUG_RATIO_HIGH, True
    elif dr > DRUG_RATIO_MEDIUM:
        dr_score, dr_thresh, dr_breach = 1, DRUG_RATIO_MEDIUM, True
    else:
        dr_score, dr_thresh, dr_breach = 0, DRUG_RATIO_MEDIUM, False
    total += dr_score
    scores.append(MetricScore(
        metric="Drug Ratio", value=round(dr, 2),
        threshold=DRUG_RATIO_MEDIUM, breached=dr_breach,
        score=dr_score, max_score=SCORE_WEIGHTS["drug_ratio"],
        detail=f">55%→+2, >40%→+1. Peer median: {round(float(bench_row.get('drug_ratio_median', 0)), 1)}%"
               if not bench_row.empty else ">55%→+2, >40%→+1",
    ))

    # ── 5. Dx Repeat Rate ────────────────────────────────────────────────────
    _add("Dx Repeat Rate", dx_repeat_rate, DX_REPEAT_THRESHOLD,
         SCORE_WEIGHTS["dx_repeat"],
         f"% of enrollee-diagnosis pairs repeated within {SHORT_INTERVAL_DAYS}d "
         f"(different visit_id only)")

    # ── 6. Short Interval ─────────────────────────────────────────────────────
    _add("Short Interval", short_interval_pct, 50.0,
         SCORE_WEIGHTS["short_interval"],
         f"% of multi-visit enrollees with avg gap < {SHORT_INTERVAL_DAYS} days")

    return scores, total, warnings


def get_alert_status(score: int) -> Tuple[str, str]:
    """Returns (status_text, emoji)."""
    if score >= ALERT_THRESHOLD:
        return "ALERT",     "🚨"
    if score >= WATCHLIST_THRESHOLD:
        return "WATCHLIST", "⚠️"
    return "CLEAR", "✅"