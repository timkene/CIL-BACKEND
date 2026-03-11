"""
Banding engines for Hospital Banding API.

Combines logic from:
  - adv_hosp_band2.py       → EnhancedBandingEngine (standard band)
  - adv_hosp_claims_band.py → RealityBandingEngine (reality-adjusted band)

Two methodologies, both using the same REALITY TARIFF thresholds:
  Weighted   (TCOC):  log-frequency weighted average — reflects real cost burden
  Unweighted (Price): simple average — reflects pure unit pricing
"""

import math
import logging
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np

from .models import (
    StandardBandResult, RealityBandResult,
    QualityMetrics, FraudRisk, ProcedureOutlier,
    PricingBehavior, BandComparison,
)

logger = logging.getLogger(__name__)

# Band ordering: D cheapest → Special most expensive → check = out of range
BAND_ORDER: Dict[str, int] = {
    "ERROR": 0, "D": 1, "C": 2, "B": 3, "A": 4, "Special": 5, "check": 6
}


# ─── shared helpers ──────────────────────────────────────────────────────────

def _norm(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return str(v).strip().lower().replace(" ", "")


def _safe_float(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if not math.isnan(f) else default
    except Exception:
        return default


def _determine_band(price: float, std_proc: dict) -> str:
    """Place a single price into a band using per-procedure thresholds."""
    bd = _safe_float(std_proc.get("band_d"), 0)
    bc = _safe_float(std_proc.get("band_c"), 0)
    bb = _safe_float(std_proc.get("band_b"), 0)
    ba = _safe_float(std_proc.get("band_a"), 0)
    bs = _safe_float(std_proc.get("band_special"), 0)
    if price <= bd:         return "D"
    elif price <= bc:       return "C"
    elif price <= bb:       return "B"
    elif price <= ba:       return "A"
    elif price <= bs:       return "Special"
    else:                   return "check"


def _assign_band_from_avg(avg: float, thresholds: dict) -> str:
    """Map an average price to a band using global (mean) thresholds."""
    if avg <= thresholds["D"]:       return "D"
    elif avg <= thresholds["C"]:     return "C"
    elif avg <= thresholds["B"]:     return "B"
    elif avg <= thresholds["A"]:     return "A"
    elif avg <= thresholds["Special"]: return "Special"
    else:                            return "check"


# ─── core banding computation ────────────────────────────────────────────────

def _compute_bands(
    proc_prices: Dict[str, float],   # {normalized_code: price}
    std_dict: Dict[str, dict],
    thresholds: Dict[str, float],
) -> Tuple[dict, dict]:
    """
    Compute both weighted (TCOC) and unweighted (unit price) band results.
    Returns (weighted_result, unweighted_result), each a dict with:
        band, avg, method, band_pcts, matched
    """
    # Weighted accumulators
    w_price_sum = 0.0
    w_freq_sum  = 0.0
    w_band_freq: Dict[str, float] = {b: 0.0 for b in BAND_ORDER}

    # Unweighted accumulators
    u_price_sum = 0.0
    u_count     = 0
    u_band_cnt: Dict[str, int] = {b: 0 for b in BAND_ORDER}

    matched = 0

    for proc_code, price in proc_prices.items():
        if proc_code not in std_dict or price <= 0:
            continue

        matched += 1
        std = std_dict[proc_code]

        # Log-frequency weighting (from effective_frequency, which may come from claims count)
        freq     = _safe_float(std.get("effective_frequency", 1), 1.0)
        log_freq = math.log1p(freq)

        band = _determine_band(price, std)

        # Weighted
        w_price_sum         += price * log_freq
        w_freq_sum          += log_freq
        w_band_freq[band]   += log_freq

        # Unweighted
        u_price_sum += price
        u_count     += 1
        u_band_cnt[band] += 1

    # ── Weighted result ──
    if w_freq_sum > 0:
        w_avg  = w_price_sum / w_freq_sum
        w_calc = _assign_band_from_avg(w_avg, thresholds)
        w_tot  = sum(w_band_freq.values())
        w_pcts = {b: (f / w_tot * 100) if w_tot > 0 else 0.0
                  for b, f in w_band_freq.items()}
        valid  = {b: p for b, p in w_pcts.items() if b not in ("check", "ERROR")}
        dom    = max(valid, key=valid.get) if valid else w_calc
        dom_p  = valid.get(dom, 0)

        if w_pcts.get("check", 0) > 30:
            w_band, w_method = w_calc, "Gaming Protection"
        elif dom_p >= 60:
            w_band, w_method = dom, "Dominant Band (≥60%)"
        else:
            w_band, w_method = w_calc, "Weighted Average"
    else:
        w_avg, w_band, w_method, w_pcts = 0.0, "ERROR", "No data", {}

    # ── Unweighted result ──
    if u_count > 0:
        u_avg  = u_price_sum / u_count
        u_calc = _assign_band_from_avg(u_avg, thresholds)
        u_pcts = {b: (c / u_count * 100) for b, c in u_band_cnt.items()}
        valid  = {b: p for b, p in u_pcts.items() if b not in ("check", "ERROR")}
        dom    = max(valid, key=valid.get) if valid else u_calc
        dom_p  = valid.get(dom, 0)

        if u_pcts.get("check", 0) > 30:
            u_band, u_method = u_calc, "Gaming Protection"
        elif dom_p >= 60:
            u_band, u_method = dom, "Dominant Band"
        else:
            u_band, u_method = u_calc, "Simple Average"
    else:
        u_avg, u_band, u_method, u_pcts = 0.0, "ERROR", "No data", {}

    weighted   = dict(band=w_band, avg=w_avg, method=w_method,
                      band_pcts=w_pcts, matched=matched)
    unweighted = dict(band=u_band, avg=u_avg, method=u_method,
                      band_pcts=u_pcts, matched=matched)

    return weighted, unweighted


# ─── quality assessment ──────────────────────────────────────────────────────

def _assess_quality(
    quality_metrics: pd.DataFrame,
    provider_id: str,
) -> QualityMetrics:
    if quality_metrics is None or quality_metrics.empty:
        return QualityMetrics(available=False)

    match = quality_metrics[
        quality_metrics["nhisproviderid"].astype(str) == str(provider_id)
    ]
    if match.empty:
        return QualityMetrics(available=False, reason="Not found in quality data")

    row   = match.iloc[0]
    score = _safe_float(row.get("quality_score"), 0)

    if score >= 80:   tier = "Excellent"
    elif score >= 60: tier = "Good"
    elif score >= 40: tier = "Fair"
    else:             tier = "Poor"

    return QualityMetrics(
        available=True,
        quality_score=round(score, 1),
        quality_tier=tier,
        readmission_count=int(row.get("readmission_count", 0)),
        patient_count=int(row.get("patient_count", 0)),
        denial_rate=round(_safe_float(row.get("denial_rate"), 0), 1),
        high_cost_outliers=int(row.get("high_cost_outlier_count", 0)),
    )


# ─── fraud risk detection ────────────────────────────────────────────────────

def _assess_fraud(
    proc_prices: Dict[str, float],
    claims_stats: pd.DataFrame,
) -> FraudRisk:
    if claims_stats is None or claims_stats.empty:
        return FraudRisk(available=False)

    cs = {str(r["procedurecode"]): r for _, r in claims_stats.iterrows()}

    above_p90, above_p95, extreme = [], [], []
    total = len(proc_prices)

    for code, price in proc_prices.items():
        if code not in cs:
            continue
        row = cs[code]
        p90 = row.get("p90")
        p95 = row.get("p95")

        if p90 is not None and not math.isnan(float(p90)) and price > float(p90):
            above_p90.append(code)

        if p95 is not None and not math.isnan(float(p95)):
            p95f = float(p95)
            if price > p95f:
                above_p95.append({
                    "procedurecode": code,
                    "price":         round(price, 2),
                    "p95":           round(p95f, 2),
                    "excess":        round(price - p95f, 2),
                    "excess_pct":    round((price - p95f) / p95f * 100, 1) if p95f > 0 else 0.0,
                })
            if price > p95f * 2:
                extreme.append(code)

    p90_pct = len(above_p90) / total * 100 if total > 0 else 0.0
    p95_pct = len(above_p95) / total * 100 if total > 0 else 0.0

    if len(extreme) > 0 or p95_pct > 20:  risk = "HIGH"
    elif p95_pct > 10 or p90_pct > 40:    risk = "MEDIUM"
    elif p90_pct > 20:                     risk = "LOW"
    else:                                  risk = "MINIMAL"

    top = sorted(above_p95, key=lambda x: x["excess"], reverse=True)[:10]

    return FraudRisk(
        available=True,
        fraud_risk=risk,
        above_p90_count=len(above_p90),
        above_p90_pct=round(p90_pct, 1),
        above_p95_count=len(above_p95),
        above_p95_pct=round(p95_pct, 1),
        extreme_outliers=len(extreme),
        top_outliers=[ProcedureOutlier(**o) for o in top],
    )


# ─── public analysis functions ───────────────────────────────────────────────

def analyze_standard(
    official_tariff_df: pd.DataFrame,
    std_dict: Dict[str, dict],
    thresholds: Dict[str, float],
    total_std_procs: int,
    claims_stats: pd.DataFrame,
    quality_metrics: pd.DataFrame,
    provider_id: str,
) -> StandardBandResult:
    """
    Standard band analysis: provider's published tariff vs band thresholds.
    Reflects what the provider officially charges.
    """
    proc_prices = {
        _norm(row["procedurecode"]): _safe_float(row["tariffamount"])
        for _, row in official_tariff_df.iterrows()
        if _safe_float(row.get("tariffamount")) > 0
    }

    weighted, unweighted = _compute_bands(proc_prices, std_dict, thresholds)
    matched      = weighted["matched"]
    coverage_pct = (matched / total_std_procs * 100) if total_std_procs > 0 else 0.0

    return StandardBandResult(
        weighted_band=weighted["band"],
        weighted_avg=round(weighted["avg"], 2),
        weighted_method=weighted["method"],
        unweighted_band=unweighted["band"],
        unweighted_avg=round(unweighted["avg"], 2),
        unweighted_method=unweighted["method"],
        matched_procedures=matched,
        total_standard_procedures=total_std_procs,
        coverage_pct=round(coverage_pct, 1),
        weighted_band_distribution={k: round(v, 1) for k, v in weighted["band_pcts"].items()},
        unweighted_band_distribution={k: round(v, 1) for k, v in unweighted["band_pcts"].items()},
        quality=_assess_quality(quality_metrics, provider_id),
        fraud=_assess_fraud(proc_prices, claims_stats),
    )


def analyze_reality(
    reality_df: pd.DataFrame,
    std_dict: Dict[str, dict],
    thresholds: Dict[str, float],
    total_std_procs: int,
    claims_stats: pd.DataFrame,
    lookback_months: int,
) -> RealityBandResult:
    """
    Reality band analysis: actual approved claim amounts vs band thresholds.
    Reveals the gap between published tariff and real billing behaviour.
    """
    proc_prices = {
        _norm(row["procedurecode"]): _safe_float(row["reality_price"])
        for _, row in reality_df.iterrows()
        if _safe_float(row.get("reality_price")) > 0
    }

    weighted, unweighted = _compute_bands(proc_prices, std_dict, thresholds)
    matched      = weighted["matched"]
    coverage_pct = (matched / total_std_procs * 100) if total_std_procs > 0 else 0.0

    # ── Pricing behaviour ──
    claims_rows      = reality_df[reality_df["price_source"] == "CLAIMS"]
    tariff_rows      = reality_df[reality_df["price_source"] == "TARIFF"]
    claims_cnt       = len(claims_rows)
    overcharging     = claims_rows[claims_rows["price_difference"].notna() & (claims_rows["price_difference"] > 0)]
    undercharging    = claims_rows[claims_rows["price_difference"].notna() & (claims_rows["price_difference"] < 0)]
    overcharge_pct   = len(overcharging)  / claims_cnt * 100 if claims_cnt > 0 else 0.0
    undercharge_pct  = len(undercharging) / claims_cnt * 100 if claims_cnt > 0 else 0.0

    if overcharge_pct > 50:    flag = "SYSTEMATIC_OVERCHARGING"
    elif undercharge_pct > 50: flag = "GENEROUS_DISCOUNTER"
    elif overcharge_pct > 25:  flag = "MODERATE_OVERCHARGING"
    elif undercharge_pct > 25: flag = "MODERATE_DISCOUNTING"
    else:                      flag = "ALIGNED_WITH_TARIFF"

    total_over  = float(overcharging["price_difference"].sum())  if len(overcharging)  > 0 else 0.0
    total_under = float(undercharging["price_difference"].abs().sum()) if len(undercharging) > 0 else 0.0

    # ── Confidence score ──
    w_freq_total = sum(
        math.log1p(_safe_float(std_dict[c].get("effective_frequency", 1), 1.0))
        for c in proc_prices if c in std_dict
    )
    vol_conf  = min(1.0, math.log1p(w_freq_total) / math.log1p(10_000))
    cov_conf  = min(1.0, matched / total_std_procs) if total_std_procs > 0 else 0.0
    confidence = round(0.60 * vol_conf + 0.40 * cov_conf, 2)

    return RealityBandResult(
        weighted_band=weighted["band"],
        weighted_avg=round(weighted["avg"], 2),
        weighted_method=weighted["method"],
        unweighted_band=unweighted["band"],
        unweighted_avg=round(unweighted["avg"], 2),
        unweighted_method=unweighted["method"],
        matched_procedures=matched,
        total_standard_procedures=total_std_procs,
        coverage_pct=round(coverage_pct, 1),
        confidence=confidence,
        lookback_months=lookback_months,
        weighted_band_distribution={k: round(v, 1) for k, v in weighted["band_pcts"].items()},
        unweighted_band_distribution={k: round(v, 1) for k, v in unweighted["band_pcts"].items()},
        pricing_behavior=PricingBehavior(
            behavior_flag=flag,
            claims_based_procedures=claims_cnt,
            tariff_based_procedures=len(tariff_rows),
            claims_coverage_pct=round(
                claims_cnt / len(reality_df) * 100 if len(reality_df) > 0 else 0.0, 1
            ),
            overcharging_count=len(overcharging),
            undercharging_count=len(undercharging),
            overcharge_pct=round(overcharge_pct, 1),
            undercharge_pct=round(undercharge_pct, 1),
            total_overcharge_amount=round(total_over, 2),
            total_undercharge_amount=round(total_under, 2),
        ),
        fraud=_assess_fraud(proc_prices, claims_stats),
    )


# ─── comparison ──────────────────────────────────────────────────────────────

def build_comparison(
    current_db_band: str,
    standard: StandardBandResult,
    reality: RealityBandResult,
) -> BandComparison:
    sw = standard.weighted_band
    rw = reality.weighted_band

    sw_ord  = BAND_ORDER.get(sw, 0)
    rw_ord  = BAND_ORDER.get(rw, 0)
    db_ord  = BAND_ORDER.get(current_db_band, 0)

    if rw_ord > sw_ord:     std_vs_reality = "REALITY_HIGHER"
    elif rw_ord < sw_ord:   std_vs_reality = "REALITY_LOWER"
    else:                   std_vs_reality = "MATCH"

    if db_ord < rw_ord:     db_vs_reality = "REALITY_HIGHER"
    elif db_ord > rw_ord:   db_vs_reality = "REALITY_LOWER"
    else:                   db_vs_reality = "MATCH"

    if db_ord < sw_ord:     db_vs_standard = "STANDARD_HIGHER"
    elif db_ord > sw_ord:   db_vs_standard = "STANDARD_LOWER"
    else:                   db_vs_standard = "MATCH"

    bhv = reality.pricing_behavior.behavior_flag

    if db_vs_reality == "REALITY_HIGHER" and bhv in (
        "SYSTEMATIC_OVERCHARGING", "MODERATE_OVERCHARGING"
    ):
        rec = (
            f"Band upgrade to {rw} recommended. Provider's actual claims ({rw}) exceed "
            f"their official tariff classification ({current_db_band}). "
            f"Review overcharging ({reality.pricing_behavior.overcharging_count} procedures, "
            f"₦{reality.pricing_behavior.total_overcharge_amount:,.0f} excess) before contract renewal."
        )
    elif db_vs_reality == "MATCH" and bhv == "GENEROUS_DISCOUNTER":
        rec = (
            f"Band {current_db_band} confirmed. Provider is a generous discounter — "
            f"₦{reality.pricing_behavior.total_undercharge_amount:,.0f} below official tariff across "
            f"{reality.pricing_behavior.undercharging_count} procedures. "
            "Consider preferred-provider designation."
        )
    elif db_vs_reality == "REALITY_LOWER":
        rec = (
            f"Band downgrade to {rw} may be warranted. Actual claim amounts are lower "
            f"than official classification ({current_db_band}). Verify before adjusting."
        )
    elif std_vs_reality == "REALITY_HIGHER":
        rec = (
            f"Standard band {sw} but reality band {rw}. Provider bills higher than their "
            "published tariff — investigate systematic overcharging."
        )
    elif sw == rw:
        rec = (
            f"Both methodologies agree on Band {sw}. "
            f"Current DB band ({current_db_band}) is "
            f"{'confirmed' if current_db_band == sw else f'potentially mis-classified — standard analysis suggests {sw}'}."
        )
    else:
        rec = (
            f"Standard band {sw} vs reality band {rw}. "
            "Divergence may indicate tariff gaming — review procedure-level pricing."
        )

    return BandComparison(
        current_db_band=current_db_band,
        standard_weighted=sw,
        standard_unweighted=standard.unweighted_band,
        reality_weighted=rw,
        reality_unweighted=reality.unweighted_band,
        standard_vs_reality_agree=(sw == rw),
        current_db_vs_reality=db_vs_reality,
        current_db_vs_standard=db_vs_standard,
        recommendation=rec,
    )
