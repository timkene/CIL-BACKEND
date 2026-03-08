"""
Fraud Scoring API - Pydantic Models
=====================================
Request / Response schemas.
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import date


# ── Request ───────────────────────────────────────────────────────────────────
class FraudScoreRequest(BaseModel):
    provider_id:   Optional[str]  = Field(None,  description="Provider ID (nhisproviderid)")
    provider_name: Optional[str]  = Field(None,  description="Provider name (partial match ok)")
    start_date:    date            = Field(...,   description="Period start date (YYYY-MM-DD)")
    end_date:      date            = Field(...,   description="Period end date   (YYYY-MM-DD)")
    include_ai:    bool            = Field(True,  description="Include AI medical commentary")
    group_id:      Optional[str]   = Field(None,  description="Group ID for network CPE benchmark")

    class Config:
        json_schema_extra = {
            "example": {
                "provider_name": "Faith City Hospital Isolo",
                "start_date":    "2025-11-01",
                "end_date":      "2026-02-28",
                "include_ai":    True
            }
        }


# ── Sub-models ────────────────────────────────────────────────────────────────
class ProviderInfo(BaseModel):
    provider_id:   str
    provider_name: str
    band:          str
    state:         Optional[str]

class RawMetrics(BaseModel):
    total_cost:        float
    unique_enrollees:  int
    total_visits:      int
    pa_visits:         int
    no_pa_visits:      int
    drug_cost:         float
    cpe:               float   # Cost Per Enrollee
    cpv:               float   # Cost Per Visit
    vpe:               float   # Visits Per Enrollee
    drug_ratio_pct:    float   # Drug Cost / Total Cost %

class PeerBenchmark(BaseModel):
    band:             str
    peer_count:       int
    metric:           str
    median:           float
    q1:               float
    q3:               float
    iqr:              float
    tukey_threshold:  float   # Q3 + 1.5 * IQR

class MetricScore(BaseModel):
    metric:        str
    value:         float
    threshold:     float
    breached:      bool
    score:         int
    max_score:     int
    detail:        str

class DxRepeat(BaseModel):
    diagnosis_code:  str
    diagnosis_desc:  str = ""
    repeat_count:    int

class ShortIntervalBucket(BaseModel):
    bucket:          str
    enrollee_count:  int
    mean_gap_days:   float

class BehavioralMetrics(BaseModel):
    dx_repeat_rate_pct:            float
    repeated_dx_pairs:             int
    total_dx_pairs:                int
    top_repeated_diagnoses:        List[DxRepeat]
    short_interval_enrollees:      int
    multi_visit_enrollees:         int
    short_interval_pct:            float
    avg_gap_short_interval:        Optional[float]
    short_interval_breakdown:      List[ShortIntervalBucket]

class NetworkSignal(BaseModel):
    provider_id:           str
    provider_name:         Optional[str]
    cpe_this_group:        float
    cpe_network:           float
    groups_served:         int
    encounters_this_group: int = 0
    cpe_ratio:             float
    network_signal:        str   # "GROUP_TARGETED" | "CLEAN" | "INSUFFICIENT_DATA"


class AICommentary(BaseModel):
    dx_repeat_assessment:   str
    cost_intensity_assessment: str
    short_interval_assessment: str
    overall_risk_narrative: str
    recommended_actions:    List[str]

# ── Main Response ─────────────────────────────────────────────────────────────
class FraudScoreResponse(BaseModel):
    provider:          ProviderInfo
    period:            Dict[str, str]
    raw_metrics:       RawMetrics
    peer_benchmarks:   List[PeerBenchmark]
    metric_scores:     List[MetricScore]
    behavioral:        BehavioralMetrics
    total_score:       int
    max_score:         int
    alert_status:      str          # "ALERT", "WATCHLIST", "CLEAR"
    alert_emoji:       str
    ai_commentary:     Optional[AICommentary]
    network_signals:   List[NetworkSignal] = []
    warnings:          List[str]    # data quality / low-peer-count warnings
    computed_at:       str

class ErrorResponse(BaseModel):
    error:   str
    detail:  Optional[str]