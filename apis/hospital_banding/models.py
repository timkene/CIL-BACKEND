"""
Pydantic models for Hospital Banding API.
"""

from pydantic import BaseModel, Field
from typing import Optional, Dict, List


class ProviderListItem(BaseModel):
    provider_id: str
    provider_name: str
    current_band: str


class QualityMetrics(BaseModel):
    available: bool
    quality_score: Optional[float] = None
    quality_tier: Optional[str] = None   # Excellent | Good | Fair | Poor
    readmission_count: Optional[int] = None
    patient_count: Optional[int] = None
    denial_rate: Optional[float] = None
    high_cost_outliers: Optional[int] = None
    reason: Optional[str] = None


class ProcedureOutlier(BaseModel):
    procedurecode: str
    price: float
    p95: float
    excess: float
    excess_pct: float


class FraudRisk(BaseModel):
    available: bool
    fraud_risk: Optional[str] = None   # MINIMAL | LOW | MEDIUM | HIGH
    above_p90_count: int = 0
    above_p90_pct: float = 0.0
    above_p95_count: int = 0
    above_p95_pct: float = 0.0
    extreme_outliers: int = 0           # procedures priced > 2× P95
    top_outliers: Optional[List[ProcedureOutlier]] = None


class StandardBandResult(BaseModel):
    """Band derived from the provider's official tariff in the database."""
    weighted_band: str
    weighted_avg: float
    weighted_method: str
    unweighted_band: str
    unweighted_avg: float
    unweighted_method: str
    matched_procedures: int
    total_standard_procedures: int
    coverage_pct: float
    weighted_band_distribution: Dict[str, float]
    unweighted_band_distribution: Dict[str, float]
    quality: QualityMetrics
    fraud: FraudRisk


class PricingBehavior(BaseModel):
    """How the provider's actual claim amounts compare to their official tariff."""
    behavior_flag: str           # SYSTEMATIC_OVERCHARGING | GENEROUS_DISCOUNTER | MODERATE_OVERCHARGING | MODERATE_DISCOUNTING | ALIGNED_WITH_TARIFF
    claims_based_procedures: int  # procedures with actual claims data
    tariff_based_procedures: int  # procedures that fell back to official tariff
    claims_coverage_pct: float    # % of reality tariff that comes from claims
    overcharging_count: int
    undercharging_count: int
    overcharge_pct: float
    undercharge_pct: float
    total_overcharge_amount: float
    total_undercharge_amount: float


class RealityBandResult(BaseModel):
    """Band derived from actual approved claim amounts (hybrid: claims + tariff fallback)."""
    weighted_band: str
    weighted_avg: float
    weighted_method: str
    unweighted_band: str
    unweighted_avg: float
    unweighted_method: str
    matched_procedures: int
    total_standard_procedures: int
    coverage_pct: float
    confidence: float             # 0–1 confidence score based on volume and coverage
    lookback_months: int
    weighted_band_distribution: Dict[str, float]
    unweighted_band_distribution: Dict[str, float]
    pricing_behavior: PricingBehavior
    fraud: FraudRisk


class BandComparison(BaseModel):
    current_db_band: str
    standard_weighted: str
    standard_unweighted: str
    reality_weighted: str
    reality_unweighted: str
    standard_vs_reality_agree: bool        # both weighted bands the same
    current_db_vs_reality: str            # MATCH | REALITY_HIGHER | REALITY_LOWER
    current_db_vs_standard: str           # MATCH | STANDARD_HIGHER | STANDARD_LOWER
    recommendation: str


class DualBandResponse(BaseModel):
    provider_id: str
    provider_name: str
    analysis_date: str
    standard_analysis: StandardBandResult
    reality_analysis: RealityBandResult
    comparison: BandComparison
