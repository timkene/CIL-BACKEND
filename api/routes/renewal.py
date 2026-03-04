"""
Renewal Analysis API Routes
============================

Comprehensive contract renewal analysis with AI-powered insights.
Replicates the Streamlit renewal analyzer functionality.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from complete_calculation_engine import CalculationEngine
from api.services.ai_engine import AIIntelligenceEngine

# Anthropic API key - loaded from environment variable
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY') or os.getenv('ANTHROPIC_KEY')

router = APIRouter()


class RenewalAnalysisRequest(BaseModel):
    """Request model for renewal analysis"""
    company_name: str
    debit_override: Optional[float] = None
    cash_override: Optional[float] = None
    use_motherduck: bool = False


class RenewalAnalysisResponse(BaseModel):
    """Response model for renewal analysis"""
    success: bool
    company_name: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str


@router.get("/companies")
async def get_active_companies(use_motherduck: bool = Query(False)):
    """
    Get list of active companies with current contracts

    Returns list of companies for the dropdown selector
    """
    try:
        engine = CalculationEngine(use_motherduck=use_motherduck)
        engine.connect()

        query = """
        SELECT DISTINCT groupname
        FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
        WHERE enddate >= CURRENT_DATE
            OR enddate IS NULL
        ORDER BY groupname
        """

        result = engine.conn.execute(query).fetchdf()
        companies = result['groupname'].tolist() if not result.empty else []

        return {
            "success": True,
            "companies": companies,
            "count": len(companies),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze", response_model=RenewalAnalysisResponse)
async def analyze_company(request: RenewalAnalysisRequest):
    """
    Run comprehensive renewal analysis for a company

    This endpoint runs the complete Python calculation engine
    and returns all metrics needed for the React frontend tabs.
    """
    try:
        # Initialize calculation engine
        engine = CalculationEngine(use_motherduck=request.use_motherduck)
        engine.connect()

        # Get contract details
        contract = engine.get_company_contract_details(request.company_name)

        if not contract['success']:
            raise HTTPException(status_code=404, detail=contract['error'])

        company_name = contract['company_name']
        groupid = contract['groupid']
        start_date = contract['startdate']
        end_date = contract['enddate']
        months_elapsed = contract['months_elapsed']

        # Get all analysis data
        financial = engine.get_financial_data(
            company_name,
            start_date,
            end_date,
            request.debit_override,
            request.cash_override
        )

        claims = engine.get_claims_data(groupid, start_date, end_date)
        pa = engine.get_pa_data(company_name, groupid, start_date, end_date)
        enrollment = engine.get_enrollment_data(groupid)

        # Calculate MLR metrics
        mlr = engine.calculate_mlr_metrics(financial, claims, pa, enrollment, months_elapsed)

        # Concentration analysis
        concentration = engine.analyze_member_concentration(groupid, start_date, end_date)

        # Condition breakdown
        conditions = engine.analyze_conditions(groupid, start_date, end_date)

        # Provider analysis
        providers = engine.analyze_providers(groupid, start_date, end_date)

        # Monthly PMPM trends
        monthly_pmpm = engine.calculate_monthly_pmpm_trend(
            groupid, start_date, end_date, enrollment
        )

        # Chronic disease burden
        chronic_disease = engine.analyze_chronic_disease_burden(
            groupid, start_date, end_date
        )

        # Claims trend decomposition
        trend_decomposition = engine.decompose_claims_trend(
            groupid, start_date, end_date, enrollment
        )

        # Provider bands
        provider_bands = engine.analyze_provider_bands(
            groupid, start_date, end_date
        )

        # Benefit limits analysis
        benefit_analysis = engine.analyze_company_benefit_limits(
            company_name, groupid, start_date, end_date
        )

        # Plan distribution analysis
        plan_analysis = engine.analyze_plan_distribution(
            company_name, groupid, start_date, end_date
        )

        # Calculate fraud indicators
        unknown_pct = providers.get('unknown_pct', 0)
        unknown_amount = providers.get('unknown_amount', 0)

        # Determine fraud risk level
        if unknown_pct > 30:
            risk_level = "HIGH RISK"
        elif unknown_pct > 10:
            risk_level = "MEDIUM RISK"
        else:
            risk_level = "LOW RISK"

        # Same-day claims analysis (fraud detection)
        same_day_query = """
        SELECT
            enrollee_id,
            encounterdatefrom,
            COUNT(*) as claims_same_day
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE CAST(nhisgroupid AS VARCHAR) = ?
            AND datesubmitted >= ?
            AND datesubmitted <= ?
        GROUP BY enrollee_id, encounterdatefrom
        HAVING COUNT(*) >= 5
        ORDER BY claims_same_day DESC
        """

        same_day_df = engine.conn.execute(
            same_day_query, [groupid, start_date, end_date]
        ).fetchdf()

        same_day_count = len(same_day_df)
        same_day_instances = same_day_df.to_dict('records') if not same_day_df.empty else []

        fraud = {
            'risk_level': risk_level,
            'unknown_pct': unknown_pct,
            'unknown_amount': unknown_amount,
            'same_day_count': same_day_count,
            'same_day_instances': same_day_instances
        }

        # Calculate comprehensive risk score
        risk_score = engine.calculate_renewal_risk_score(
            mlr, concentration, chronic_disease, financial, trend_decomposition
        )

        # Compile complete analysis data
        analysis_data = {
            'success': True,
            'company_name': company_name,
            'contract': contract,
            'financial': financial,
            'claims': claims,
            'pa': pa,
            'enrollment': enrollment,
            'mlr': mlr,
            'concentration': concentration,
            'conditions': conditions,
            'providers': providers,
            'fraud': fraud,
            'monthly_pmpm': monthly_pmpm,
            'chronic_disease': chronic_disease,
            'trend_decomposition': trend_decomposition,
            'provider_bands': provider_bands,
            'benefit_analysis': benefit_analysis,
            'plan_analysis': plan_analysis,
            'risk_score': risk_score
        }

        return RenewalAnalysisResponse(
            success=True,
            company_name=company_name,
            data=analysis_data,
            timestamp=datetime.now().isoformat()
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.post("/ai/summary")
async def generate_ai_summary(analysis_data: Dict[str, Any]):
    """
    Generate AI-powered executive summary

    Integrates with Claude API for brutally honest executive insights.
    """
    try:
        ai_engine = AIIntelligenceEngine(ANTHROPIC_API_KEY)
        summary = ai_engine.generate_executive_summary(analysis_data)
        cost_summary = ai_engine.get_cost_summary()

        return {
            "success": True,
            "summary": summary,
            "cost_naira": cost_summary['cost_per_analysis'],
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ai/predictions")
async def generate_ai_predictions(analysis_data: Dict[str, Any]):
    """
    Generate AI-powered MLR predictions
    """
    try:
        ai_engine = AIIntelligenceEngine(ANTHROPIC_API_KEY)
        predictions = ai_engine.predict_future_mlr(analysis_data)
        cost_summary = ai_engine.get_cost_summary()

        return {
            "success": True,
            "predictions": predictions,
            "cost_naira": cost_summary['cost_per_analysis'],
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ai/strategy")
async def generate_negotiation_strategy(analysis_data: Dict[str, Any]):
    """
    Generate AI-powered negotiation strategy
    """
    try:
        ai_engine = AIIntelligenceEngine(ANTHROPIC_API_KEY)
        strategy = ai_engine.generate_negotiation_strategy(analysis_data)
        cost_summary = ai_engine.get_cost_summary()

        return {
            "success": True,
            "strategy": strategy,
            "cost_naira": cost_summary['cost_per_analysis'],
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ai/anomalies")
async def detect_anomalies(analysis_data: Dict[str, Any]):
    """
    AI-powered anomaly detection
    """
    try:
        ai_engine = AIIntelligenceEngine(ANTHROPIC_API_KEY)
        anomalies = ai_engine.detect_anomalies(analysis_data)
        cost_summary = ai_engine.get_cost_summary()

        return {
            "success": True,
            "anomalies": anomalies,
            "cost_naira": cost_summary['cost_per_analysis'],
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
