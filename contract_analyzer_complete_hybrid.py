#!/usr/bin/env python3
"""
🤖 CLEARLINE CONTRACT RENEWAL ANALYZER - COMPLETE HYBRID SYSTEM
================================================================

Production-ready AI-powered healthcare insurance contract analysis.

✅ COMPLETE Business Context Injection
✅ Hospital Banding Integration  
✅ Your Exact Style (Emojis, Tables, Brutal Truth)
✅ All Metrics from Godrej/Alert/NAHCO
✅ Cost: ~₦60 per comprehensive analysis

Author: Casey @ Clearline HMO
Version: 3.0-PRODUCTION-COMPLETE
Date: December 2025
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date, timedelta
from pathlib import Path
import io
import json
import os
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')
# ADD THESE LINES:
from complete_calculation_engine import run_comprehensive_analysis
from hospital_banding_integration import run_banding_analysis_for_company
# Also import all the tab rendering functions:
from complete_ui_tabs import (
    render_ai_executive_summary,
    render_ai_predictions,
    render_financial_analysis,
    render_utilization_analysis,
    render_conditions_breakdown,
    render_provider_analysis,
    render_plan_analysis,
    render_anomaly_detection,
    render_negotiation_strategy,
    render_risk_dashboard,
    render_benefit_limits_tab
)

# Claude API
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# Hospital Banding (from your project)
try:
    import sys
    sys.path.append('/Users/kenechukwuchukwuka/Downloads/DLT')
    from hospital_banding import HospitalBandingEngine, DuckDBClaimsLoader
    BANDING_AVAILABLE = True
except ImportError:
    BANDING_AVAILABLE = False

# Page Configuration
st.set_page_config(
    page_title="🤖 Clearline AI Analyzer",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS - Your Style
CUSTOM_CSS = """
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .brutal-truth-box {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        margin: 2rem 0;
        border-left: 8px solid #c92a2a;
        box-shadow: 0 8px 16px rgba(0,0,0,0.3);
    }
    
    .brutal-truth-box h2 {
        color: white;
        font-size: 2rem;
        margin-bottom: 1rem;
    }
    
    .ai-insight-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        margin: 1.5rem 0;
        box-shadow: 0 8px 16px rgba(0,0,0,0.2);
    }
    
    .prediction-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        margin: 1.5rem 0;
    }
    
    .critical-flag {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
        border-left: 6px solid #c92a2a;
    }
    
    .warning-flag {
        background: linear-gradient(135deg, #ffa500 0%, #ff8c00 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
        border-left: 6px solid #e67700;
    }
    
    .good-flag {
        background: linear-gradient(135deg, #51cf66 0%, #37b24d 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
        border-left: 6px solid #2b8a3e;
    }
    
    .negotiation-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        margin: 1.5rem 0;
    }
    
    .action-timeline {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 6px solid #667eea;
        margin: 1rem 0;
    }
    
    .banding-alert {
        background: linear-gradient(135deg, #ffa500 0%, #ff8c00 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 1rem 0;
        border-left: 6px solid #e67700;
    }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ============================================================================
# BUSINESS CONTEXT MANAGER - THE KEY TO AI UNDERSTANDING
# ============================================================================

class ClearlineBusinessContext:
    """Injects complete Clearline business knowledge into every AI prompt"""
    
    COMPLETE_CONTEXT = """
**YOU ARE ANALYZING FOR CLEARLINE INTERNATIONAL LIMITED**

Clearline is an HMO (Health Maintenance Organization) in Lagos, Nigeria, providing healthcare insurance to corporate clients.

---

## 🎯 CRITICAL BUSINESS RULES

### MLR CALCULATION METHODOLOGY:
```
MLR = (Medical Cost + Commission) / Debit Note × 100

Where:
- Medical Cost = Claims Paid + Unclaimed PA
- Claims Paid = Sum of approved claims during contract period
- Unclaimed PA = PA issued during contract that haven't been claimed YET
- Commission = Debit Note × Commission Rate
- Debit Note = Total invoiced amount to client
```

**CRITICAL:** Unclaimed PA is a FUTURE LIABILITY. It MUST be included in MLR calculation even though it hasn't been paid yet.

### COMMISSION RATES (MEMORIZE THESE):
- **CARAWAY AFRICA NIGERIA LIMITED: 12%**
- **POLARIS BANK PLC: 15%**
- **LORNA NIGERIA LIMITED (GODREJ): 20%**
- **ALL OTHERS: 10% (default)**

### TARGET METRICS:
- **Target MLR: <75%** (allows 15% admin + 10% commission, or 10% admin + 15% commission)
- **Target PMPM: ₦3,000** (Per Member Per Month medical cost)
- **Acceptable MLR Range: 65-75%**
  - Below 65%: Overpricing (client will complain)
  - 65-75%: Healthy profitability  
  - 75-85%: Monitoring required
  - 85-100%: Loss-making, action required
  - Above 100%: Severe losses, immediate action

---

## 📊 CONCENTRATION PATTERN ANALYSIS

### EPISODIC PATTERN (Fixable, One-off Events):
- **Definition:** Top 5 members account for >40% of total costs
- **OR:** One-off events (maternity, surgery, trauma) >40% of costs
- **Characteristics:**
  - Few individuals driving costs
  - Likely one-time expensive events
  - Won't repeat next year
- **Strategy:** Premium can stay same or +5-10% increase
- **Example:** Alert Microfinance (Top 5 = 24%, 52% maternity)

### STRUCTURAL PATTERN (Systemic, Harder to Fix):
- **Definition:** Costs spread across many members
- **OR:** Chronic conditions >30% of costs
- **Characteristics:**
  - Widespread utilization
  - Ongoing chronic diseases
  - Will continue and worsen
- **Strategy:** Premium increase 15-20%+ required
- **Example:** Godrej Lorna (Top 5 = 19%, 30% chronic, 37% preventable)

---

## 🚨 FRAUD INDICATORS (From Nigerian Market Experience)

### Unknown Provider Risk Levels:
- **<10%: LOW RISK** - Normal record-keeping errors
- **10-30%: MEDIUM RISK** - Poor systems or minor gaming
- **>30%: HIGH RISK** - Systematic fraud or money laundering

### Medical Impossibilities:
- **Same-day 5+ claims:** No patient can have 5+ procedures same day
- **>100 claims/year per patient:** Human maximum is ~50 legitimate visits
- **Weekly recurring acute conditions:** Acute means one-time, not chronic

### Gaming Patterns:
- Large claims at contract end (trying to maximize usage before renewal)
- Clusters of expensive claims for same family
- Providers billing at exactly tariff maximums

---

## 🏥 NIGERIAN HEALTHCARE CONTEXT

### Preventable Disease Economics:
- **Malaria:** ₦35,000 treatment vs ₦2,000 prevention (nets + prophylaxis)
  - ROI: 97% return on prevention
  - Payback period: 4 months
- **URIs (Upper Respiratory Infections):** Often poor hygiene/environment
- **UTIs:** Usually poor hygiene/water quality

### Normal Disease Distributions:
- **Maternity: 15-20%** (>25% indicates adverse selection)
- **Chronic: <20%** (>30% indicates aging/high-risk population)
- **Preventable: <25%** (>40% indicates poor health management)
- **Catastrophic (Cancer/Stroke): <5%**

### Chronic Disease Management:
- **Hypertension/Diabetes:** Lifetime costs, require ongoing management
- **Without intervention:** Costs increase 15-20% annually
- **With chronic disease management:** Can reduce costs 20-30%

---

## 💰 PAYMENT CONTEXT (Critical in Nigeria)

### Payment Patterns:
- Nigerian companies often delay payment 3-6 months
- Outstanding >₦10M for >6 months = red flag
- **Zero payment = Immediate termination** regardless of MLR

### Payment Rate Thresholds:
- **>80%: GOOD** - Reliable payer
- **50-80%: MODERATE** - Some delays but paying
- **<50%: BAD** - Chronic non-payer
- **0%: TERMINATE** - No leverage for utilization management

---

## 📚 PAST ANALYSES (Reference These)

### Case 1: LORNA NIGERIA LIMITED (GODREJ) - December 2025
**Situation:**
- BV-MLR: 91%, Projected: 170%
- PMPM: ₦4,383 (46% over target)
- Concentration: STRUCTURAL (Top 5 = 19%)
- Conditions: 30% chronic, 37% preventable, 30% maternity
- Commission: 20% (highest tier)
- Payment: ₦0 received, ₦46M outstanding for 8+ months

**Decision: TERMINATE**

**Reasoning:**
1. Zero payment = no leverage
2. Structural pattern = systemic issues
3. 37% preventable = poor health management
4. 30% maternity = adverse selection
5. Even at +85% premium, still losing money

**Key Lesson:** No payment + structural overutilization = Walk away

---

### Case 2: ALERT MICROFINANCE BANK - December 2025
**Situation:**
- BV-MLR: 94%, Projected: 105-110%
- PMPM: ₦5,343 (78% over target)
- Concentration: EPISODIC (Top 5 = 24%)
- Conditions: 52% maternity, 21% unknown provider
- Commission: 10%
- Payment: GOOD (₦12.7M surplus)

**Decision: RENEW +30% with conditions**

**Reasoning:**
1. Episodic pattern = fixable
2. Good payment = leverage for controls
3. High maternity = one-time spike
4. With premium increase + controls = salvageable

**Key Lesson:** Episodic + good payment = Salvageable with adjustments

---

### Case 3: NAHCO - December 2025
**Situation:**
- MLR: 140% (using custom figure ₦49,630,804.57)
- Unknown provider: 32% (HIGH RISK)
- Fraud: 38 same-day multiple claims detected
- Payment: GOOD

**Decision: TERMINATE or +50% minimum**

**Reasoning:**
1. Systematic fraud despite good payment
2. Medical impossibilities (same-day multiple claims)
3. Unknown provider >30% = fraud
4. Even good payers aren't worth fraud risk

**Key Lesson:** Fraud overrides payment compliance

---

## 🎯 DECISION FRAMEWORK (Apply This Logic)

### Step 1: Check Payment
```
IF payment_rate = 0%:
    RETURN "TERMINATE - No leverage without payment"
```

### Step 2: Check Fraud
```
IF unknown_provider > 30% OR same_day_claims > 5:
    RETURN "TERMINATE or +50% with forensic audit"
```

### Step 3: Check MLR + Pattern
```
IF mlr < 75%:
    IF episodic: "KEEP PREMIUM"
    ELSE: "KEEP PREMIUM - Consider 5% reduction"

ELIF mlr >= 75% AND mlr < 85%:
    IF episodic: "INCREASE 5-10%"
    ELSE: "INCREASE 10-15%"

ELIF mlr >= 85% AND mlr < 100%:
    IF episodic AND chronic < 20%: "INCREASE 10-15%"
    ELSE: "INCREASE 15-20%"

ELIF mlr >= 100%:
    IF chronic > 40%: "INCREASE 25%+ or TERMINATE"
    ELSE: "INCREASE 20-25%"
```

---

## 📋 DATABASE STRUCTURE (For Your Reference)

### Key Tables:
- **CLAIMS DATA:** 
  - `datesubmitted` = When claim was PAID (use for financial calculations)
  - `encounterdatefrom` = When service occurred (use for fraud detection)
  - `nhisgroupid` = Group ID (join key)
  - `nhisproviderid` = Provider ID
  - `approvedamount` = Actual paid amount

- **PA DATA:**
  - `requestdate` = When PA was issued
  - `panumber` = PA number (join key with claims)
  - `granted` = Approved PA amount
  - `groupname` = Company name

- **DEBIT_NOTE:**
  - `CompanyName` = Company name (must match PA DATA.groupname)
  - `Amount` = Invoice amount
  - `From` = Invoice date
  - Filter out rows with "TPA" in Description (admin fees)

- **GROUPS:**
  - `groupid` = Group ID (join key)
  - `groupname` = Company name

### Unclaimed PA Logic:
```sql
-- Get all PA numbers issued during contract
pa_issued = SELECT panumber FROM PA_DATA 
WHERE requestdate BETWEEN contract_start AND contract_end

-- Get all PA numbers that were claimed
pa_claimed = SELECT panumber FROM CLAIMS_DATA
WHERE panumber IN pa_issued

-- Unclaimed PA = PA issued but not yet in claims
unclaimed_pa = pa_issued MINUS pa_claimed
```

---

## 🎨 RESPONSE FORMATTING REQUIREMENTS

### Must Use Emoji Coding System:
- 🔴 **Critical** - Urgent action required (MLR >100%, zero payment, fraud)
- 🟡 **Warning** - Monitoring required (MLR 75-85%, payment delays)
- ✅ **Good** - Healthy metrics (MLR <75%, good payment)
- 🚨 **Urgent** - Immediate escalation needed
- ❌ **Bad** - Failed metrics
- ⚠️ **Alert** - Attention needed

### Required Sections:
1. **THE BRUTAL TRUTH** - Direct, no-sugar-coating summary
2. **Key Metrics Table** - Structured comparison to benchmarks
3. **Concentration Analysis** - Episodic vs Structural determination
4. **Fraud Indicators** - Specific flags with evidence
5. **Premium Recommendation** - Multiple options with % increases
6. **Walk-Away Criteria** - When to terminate
7. **Action Timeline** - Week 1, Week 2-4, Month 2-5

### Table Format Example:
```markdown
| Metric | Value | Target | Variance | Status |
|--------|-------|--------|----------|--------|
| MLR | 91.2% | <75% | +16.2% | 🔴 Critical |
| PMPM | ₦4,383 | ₦3,000 | +46.1% | 🔴 Critical |
```

---

## 💡 KEY PRINCIPLES TO REMEMBER

1. **Payment Trumps Everything:** No payment = no deal, regardless of MLR
2. **Fraud Trumps Payment:** Systematic fraud = walk away even with good payment
3. **Episodic ≠ Structural:** Don't punish episodic spikes with massive increases
4. **Chronic = Long-term:** Chronic conditions require premium increases
5. **Preventable = Opportunity:** High preventable costs = negotiation leverage
6. **Nigerian Context Matters:** Payment delays are normal, factor this in
7. **Be Brutally Honest:** Executives need truth, not sugar-coating

---

## 🆕 ADVANCED METRICS FOR RENEWAL DECISIONS

### MONTHLY PMPM TRENDING (Gaming Prevention):
- **Gaming Pattern:** Premium spikes in final 2 months >25% = gaming risk
- **Healthy Pattern:** Stable PMPM ±10% month-to-month
- **Action:** If gaming detected, adjust renewal price based on first 10 months only

### HCC PERSISTENCE ANALYSIS:
- **Episodic HCCs:** One-time events (surgery, childbirth) - won't repeat
- **Persistent HCCs:** Chronic conditions (diabetes, heart disease) - will continue
- **Target:** <30% of population with chronic conditions
- **Red Flag:** >40% with chronic = structural problem requiring 20%+ premium increase

### CLAIMS TREND DECOMPOSITION:
- **Unit Cost Trend:** Provider pricing changes (Target: <3% annual)
- **Utilization Trend:** Service usage changes (Target: <5% annual)
- **Total Trend:** Compound effect - if both increasing, major concern
- **Action:** Different solutions for each:
  - Unit cost increase → Provider negotiation
  - Utilization increase → Care management programs

### CASH COLLECTION METRICS:
- **Target Collection Rate:** >90% within 90 days
- **Red Flag:** <70% collection = high default risk
- **Action:** Poor collection + high MLR = TERMINATE or require prepayment
- **Aging Buckets:**
  - Current (<30 days): Normal operations
  - 30-60 days: Monitor closely
  - 60-90 days: Escalate to management
  - 90+ days: Collection risk, consider legal action

### PA EFFECTIVENESS:
- **Good PA System:** >70% conversion rate, approval rate 80-95%, <10% denial rate
- **Poor PA System:** <50% conversion = rubber stamping (ineffective gatekeeping)
- **Cost Variance:** PA should be within ±10% of actual claims
- **Red Flags:**
  - >95% approval rate = not enforcing controls
  - <50% conversion rate = approving unnecessary procedures

### PLAN-LEVEL ANALYSIS (Enrollment vs Utilization):
- **Enrollment Calculation:** Total lives covered = Individual members + (Family count × (Dependants + 1))
- **Utilization Variance:** Claims % - Enrollment % shows plan efficiency
- **Over-Utilizing Plans (Claims % > Enrollment % by >20%):**
  - **RED FLAG:** Plan attracting adverse selection or mispriced
  - **Example:** 15% enrollment but 35% claims = +20% variance
  - **Action:** Increase premium 15-25% for this plan, implement stricter PA, review benefits
- **Under-Utilizing Plans (Claims % < Enrollment % by >20%):**
  - **POSITIVE:** Plan is profitable, potentially healthier population
  - **Example:** 30% enrollment but 15% claims = -15% variance
  - **Action:** Maintain pricing, consider slight reduction to capture market share
- **Balanced Plans (±20% variance):**
  - **IDEAL:** Enrollment matches utilization, pricing is appropriate
  - **Action:** Continue current strategy with inflation adjustments
- **Portfolio-Level Insights:**
  - If >50% plans are over-utilizing: Systematic mispricing, need portfolio-wide adjustment
  - If >50% plans are under-utilizing: Strong position, opportunity for growth
  - Monitor cost per member across plans to identify outliers

### COMPREHENSIVE RISK SCORING:
Risk Score (0-100) determines renewal action:
- **0-30 (LOW):** Renew at current terms (0-5% increase)
  - Good payment, stable MLR <75%, episodic pattern
  - Recommended: Maintain relationship, minor adjustments

- **31-60 (MEDIUM):** Renew with adjustments (5-15% increase)
  - Moderate MLR 75-85%, some chronic disease burden
  - Recommended: Implement care management, increase premium

- **61-80 (HIGH):** Major changes needed (15-25% increase + care management)
  - High MLR >85%, structural pattern, growing chronic burden
  - Recommended: Significant restructuring, enhanced controls

- **81-100 (EXTREME):** Consider non-renewal or complete restructuring (>25% increase)
  - MLR >100%, poor payment, high fraud risk, persistent chronic conditions
  - Recommended: Exit relationship or dramatic intervention

**Risk Score Components (Weighted):**
1. MLR Risk (30% weight): Based on current and projected MLR
2. HCC Persistence (25% weight): Episodic vs chronic disease patterns
3. Chronic Disease Burden (20% weight): % of population with chronic conditions
4. Cash Collection Risk (15% weight): Payment behavior and AR aging
5. Claims Trend (10% weight): Year-over-year cost trajectory

---

**NOW ANALYZE THE COMPANY WITH THIS COMPLETE CONTEXT IN MIND, INCLUDING ALL ADVANCED METRICS.**
"""

    @staticmethod
    def get_context() -> str:
        """Get complete business context for AI prompts"""
        return ClearlineBusinessContext.COMPLETE_CONTEXT
    
    @staticmethod
    def enrich_prompt(base_prompt: str) -> str:
        """Inject business context into any prompt"""
        return f"{ClearlineBusinessContext.COMPLETE_CONTEXT}\n\n---\n\n{base_prompt}"


# ============================================================================
# AI INTELLIGENCE ENGINE - WITH BUSINESS CONTEXT INJECTION
# ============================================================================

class AIIntelligenceEngine:
    """Claude API with complete Clearline business context"""
    
    def __init__(self, api_key: str):
        if not ANTHROPIC_AVAILABLE:
            raise ImportError("pip install anthropic")
        
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-sonnet-4-20250514"
        self.token_count = 0
        self.cost_naira = 0.0
    
    def _calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost in Naira"""
        # Anthropic pricing: $3 per 1M input, $15 per 1M output
        # USD to NGN: ~₦1,500 per dollar (approximate)
        input_cost_usd = (input_tokens / 1_000_000) * 3
        output_cost_usd = (output_tokens / 1_000_000) * 15
        total_usd = input_cost_usd + output_cost_usd
        total_ngn = total_usd * 1500
        return total_ngn
    
    def generate_executive_summary(self, data: Dict) -> str:
        """🤖 THE BRUTAL TRUTH - AI Executive Summary"""
        
        prompt = f"""Generate an executive summary for {data['company_name']}.

**CONTRACT PERFORMANCE:**
- Contract Period: {data['contract']['months_elapsed']} of {data['contract']['total_months']} months ({data['contract']['months_remaining']} remaining)
- BV-MLR: {data['mlr']['bv_mlr']:.1f}%
- CASH-MLR: {data['mlr'].get('cash_mlr', 'N/A')}
- PMPM: ₦{data['mlr']['pmpm']:,.0f}
- PMPM Variance: {data['mlr']['pmpm_variance']:+.1f}%
- Monthly Burn: ₦{data['mlr']['monthly_burn']:,.0f}

**FINANCIAL STATUS:**
- Total Debit: ₦{data['financial']['debit']:,.0f}
- Total Cash: ₦{data['financial']['cash']:,.0f}
- Payment Rate: {data['financial']['payment_rate']:.1f}%
- Outstanding: ₦{data['financial']['outstanding']:,.0f}
- Commission Rate: {data['mlr']['commission_rate']:.0f}%

**UTILIZATION:**
- Pattern: {data['concentration']['type']}
- Top 5 Members: {data['concentration']['top_5_pct']:.1f}% of costs
- Top 10 Members: {data['concentration']['top_10_pct']:.1f}% of costs
- Enrolled: {data['enrollment']['total']}
- Claimed: {data['claims']['unique_claimed']}
- Utilization: {(data['claims']['unique_claimed'] / data['enrollment']['total'] * 100) if data['enrollment']['total'] > 0 else 0:.1f}%

**CONDITIONS:**
- One-off (Maternity/Surgery/Trauma): {data['conditions']['one_off_pct']:.1f}%
- Chronic (HTN/Diabetes/Respiratory/Kidney): {data['conditions']['chronic_pct']:.1f}%
- Preventable (Malaria/URI/UTI): {data['conditions']['preventable_pct']:.1f}%
- Catastrophic (Cancer/Stroke): {data['conditions']['catastrophic_pct']:.1f}%

**FRAUD INDICATORS:**
- Risk Level: {data['fraud']['risk_level']}
- Unknown Provider: {data['fraud']['unknown_pct']:.1f}% (₦{data['fraud']['unknown_amount']:,.0f})
- Same-Day Multiple Claims: {data['fraud']['same_day_count']} instances

**MONTHLY PMPM TRENDS:**
{json.dumps(data.get('monthly_pmpm', {}), indent=2)}
- Analyze for gaming patterns (spikes in final months)
- Identify true underlying cost vs artificial inflation

**CHRONIC DISEASE BURDEN:**
{json.dumps(data.get('chronic_disease', {}), indent=2)}
- Assess population health risk
- Determine if costs are episodic (fixable) or structural (persistent)

**CLAIMS TREND ANALYSIS:**
{json.dumps(data.get('trend_decomposition', {}), indent=2)}
- Separate unit cost inflation from utilization increases
- Recommend different interventions for each

**COLLECTION & PAYMENT RISK:**
{json.dumps(data.get('financial', {}).get('collection_analysis', {}), indent=2)}
- Assess default risk
- Factor into renewal decision

**PA SYSTEM EFFECTIVENESS:**
{json.dumps(data.get('pa', {}).get('pa_effectiveness', {}), indent=2)}
- Evaluate gatekeeping effectiveness
- Identify cost control opportunities

**PLAN-LEVEL ANALYSIS (Enrollment vs Utilization):**
{json.dumps(data.get('plan_analysis', {}), indent=2)}
- Analyze each plan's enrollment % vs claims %
- Identify over-utilizing plans (need premium increase)
- Identify under-utilizing plans (opportunity for growth)
- Assess overall portfolio balance
- Provide plan-specific pricing recommendations

**PROVIDER BANDING ANALYSIS:**
{json.dumps(data.get('provider_bands', {}), indent=2)}
- Review provider band distribution (A/B/C/D/Special/Unknown)
- Identify cost exposure from high-band providers
- Flag unknown bands requiring verification

**BENEFIT LIMIT VIOLATIONS:**
{json.dumps(data.get('benefit_analysis', {}), indent=2)}
- Identify members who exceeded monetary or count-based limits
- Calculate total monetary loss from over-limit claims
- Flag benefits with NO limits defined (unlimited exposure)
- Assess if limits are too low, not enforced, or if fraud is occurring
- Recommend limit adjustments and enforcement strategies

**COMPREHENSIVE RISK SCORE:**
{json.dumps(data.get('risk_score', {}), indent=2)}
- Overall renewal risk assessment (0-100 scale)
- Recommended action based on industry standards

**YOUR TASK:**
Analyze ALL these metrics together to provide:
1. Executive summary considering ALL factors (not just MLR) - use the COMPREHENSIVE RISK SCORE as your primary guide
2. Industry-standard renewal recommendation based on risk categories:
   - 0-30 = RENEW (minor adjustments)
   - 31-60 = ADJUST (moderate changes needed)
   - 61-80 = MAJOR CHANGES (significant restructuring)
   - 81-100 = TERMINATE (or extreme intervention)
3. Specific action items based on root causes:
   - If unit cost trend high → Provider negotiation
   - If utilization trend high → Care management
   - If chronic disease high → Disease management programs
   - If gaming detected → Adjust pricing based on non-gaming months
   - If collection poor → Payment terms restructuring
   - If plans over-utilizing → Plan-specific premium increases (15-25%)
   - If provider bands unknown → Verify with NHIS immediately
   - If high-cost bands dominant → Redirect to lower-band providers
   - If benefit limits violated → Review and adjust limits, investigate fraud/abuse
   - If unlimited benefits exist → Define limits immediately to control exposure
4. Brutal truth about actual risks (don't sugarcoat if risk score is >60)
5. Plan-specific recommendations for over/under-utilizing plans

**Generate:**

## 🔴 THE BRUTAL TRUTH
(2-3 sentences - be DIRECT and HONEST about status - reference the RISK SCORE)

## 📊 KEY METRICS vs BENCHMARKS

| Metric | Value | Target | Variance | Status |
|--------|-------|--------|----------|--------|
| Risk Score | {data.get('risk_score', {}).get('total_risk_score', 'N/A')} | <30 (Low) | - | {' 🔴' if data.get('risk_score', {}).get('total_risk_score', 0) > 60 else '🟡' if data.get('risk_score', {}).get('total_risk_score', 0) > 30 else '✅'} |
| (Fill with remaining emoji indicators: 🔴/🟡/✅)

## 🎯 CONCENTRATION ANALYSIS
**Pattern:** EPISODIC or STRUCTURAL?
**Reasoning:** (Explain why based on Top 5 % and condition mix)

## 🚨 CRITICAL RISK FACTORS
(3-5 bullet points - what's driving this situation)

## ⚠️ FRAUD & GAMING INDICATORS
(List specific evidence with severity levels)

## 💡 STRATEGIC ASSESSMENT
Is this EPISODIC (fixable) or STRUCTURAL (systemic)? Why?

## 🎯 IMMEDIATE ACTIONS (Next 30 Days)
- Week 1: (specific actions)
- Week 2-4: (specific actions)
- If no progress: (escalation)

Use emoji coding throughout. Be brutally honest like Godrej/Alert/NAHCO analyses."""

        # Inject business context
        full_prompt = ClearlineBusinessContext.enrich_prompt(prompt)
        
        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=3000,
                messages=[{"role": "user", "content": full_prompt}]
            )
            
            self.token_count += message.usage.input_tokens + message.usage.output_tokens
            self.cost_naira += self._calculate_cost(message.usage.input_tokens, message.usage.output_tokens)
            
            return message.content[0].text
            
        except Exception as e:
            return f"⚠️ AI analysis failed: {str(e)}"
    
    def predict_future_mlr(self, data: Dict) -> Dict:
        """🔮 AI MLR Projection"""
        
        prompt = f"""Project end-of-contract MLR for {data['company_name']}.

**CURRENT:** MLR {data['mlr']['bv_mlr']:.1f}%, Month {data['contract']['months_elapsed']} of {data['contract']['total_months']}

**UTILIZATION:** {data['concentration']['type']}, Chronic {data['conditions']['chronic_pct']:.1f}%, Preventable {data['conditions']['preventable_pct']:.1f}%

**LIABILITY:** Unclaimed PA ₦{data['pa']['unclaimed_amount']:,.0f}

Provide JSON:
{{
  "projected_mlr": <number>,
  "confidence": "High/Medium/Low",
  "assumptions": ["key assumption 1", "key assumption 2"],
  "risk_factors": ["risk 1", "risk 2"],
  "reasoning": "brief explanation"
}}"""

        full_prompt = ClearlineBusinessContext.enrich_prompt(prompt)
        
        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                messages=[{"role": "user", "content": full_prompt}]
            )
            
            self.token_count += message.usage.input_tokens + message.usage.output_tokens
            self.cost_naira += self._calculate_cost(message.usage.input_tokens, message.usage.output_tokens)
            
            response_text = message.content[0].text
            if '```json' in response_text:
                response_text = response_text.split('```json')[1].split('```')[0]
            
            return json.loads(response_text.strip())
            
        except Exception as e:
            # Fallback
            return {
                "projected_mlr": data['mlr']['bv_mlr'] * 1.2,
                "confidence": "Low",
                "assumptions": ["Linear projection"],
                "risk_factors": ["Unclaimed PA"],
                "reasoning": f"Fallback: {str(e)}"
            }
    
    def generate_negotiation_strategy(self, data: Dict, recommendation: Dict) -> str:
        """💼 AI Negotiation Strategy"""
        
        prompt = f"""Create negotiation strategy for {data['company_name']}.

**RECOMMENDATION:** {recommendation['action']}
**PREMIUM CHANGE:** {recommendation['premium_change_pct']:+.0f}%
**SUCCESS PROBABILITY:** {recommendation['success_probability']}%

**METRICS:** MLR {data['mlr']['bv_mlr']:.1f}%, PMPM ₦{data['mlr']['pmpm']:,.0f}, Payment {data['financial']['payment_rate']:.1f}%

Generate:

## 💼 NEGOTIATION STRATEGY

### Opening Statement
(How to start - diplomatic but data-driven)

### Data Points to Present
(Specific numbers to share - table format)

### Client Objections & Responses
**Objection 1:** "..."
**Response:** "..."

### Alternative Options
**Option A:** (Premium + conditions)
**Option B:** (Different approach)

### Walk-Away Criteria
(When to terminate - be specific)

Use your Godrej/Alert/NAHCO learnings. Be firm but professional."""

        full_prompt = ClearlineBusinessContext.enrich_prompt(prompt)
        
        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=3000,
                messages=[{"role": "user", "content": full_prompt}]
            )
            
            self.token_count += message.usage.input_tokens + message.usage.output_tokens
            self.cost_naira += self._calculate_cost(message.usage.input_tokens, message.usage.output_tokens)
            
            return message.content[0].text
            
        except Exception as e:
            return f"⚠️ Strategy failed: {str(e)}"
    
    def detect_anomalies(self, data: Dict, portfolio: Optional[List[Dict]] = None) -> str:
        """🔍 AI Anomaly Detection"""
        
        context = ""
        if portfolio:
            context = "**PORTFOLIO CONTEXT:**\n"
            for comp in portfolio[:5]:
                context += f"- {comp['company_name']}: MLR {comp['mlr']['bv_mlr']:.1f}%, PMPM ₦{comp['mlr']['pmpm']:,.0f}\n"
        
        prompt = f"""Forensic analysis for {data['company_name']}.

{context}

**METRICS:** MLR {data['mlr']['bv_mlr']:.1f}%, Unknown Provider {data['fraud']['unknown_pct']:.1f}%, Chronic {data['conditions']['chronic_pct']:.1f}%

Identify:
## 🔍 STATISTICAL ANOMALIES
(Numbers that are outliers)

## 🚨 FRAUD INDICATORS  
(Patterns suggesting fraud/gaming with evidence)

## 🔗 HIDDEN CORRELATIONS
(Unexpected relationships)

## ⚠️ EARLY WARNING SIGNS
(Problems not yet obvious)

Use emoji indicators. Be forensic."""

        full_prompt = ClearlineBusinessContext.enrich_prompt(prompt)
        
        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{"role": "user", "content": full_prompt}]
            )
            
            self.token_count += message.usage.input_tokens + message.usage.output_tokens
            self.cost_naira += self._calculate_cost(message.usage.input_tokens, message.usage.output_tokens)
            
            return message.content[0].text
            
        except Exception as e:
            return f"⚠️ Detection failed: {str(e)}"
    
    def get_cost_summary(self) -> Dict:
        """Get cost tracking summary"""
        return {
            'total_tokens': self.token_count,
            'total_cost_naira': self.cost_naira,
            'cost_per_analysis': self.cost_naira
        }


# ═══════════════════════════════════════════════════════════════════════
# STEP 3: COMPLETE ANALYSIS FUNCTION
# ═══════════════════════════════════════════════════════════════════════

def run_complete_analysis(company_name: str,
                         debit_override: Optional[float] = None,
                         cash_override: Optional[float] = None,
                         use_motherduck: bool = False):
    """
    Complete hybrid analysis: Python + AI

    This replaces your old run_analysis() function
    """

    # Progress tracking
    progress = st.empty()

    # PHASE 1: Python Calculations (Fast - no AI cost)
    db_source = "MotherDuck (Cloud)" if use_motherduck else "Local Database"
    progress.info(f"⚡ Phase 1/4: Running Python calculations... (Source: {db_source})")
    analysis_data = run_comprehensive_analysis(
        company_name,
        debit_override,
        cash_override,
        use_motherduck
    )

    if not analysis_data.get('success'):
        st.error(f"❌ {analysis_data.get('error')}")
        return

    # PHASE 2: AI Executive Summary
    progress.info("🤖 Phase 2/4: Generating AI executive summary...")
    # Try Streamlit secrets first, then environment variable, then fallback
    try:
        api_key = (
            st.secrets.get('anthropic', {}).get('api_key')
            or os.getenv('ANTHROPIC_API_KEY')
            or ""
        )
    except Exception:
        api_key = os.getenv('ANTHROPIC_API_KEY') or ""
    ai_engine = AIIntelligenceEngine(api_key)

    ai_summary = ai_engine.generate_executive_summary(analysis_data)

    # PHASE 3: AI Predictions & Strategy
    progress.info("🔮 Phase 3/4: Creating predictions & strategy...")
    ai_predictions = ai_engine.predict_future_mlr(analysis_data)

    # Create a dummy recommendation for the strategy
    dummy_recommendation = {
        'action': 'RENEW' if analysis_data['mlr']['bv_mlr'] < 75 else 'INCREASE PREMIUM',
        'premium_change_pct': 0 if analysis_data['mlr']['bv_mlr'] < 75 else 15,
        'success_probability': 75
    }
    ai_strategy = ai_engine.generate_negotiation_strategy(analysis_data, dummy_recommendation)

    # PHASE 4: AI Anomaly Detection
    progress.info("🔍 Phase 4/4: Detecting anomalies...")
    ai_anomalies = ai_engine.detect_anomalies(analysis_data)

    # Get cost summary from the AI engine
    cost_summary = ai_engine.get_cost_summary()
    cost_tracking = {
        'summary': cost_summary['cost_per_analysis'] / 4,  # Rough estimate per call
        'predictions': cost_summary['cost_per_analysis'] / 4,
        'strategy': cost_summary['cost_per_analysis'] / 4,
        'anomalies': cost_summary['cost_per_analysis'] / 4,
        'total': cost_summary['cost_per_analysis']
    }

    # Hospital Banding (optional - only if user requests)
    banding_results = None
    if st.session_state.get('run_banding', False):
        progress.info("🏥 Bonus: Analyzing hospital bands...")
        banding_results = run_banding_analysis_for_company(analysis_data)
        st.session_state['run_banding'] = False  # Reset flag

    progress.empty()

    # Success!
    total_cost = cost_tracking['total']
    st.success(f"✅ Analysis complete! AI cost: ₦{total_cost:.2f} (${total_cost/1500:.2f})")

    # Save to session state
    st.session_state.update({
        'analysis_data': analysis_data,
        'ai_summary': ai_summary,
        'ai_predictions': ai_predictions,
        'ai_strategy': ai_strategy,
        'ai_anomalies': ai_anomalies,
        'banding_results': banding_results,
        'cost_tracking': cost_tracking
    })


# ═══════════════════════════════════════════════════════════════════════
# STEP 4: DISPLAY FUNCTION
# ═══════════════════════════════════════════════════════════════════════

def show_results():
    """Display analysis results in 8 tabs"""

    # Check if analysis was run
    if 'analysis_data' not in st.session_state:
        st.info("👆 Run an analysis above to see results")
        return

    # Get all data
    data = st.session_state['analysis_data']
    ai_summary = st.session_state.get('ai_summary', {})
    ai_predictions = st.session_state.get('ai_predictions', {})
    ai_strategy = st.session_state.get('ai_strategy', {})
    ai_anomalies = st.session_state.get('ai_anomalies', {})
    banding = st.session_state.get('banding_results')
    costs = st.session_state.get('cost_tracking', {})

    # Header
    st.markdown(f"# 📊 {data['company_name']}")

    contract = data['contract']
    col1, col2, col3 = st.columns(3)
    col1.metric("Contract Start", str(contract['startdate']))
    col2.metric("Contract End", str(contract['enddate']))
    col3.metric("Months Elapsed", contract['months_elapsed'])

    # Cost summary
    st.info(f"💰 Total AI Cost: ₦{costs.get('total', 0):.2f} (${costs.get('total', 0)/1500:.2f})")

    st.markdown("---")

    # Create 10 tabs
    tabs = st.tabs([
        "🤖 AI Summary",
        "🔮 Predictions",
        "💰 Financial",
        "📊 Utilization",
        "🏥 Conditions",
        "🏥 Providers + Banding",
        "📋 Plan Analysis",
        "🚨 Benefit Limits",
        "🔍 Anomalies",
        "💼 Strategy",
        "⚠️ Risk Score"
    ])

    with tabs[0]:
        render_ai_executive_summary(data, ai_summary, costs)

    with tabs[1]:
        render_ai_predictions(data, ai_predictions, costs)

    with tabs[2]:
        render_financial_analysis(data)

    with tabs[3]:
        render_utilization_analysis(data)

    with tabs[4]:
        render_conditions_breakdown(data)

    with tabs[5]:
        render_provider_analysis(data, banding)
        # Add banding button if not run yet
        if not banding:
            st.markdown("---")
            if st.button("🏥 Run Hospital Banding Analysis"):
                st.session_state['run_banding'] = True
                st.rerun()

    with tabs[6]:
        render_plan_analysis(data)

    with tabs[7]:
        render_benefit_limits_tab(data.get('benefit_analysis', {}), data['company_name'])

    with tabs[8]:
        render_anomaly_detection(data, ai_anomalies, costs)

    with tabs[9]:
        render_negotiation_strategy(data, ai_strategy, costs)

    with tabs[10]:
        render_risk_dashboard(data, data.get('risk_score', {}))


# ═══════════════════════════════════════════════════════════════════════
# STEP 5: MAIN FUNCTION
# ═══════════════════════════════════════════════════════════════════════

def main():
    """Main application"""

    # Check API key (but allow hardcoded fallback in run_complete_analysis)
    # Just verify Anthropic library is available
    if not ANTHROPIC_AVAILABLE:
        st.error("❌ Install anthropic: pip install anthropic")
        st.stop()

    # Title
    st.title("🤖 AI Contract Renewal Analyzer")
    st.caption("Hybrid: Python Speed + Claude Intelligence")

    # Sidebar
    with st.sidebar:
        st.header("📋 Analysis Setup")

        # Mode
        mode = st.radio("Mode", ["Single Company (AI)", "Quick Batch", "Compare 2-3"])

        if mode == "Single Company (AI)":
            st.markdown("---")
            st.subheader("Company")

            # Database Source Toggle (moved before company loading)
            st.markdown("### 🗄️ Database Source")
            use_motherduck = st.checkbox(
                "☁️ Use MotherDuck (Cloud)",
                value=False,
                help="Use cloud database for faster access. Requires initial sync.",
                key="motherduck_toggle"
            )

            if use_motherduck:
                st.success("🌐 Using MotherDuck cloud database")
            else:
                st.info("💻 Using local database")

            st.markdown("---")

            # Load active companies from database
            @st.cache_data(ttl=300)
            def get_active_companies(use_md: bool = False):
                """Get list of active companies from database"""
                try:
                    import duckdb

                    if use_md:
                        # Load token from motherduck.py
                        import importlib.util
                        spec = importlib.util.spec_from_file_location(
                            "motherduck_config",
                            "/Users/kenechukwuchukwuka/Downloads/DLT/motherduck.py"
                        )
                        motherduck_config = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(motherduck_config)
                        token = motherduck_config.MOTHERDUCK_TOKEN

                        conn = duckdb.connect(f'md:?motherduck_token={token}', read_only=True)
                        conn.execute("USE ai_driven_data")
                    else:
                        db_path = '/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb'
                        conn = duckdb.connect(db_path, read_only=True)

                    query = """
                    SELECT DISTINCT groupname
                    FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
                    WHERE enddate >= CURRENT_DATE
                        OR enddate IS NULL
                    ORDER BY groupname
                    """

                    result = conn.execute(query).fetchdf()
                    conn.close()

                    if len(result) > 0:
                        return [''] + result['groupname'].tolist()
                    else:
                        return ['LORNA NIGERIA LIMITED (GODREJ)']

                except Exception as e:
                    st.warning(f"Could not load companies: {e}")
                    return ['LORNA NIGERIA LIMITED (GODREJ)']

            companies = get_active_companies(use_md=use_motherduck)

            # Company dropdown
            company = st.selectbox(
                "Select Company",
                companies,
                index=0,
                help="Choose from active companies in database"
            )

            if not company:
                st.warning("⚠️ Please select a company")
                st.stop()

            # Optional overrides
            st.markdown("### 💰 Custom Financials")
            st.caption("Leave at 0 to use database")

            debit = st.number_input("Debit (₦)", 0.0, step=1000.0, format="%.2f")
            cash = st.number_input("Cash (₦)", 0.0, step=1000.0, format="%.2f")

            debit_override = debit if debit > 0 else None
            cash_override = cash if cash > 0 else None

            # Run button
            st.markdown("---")
            if st.button("🚀 RUN ANALYSIS", type="primary", use_container_width=True):
                run_complete_analysis(company, debit_override, cash_override, use_motherduck)

        else:
            st.info(f"{mode} - Coming soon!")

        # Cost tracker
        if 'cost_tracking' in st.session_state:
            st.markdown("---")
            st.subheader("💰 Session Costs")
            costs = st.session_state['cost_tracking']
            st.metric("This Analysis", f"₦{costs.get('total', 0):.2f}")

    # Main display area
    show_results()

    # Footer
    st.markdown("---")
    st.caption("🤖 Powered by Claude Sonnet 4 | Built for Clearline HMO | 2025")


if __name__ == "__main__":
    main()