"""
AI Intelligence Engine for Renewal Analysis
===========================================

Claude API integration with complete Clearline business context.
Replicates AIIntelligenceEngine from contract_analyzer_complete_hybrid.py
"""

import anthropic
import json
import os
from typing import Dict, Optional, List


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

## 🆕 ADVANCED METRICS FOR RENEWAL DECISIONS

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


class AIIntelligenceEngine:
    """Claude API with complete Clearline business context"""

    def __init__(self, api_key: str):
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

        # Safely extract data with defaults
        company_name = data.get('company_name', 'Unknown Company')
        contract = data.get('contract', {})
        mlr = data.get('mlr', {})
        financial = data.get('financial', {})
        concentration = data.get('concentration', {})
        enrollment = data.get('enrollment', {})
        claims = data.get('claims', {})
        conditions = data.get('conditions', {})
        fraud = data.get('fraud', {})

        prompt = f"""Generate an executive summary for {company_name}.

**CONTRACT PERFORMANCE:**
- Contract Period: {contract.get('months_elapsed', 0)} of {contract.get('total_months', 12)} months ({contract.get('months_to_end', 0)} remaining)
- BV-MLR: {mlr.get('bv_mlr', 0):.1f}%
- CASH-MLR: {mlr.get('cash_mlr', 0):.1f}%
- PMPM: ₦{mlr.get('pmpm', 0):,.0f}
- PMPM Variance: {mlr.get('pmpm_variance', 0):+.1f}%
- Monthly Burn: ₦{mlr.get('monthly_burn', 0):,.0f}

**FINANCIAL STATUS:**
- Total Debit: ₦{financial.get('debit', 0):,.0f}
- Total Cash: ₦{financial.get('cash', 0):,.0f}
- Payment Rate: {financial.get('payment_rate', 0):.1f}%
- Outstanding: ₦{financial.get('outstanding', 0):,.0f}
- Commission Rate: {financial.get('commission_rate', 0.10) * 100:.0f}%

**UTILIZATION:**
- Pattern: {concentration.get('pattern_type', 'Unknown')}
- Top 5 Members: {concentration.get('top5_pct', 0):.1f}% of costs
- Top 10 Members: {concentration.get('top10_pct', 0):.1f}% of costs
- Enrolled: {enrollment.get('member_count', 0)}
- Claimed: {claims.get('unique_claimants', 0)}
- Utilization: {(claims.get('unique_claimants', 0) / enrollment.get('member_count', 1) * 100) if enrollment.get('member_count', 0) > 0 else 0:.1f}%

**CONDITIONS:**
- One-off (Maternity/Surgery/Trauma): {conditions.get('one_off_pct', 0):.1f}%
- Chronic (HTN/Diabetes/Respiratory/Kidney): {conditions.get('chronic_pct', 0):.1f}%
- Preventable (Malaria/URI/UTI): {conditions.get('preventable_pct', 0):.1f}%
- Catastrophic (Cancer/Stroke): {conditions.get('catastrophic_pct', 0):.1f}%

**FRAUD INDICATORS:**
- Risk Level: {fraud.get('risk_level', 'Unknown')}
- Unknown Provider: {fraud.get('unknown_pct', 0):.1f}% (₦{fraud.get('unknown_amount', 0):,.0f})
- Same-Day Multiple Claims: {fraud.get('same_day_count', 0)} instances

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
3. Specific action items based on root causes
4. Brutal truth about actual risks (don't sugarcoat if risk score is >60)

**Generate a COMPREHENSIVE executive summary with ALL sections below:**

## 🔴 THE BRUTAL TRUTH
(2-3 sentences - be DIRECT and HONEST about status - reference the RISK SCORE. Start with company name and overall assessment)

## 📊 KEY METRICS vs BENCHMARKS

Create a COMPLETE table with ALL these metrics:

| Metric | Value | Target | Variance | Status |
|--------|-------|--------|----------|--------|
| Risk Score | {data.get('risk_score', {}).get('total_score', 'N/A')} | <30 (Low) | Calculate variance | {' 🔴' if data.get('risk_score', {}).get('total_score', 0) > 60 else '🟡' if data.get('risk_score', {}).get('total_score', 0) > 30 else '✅'} |
| BV-MLR | {mlr.get('bv_mlr', 0):.1f}% | <75% | Calculate +/- | Use 🔴/🟡/✅ |
| CASH-MLR | {mlr.get('cash_mlr', 0):.1f}% | <75% | Calculate +/- | Use 🔴/🟡/✅ |
| PMPM | ₦{mlr.get('pmpm', 0):,.0f} | ₦3,000 | Calculate +/- % | Use 🔴/🟡/✅ |
| Payment Rate | {financial.get('payment_rate', 0):.1f}% | >80% | Calculate +/- | Use 🔴/🟡/✅ |
| Outstanding | ₦{financial.get('outstanding', 0):,.0f} | <₦10M | Calculate +/- % | Use 🔴/🟡/✅ |
| Utilization | {(claims.get('unique_claimants', 0) / enrollment.get('member_count', 1) * 100) if enrollment.get('member_count', 0) > 0 else 0:.1f}% | 30-35% | Calculate +/- | Use 🔴/🟡/✅ |
| Chronic Disease | {conditions.get('chronic_pct', 0):.1f}% | <20% | Calculate +/- % | Use 🔴/🟡/✅ |
| Preventable | {conditions.get('preventable_pct', 0):.1f}% | <25% | Calculate +/- % | Use 🔴/🟡/✅ |

## 🎯 CONCENTRATION ANALYSIS
**Pattern:** EPISODIC or STRUCTURAL?
**Reasoning:** (Explain in detail based on Top 5 %, Top 10 %, and condition mix. Use specific percentages provided)

## 🚨 CRITICAL RISK FACTORS
List 5-7 specific bullet points about what's driving this situation. Include:
- Payment/collection issues
- Benefit violations if any
- PA system effectiveness
- Same-day claims if high
- Structural patterns
- Use SPECIFIC numbers from the data

## ⚠️ FRAUD & GAMING INDICATORS
List ALL fraud indicators with severity levels:
- Same-Day Claims: {fraud.get('same_day_count', 0)} instances (assess if normal/medium/high)
- Unknown Providers: {fraud.get('unknown_pct', 0):.1f}% (LOW/MEDIUM/HIGH RISK based on thresholds)
- PA Gaming: (mention approval rates if 100% or suspicious)
- Benefit Violations: (mention if any overage detected)
- Monthly Trending: (mention if gaming pattern detected)

## 💡 STRATEGIC ASSESSMENT
Detailed assessment: Is this EPISODIC (fixable) or STRUCTURAL (systemic)? Explain WHY with supporting evidence from concentration %, chronic disease %, and utilization patterns.

## 🔴 PLAN-LEVEL CRITICAL FINDINGS
(Analyze plan distribution if data available - mention over-utilizing or balanced patterns)

## 🎯 IMMEDIATE ACTIONS (Next 30 Days)

### Week 1: Financial Stabilization
- URGENT: (specific payment demands)
- (PA controls)
- (Benefit limit actions)

### Week 2-4: Control Implementation
- (Chronic disease management)
- (PA system changes)
- (Provider steering)

### If No Progress: Escalation
- Month 2: (specific action)
- Month 3: (premium increase or termination)
- Month 4: (forensic audit if needed)

## 💰 RENEWAL RECOMMENDATIONS

### Option 1: AGGRESSIVE RESTRUCTURING (+X% Premium)
- Specific premium increase %
- PA controls
- Payment terms
- Projected MLR range

### Option 2: SELECTIVE CONTINUATION (+X% Premium + Controls)
- Moderate premium increase %
- Enhanced care management
- Payment requirements
- Projected MLR range

### Option 3: NON-RENEWAL
- When to consider (if payment/fraud/MLR thresholds met)

## 🚨 WALK-AWAY CRITERIA
List 4-5 specific criteria when termination is required:
- Payment rate thresholds
- Benefit violation limits
- PA improvement requirements
- Program acceptance

**RECOMMENDATION:** Final clear recommendation (Option 1/2/3) with justification.

Use emoji coding throughout (🔴/🟡/✅). Be brutally honest. Include ALL sections with COMPLETE details like the Godrej/Alert/NAHCO analyses."""

        # Inject business context
        full_prompt = ClearlineBusinessContext.enrich_prompt(prompt)

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=8000,  # Increased for comprehensive analysis
                messages=[{"role": "user", "content": full_prompt}]
            )

            self.token_count += message.usage.input_tokens + message.usage.output_tokens
            self.cost_naira += self._calculate_cost(message.usage.input_tokens, message.usage.output_tokens)

            return message.content[0].text

        except Exception as e:
            return f"⚠️ AI analysis failed: {str(e)}"

    def predict_future_mlr(self, data: Dict) -> Dict:
        """🔮 AI MLR Projection"""

        mlr = data.get('mlr', {})
        contract = data.get('contract', {})
        concentration = data.get('concentration', {})
        conditions = data.get('conditions', {})
        pa = data.get('pa', {})

        prompt = f"""Project end-of-contract MLR for {data.get('company_name', 'Unknown Company')}.

**CURRENT:** MLR {mlr.get('bv_mlr', 0):.1f}%, Month {contract.get('months_elapsed', 0)} of {contract.get('total_months', 12)}

**UTILIZATION:** {concentration.get('pattern_type', 'Unknown')}, Chronic {conditions.get('chronic_pct', 0):.1f}%, Preventable {conditions.get('preventable_pct', 0):.1f}%

**LIABILITY:** Unclaimed PA ₦{pa.get('unclaimed_amount', 0):,.0f}

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
                "projected_mlr": mlr.get('bv_mlr', 0) * 1.2,
                "confidence": "Low",
                "assumptions": ["Linear projection"],
                "risk_factors": ["Unclaimed PA"],
                "reasoning": f"Fallback: {str(e)}"
            }

    def generate_negotiation_strategy(self, data: Dict) -> str:
        """💼 AI Negotiation Strategy"""

        mlr = data.get('mlr', {})
        financial = data.get('financial', {})
        risk_score = data.get('risk_score', {})

        # Determine recommendation based on risk score
        total_score = risk_score.get('total_score', 0)
        if total_score < 30:
            action = "RENEW"
            premium_change = 0
            probability = 90
        elif total_score < 60:
            action = "ADJUST PREMIUM"
            premium_change = 10
            probability = 75
        elif total_score < 80:
            action = "MAJOR CHANGES"
            premium_change = 20
            probability = 50
        else:
            action = "TERMINATE OR RESTRUCTURE"
            premium_change = 30
            probability = 25

        prompt = f"""Create negotiation strategy for {data.get('company_name', 'Unknown Company')}.

**RECOMMENDATION:** {action}
**PREMIUM CHANGE:** {premium_change:+.0f}%
**SUCCESS PROBABILITY:** {probability}%

**METRICS:** MLR {mlr.get('bv_mlr', 0):.1f}%, PMPM ₦{mlr.get('pmpm', 0):,.0f}, Payment {financial.get('payment_rate', 0):.1f}%

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
                max_tokens=4000,  # Increased for strategy details
                messages=[{"role": "user", "content": full_prompt}]
            )

            self.token_count += message.usage.input_tokens + message.usage.output_tokens
            self.cost_naira += self._calculate_cost(message.usage.input_tokens, message.usage.output_tokens)

            return message.content[0].text

        except Exception as e:
            return f"⚠️ Strategy failed: {str(e)}"

    def detect_anomalies(self, data: Dict) -> str:
        """🔍 AI Anomaly Detection"""

        mlr = data.get('mlr', {})
        fraud = data.get('fraud', {})
        conditions = data.get('conditions', {})

        prompt = f"""Forensic analysis for {data.get('company_name', 'Unknown Company')}.

**METRICS:** MLR {mlr.get('bv_mlr', 0):.1f}%, Unknown Provider {fraud.get('unknown_pct', 0):.1f}%, Chronic {conditions.get('chronic_pct', 0):.1f}%

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
