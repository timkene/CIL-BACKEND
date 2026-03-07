"""
AI Narrator — uses Claude claude-sonnet-4-6 to generate intelligent narrative sections
for the renewal report. Each section is driven by actual data.
"""

import json
import os
import anthropic
from .data_collector import RenewalData

MODEL = "claude-sonnet-4-6"
OPUS_MODEL = "claude-opus-4-5"  # Opus for deep strategic reasoning

def _call_claude(prompt: str, system: str = None, max_tokens: int = 1500) -> str:
    """Call Claude API and return text response."""
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    
    messages = [{"role": "user", "content": prompt}]
    
    kwargs = {
        "model": MODEL,
        "max_tokens": max_tokens,
        "messages": messages
    }
    if system:
        kwargs["system"] = system
    
    response = client.messages.create(**kwargs)
    return response.content[0].text.strip()


def generate_executive_summary(data: RenewalData) -> list[str]:
    """Generate executive summary bullet points using AI."""
    
    # Build context summary for AI
    top5 = data.top_members[:5] if data.top_members else []
    top5_str = "\n".join([
        f"  - {m['name']} ({m['gender']}/{m['age']}yrs): ₦{m['amount']:,.0f} ({m['pct']}%)"
        for m in top5
    ])
    
    top_prov = data.top_providers[:3] if data.top_providers else []
    prov_str = "\n".join([
        f"  - {p['name']}: ₦{p['amount']:,.0f} ({p['pct']}% of PA)"
        for p in top_prov
    ])
    
    prompt = f"""You are a Nigerian HMO actuarial analyst at Clearline International Limited.
    
Generate an executive summary for the {data.group_name} renewal analysis.
Write EXACTLY 6 bullet points — concise, data-driven, frank.

KEY DATA:
- Group: {data.group_name}
- Active Members: {data.active_members}
- Current Contract: {data.current_start} to {data.current_end}
- Claims (YTD {data.claims_months} months): ₦{data.claims_total:,.0f} ({data.claims_count} claims)
- Annualized Claims: ₦{data.annualized_claims:,.0f}
- Total Debit (billed): ₦{data.total_debit:,.0f}
- Cash Received: ₦{data.cash_received:,.0f} ({round(data.cash_received/data.total_debit*100,1) if data.total_debit else 0}%)
- Projected Annual MLR: {data.projected_mlr}% (threshold: 75%)
- SRS Classification: {data.srs_classification}
- Top 5 Member Concentration: {data.top5_pct}% of claims
- Previous Contract MLR: {data.prev_mlr}%

Top 5 high-cost members:
{top5_str}

Top providers (PA authorized):
{prov_str}

CRITICAL INSTRUCTIONS:
1. DO NOT apply the 3:1 premium-to-limit ratio as an absolute rule. The Nigerian HMO corporate group market typically has limit:premium ratios of 10x-25x. High ratios are normal for group corporate plans.
2. Premium adequacy must be assessed using PMPM analysis:
   - Previous PMPM: ₦{data.prev_pmpm:,.0f}/member/month
   - Current PMPM: ₦{data.curr_pmpm:,.0f}/member/month  
   - Actuarial premium (70% MLR target): ₦{data.actuarial_premium:,.0f}/head/year
3. Elevated MLR driven by high-cost episodic events requires sub-limits, not necessarily premium hikes equal to 3x the limit.
4. Be direct. No hedging. Use Nigerian health insurance terminology.
5. Mention outstanding cash compliance if payment rate <70%.

Return ONLY the 6 bullet points, one per line, starting with "•". No headers, no extra text."""
    
    result = _call_claude(prompt, max_tokens=1000)
    
    # Parse bullet points
    lines = [l.strip().lstrip("•-").strip() for l in result.split("\n") if l.strip()]
    lines = [l for l in lines if len(l) > 10]
    return lines[:6] if len(lines) >= 6 else lines


def generate_srs_analysis(data: RenewalData) -> dict:
    """Generate SRS narrative analysis."""
    
    top5 = data.top_members[:5] if data.top_members else []
    top5_str = json.dumps([
        {"name": m["name"], "amount": m["amount"], "pct": m["pct"]}
        for m in top5
    ], indent=2)
    
    prompt = f"""You are a Nigerian HMO actuarial analyst.

Provide a Structural Risk Score (SRS) analysis for {data.group_name}.

DATA:
- Top 5 Concentration: {data.top5_pct}% of claims (>40% = Episodic)
- Chronic Disease Load: {data.chronic_pct}% of claims (<30% = Low Structural)
- Projected MLR: {data.projected_mlr}%
- Members Utilizing: {data.members_utilizing}/{data.active_members} ({round(data.members_utilizing/data.active_members*100,1) if data.active_members else 0}%)
- Utilisation Breadth Classification: {"✅ LOW (<50%)" if data.active_members and data.members_utilizing/data.active_members < 0.50 else "✅ NORMAL (50-70%)" if data.active_members and data.members_utilizing/data.active_members < 0.70 else "⚠ ELEVATED (70-80%) — above industry normal, indicates broad scheme uptake beyond episodic events" if data.active_members and data.members_utilizing/data.active_members < 0.80 else "⚠ HIGH (>80%) — structural concern, near-universal utilisation signals embedded disease burden or moral hazard"}
- SRS Result: {data.srs_classification}
- Previous MLR: {data.prev_mlr}%

Top 5 members:
{top5_str}

Write 2-3 short paragraphs:
1. What the SRS classification means for this portfolio
2. Key drivers of concentration 
3. What this means for renewal premium strategy — applying PMPM-based pricing (NOT applying 3:1 ratio as absolute rule; 3:1 framework governs premium increase:limit increase ratio during renewals, NOT absolute premium sizing)

Return plain text paragraphs only. No headers, no bullets. Maximum 200 words total."""
    
    return {"narrative": _call_claude(prompt, max_tokens=500)}


def generate_premium_recommendation(data: RenewalData) -> dict:
    """Generate premium recommendation with correct PMPM-based actuarial reasoning."""
    
    plans_str = json.dumps(data.plans, indent=2)
    
    prompt = f"""You are a certified actuarial analyst at a Nigerian HMO.

Generate a renewal premium recommendation for {data.group_name}.

FINANCIAL DATA:
- Current premium (main plan): {f"₦{data.plans[0]['premium']:,.0f}/head" if data.plans else "N/A (no plan data)"}
- Plans: {plans_str}
- Historical PMPM: ₦{data.prev_pmpm:,.0f}/member/month (previous contract)
- Current PMPM: ₦{data.curr_pmpm:,.0f}/member/month (YTD, may be inflated by acute events)
- Actuarially-sound premium (PMPM × 12 / 0.70 target MLR): ₦{data.actuarial_premium:,.0f}/head/year
- Projected Annual MLR: {data.projected_mlr}%
- SRS: {data.srs_classification}
- Previous MLR: {data.prev_mlr}%

PRICING FRAMEWORK (industry standard):
1. PMPM-BASED PRICING: Premium = Expected PMPM × 12 / Target MLR (70%)
2. MLR BANDS:
   - 75-85% Episodic → 10-15% increase
   - 85-100% Episodic → 15-20% increase  
   - >100% Any → 25-35% increase
3. The 3:1 ratio (premium:limit) governs RENEWAL ADJUSTMENTS (e.g., 15% premium increase should be justified against limit changes) — it is NOT an absolute pricing rule that limits must equal 3× premium. Corporate group plans in Nigeria have 10x-25x limit:premium ratios and this is normal.
4. Limit:premium ratio of {data.plans[0].get('ratio', 0) if data.plans else 0}× is {'within normal range (10x-25x typical)' if data.plans and data.plans[0].get('ratio', 0) <= 25 else 'above normal range — flag for review'}.

Write a 2-paragraph recommendation:
1. Recommended premium increase percentage and actuarial basis
2. Whether benefit limits should change and why (focus on SURGICAL SUB-LIMITS as risk control, not reducing overall benefit limit)

Return plain text only. Max 150 words."""
    
    return {"narrative": _call_claude(prompt, max_tokens=400)}


def generate_provider_narrative(data: RenewalData) -> dict:
    """Generate provider analysis narrative."""
    
    top_prov = data.top_providers[:5] if data.top_providers else []
    prev_prov = {p["name"]: p["amount"] for p in data.prev_top_providers}
    
    prov_data = []
    for p in top_prov:
        prev_amt = prev_prov.get(p["name"], 0)
        change = f"+{round((p['amount']/prev_amt - 1)*100)}%" if prev_amt > 0 else "NEW"
        prov_data.append({**p, "prev_amount": prev_amt, "change": change})
    
    prompt = f"""You are a Nigerian HMO claims analyst.

Analyze provider performance for {data.group_name}.

TOP PROVIDERS (current contract):
{json.dumps(prov_data, indent=2)}

Write 2 short paragraphs:
1. Key provider concentration findings and any red flags (new providers with high spend, abnormal per-PA values)
2. Recommended action for the top concern provider

Include specific numbers. Return plain text only. Max 120 words."""
    
    return {"narrative": _call_claude(prompt, max_tokens=350)}



def generate_renewal_strategy(data: RenewalData) -> str:
    """
    Deep AI renewal strategy using Claude Opus.
    Reasons across providers, repeat enrollees, utilisation breadth, MLR trend,
    and PMPM pricing to produce specific, data-driven renewal recommendations.
    """
    util_rate = round(data.members_utilizing / data.active_members * 100, 1) if data.active_members else 0
    util_class = (
        "LOW (<50%) — scheme underutilised" if util_rate < 50 else
        "NORMAL (50-70%)" if util_rate < 70 else
        "ELEVATED (70-80%) — above industry normal. Combined with high top-5 concentration, this means you have BOTH deep (expensive few) AND broad (many using) utilisation pressure. This is more serious than episodic alone." if util_rate < 80 else
        "HIGH (>80%) — near-universal utilisation. Structural concern: nearly every member is using the scheme, pointing to embedded disease burden or moral hazard."
    )
    
    repeat_members_str = json.dumps(data.repeat_high_cost_members, indent=2) if data.repeat_high_cost_members else "[]"
    providers_str = json.dumps(data.provider_dual_period, indent=2) if data.provider_dual_period else "[]"
    top5_str = json.dumps([
        {"name": m["name"], "iid": m["iid"], "amount": m["amount"],
         "pct": m["pct"], "primary_condition": m["primary_condition"], "nature": m["nature"]}
        for m in data.top_members[:5]
    ], indent=2)
    plans_str = json.dumps(data.plans, indent=2)
    monthly_str = json.dumps(data.monthly_claims, indent=2)

    prompt = f"""You are the Chief Actuarial Officer at Clearline International Limited, a Nigerian HMO.
Produce the RENEWAL STRATEGY section for {data.group_name}.
Reason through ALL data provided. Every recommendation must cite a specific number. No generic statements.

FINANCIAL SUMMARY
Contract: {data.current_start} to {data.current_end} | Active Members: {data.active_members}
Total Debit: ₦{data.total_debit:,.0f} | Cash Received: ₦{data.cash_received:,.0f} ({round(data.cash_received/data.total_debit*100,1) if data.total_debit else 0}% payment rate)
Claims Total: ₦{data.claims_total:,.0f} | Unclaimed PA: ₦{data.unclaimed_pa:,.0f} ({data.unclaimed_pa_count} pending)
MLR Numerator (Claims + Unclaimed PA): ₦{data.claims_total + data.unclaimed_pa:,.0f}
YTD MLR: {data.ytd_mlr}% | Projected Annual MLR: {data.projected_mlr}% | Cash MLR: {data.cash_mlr}%
Previous Contract: Claims ₦{data.prev_claims:,.0f} | Debit ₦{data.prev_debit:,.0f} | MLR {data.prev_mlr}%
Previous PMPM: ₦{data.prev_pmpm:,.0f}/member/month → Current PMPM: ₦{data.curr_pmpm:,.0f}/member/month ({round((data.curr_pmpm/data.prev_pmpm-1)*100,1) if data.prev_pmpm else 0}% change)
Actuarial Premium Target (PMPM×12÷0.70): ₦{data.actuarial_premium:,.0f}/head/year

SRS: {data.srs_classification} | Top 5 Concentration: {data.top5_pct}% | Chronic Load: {data.chronic_pct}%

UTILISATION BREADTH: {util_rate}% ({util_class})
IMPORTANT: Industry standard is 50-70% for group corporate plans. This group is at {util_rate}%.
If ELEVATED or HIGH: this COMPOUNDS the risk because it means the scheme is under pressure from BOTH concentrated high-cost members AND a wide base of regular utilisers. Factor this into your premium recommendation.

PLAN STRUCTURE:
{plans_str}
NOTE: Plans with suffix like -12kiz are mid-contract joiners (prorated premium/benefit). Premium recommendation applies only to the BASE plan (the one WITHOUT a suffix).

TOP 5 HIGH-COST MEMBERS (current contract):
{top5_str}

REPEAT HIGH-COST MEMBERS (expensive in BOTH contracts — these are structural risks):
{repeat_members_str}

PROVIDER PERFORMANCE (both contracts):
FLAGS: NEW_PROVIDER=not seen before | HIGH_AVG_PA=avg PA >₦80k | RAPID_GROWTH=>80% spend growth
{providers_str}

MONTHLY CLAIMS TREND:
{monthly_str}

TASK: Write a renewal strategy with these 5 sections using **SECTION NAME** as headers:

**1. PREMIUM INCREASE RECOMMENDATION**
State exact % increase. Base on: PMPM trend, MLR trajectory, utilisation breadth classification, and SRS. 
Use PMPM-based pricing (Premium = Expected PMPM × 12 / 0.70). 
If utilisation breadth is ELEVATED or HIGH, this warrants a higher increase than SRS alone would suggest — explain why.
Nigerian group corporate plans have 10x-25x limit:premium ratios — this is NORMAL.

**2. ENROLLEE RISK MANAGEMENT**
For each repeat high-cost member: state their risk level (ESCALATING/PERSISTENT/DECLINING) and specific action.
Actions: CDMP enrolment | mandatory referral gating | sub-limit per condition | flag for HR discussion.
Do NOT recommend removing members outright (contractual issue) — recommend plan structure changes.

**3. PROVIDER NETWORK ACTIONS**
For each flagged provider (NEW_PROVIDER, HIGH_AVG_PA, RAPID_GROWTH): state action clearly.
Actions: NEGOTIATE TARIFF | ADD TO WATCHLIST | REQUIRE PRE-AUTH ESCALATION | CONSIDER NETWORK REMOVAL.
Be specific about which providers and why. Per-PA averages above ₦200k are red flags.

**4. BENEFIT STRUCTURE CHANGES**
Should overall limit change? By how much? Justify from utilisation data.
Are surgical sub-limits needed? At what level?
Any CDMP additions given chronic load?

**5. CASH COMPLIANCE** (only include if payment rate <70%)
State outstanding balance and required action before renewal confirmation.

Maximum 500 words. Direct, specific, Nigerian HMO industry language."""

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model=OPUS_MODEL,
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text.strip()


def generate_all_narratives(data: RenewalData) -> dict:
    """Generate all narrative sections. Returns dict of section_name -> content."""
    print(f"[AI] Generating narratives for {data.group_name}...")
    
    narratives = {}
    
    try:
        print("[AI] Executive summary...")
        narratives["executive_bullets"] = generate_executive_summary(data)
    except Exception as e:
        print(f"[AI] Executive summary failed: {e}")
        narratives["executive_bullets"] = [
            f"Portfolio projected annual MLR: {data.projected_mlr}% (threshold: 75%)",
            f"SRS classification: {data.srs_classification} — Top 5 members = {data.top5_pct}% of claims",
            f"Cash compliance: ₦{data.cash_received:,.0f} received of ₦{data.total_debit:,.0f} billed",
        ]
    
    try:
        print("[AI] SRS analysis...")
        narratives["srs_narrative"] = generate_srs_analysis(data)["narrative"]
    except Exception as e:
        print(f"[AI] SRS failed: {e}")
        narratives["srs_narrative"] = f"SRS classification: {data.srs_classification}. Top 5 concentration: {data.top5_pct}%. Chronic disease load: {data.chronic_pct}%."
    
    try:
        print("[AI] Premium recommendation...")
        narratives["premium_narrative"] = generate_premium_recommendation(data)["narrative"]
    except Exception as e:
        print(f"[AI] Premium recommendation failed: {e}")
        narratives["premium_narrative"] = f"Based on PMPM analysis, the actuarially-sound premium is ₦{data.actuarial_premium:,.0f}/head/year at 70% target MLR."
    
    try:
        print("[AI] Provider narrative...")
        narratives["provider_narrative"] = generate_provider_narrative(data)["narrative"]
    except Exception as e:
        print(f"[AI] Provider narrative failed: {e}")
        narratives["provider_narrative"] = "Provider concentration analysis pending."
    
    try:
        print("[AI] Renewal strategy (Opus)...")
        narratives["renewal_strategy"] = generate_renewal_strategy(data)
    except Exception as e:
        print(f"[AI] Renewal strategy failed: {e}")
        narratives["renewal_strategy"] = (
            f"{data.srs_classification} portfolio — {data.top5_pct:.1f}% of claims in top 5 members. "
            f"Projected MLR: {data.projected_mlr}%. "
            f"Utilisation breadth: {round(data.members_utilizing/data.active_members*100,1) if data.active_members else 0}% of members utilizing. "
            f"PMPM trend: ₦{data.prev_pmpm:,.0f} → ₦{data.curr_pmpm:,.0f}/member/month. "
            f"Actuarial premium target: ₦{data.actuarial_premium:,.0f}/head/year."
        )

    print("[AI] All narratives generated.")
    return narratives