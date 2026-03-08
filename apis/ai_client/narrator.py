"""
AI Narrator — uses Claude claude-sonnet-4-6 to generate intelligent narrative sections
for the renewal report. Each section is driven by actual data.
"""

import json
import os
import anthropic
import httpx
from .data_collector import RenewalData

MODEL = "claude-sonnet-4-6"
OPUS_MODEL = "claude-opus-4-6"  # Opus for deep strategic reasoning

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


def get_provider_fraud_scores(data: RenewalData) -> dict:
    """
    Calls the fraud API (include_ai=False) for each top provider.
    Passes group_id so the API runs the network CPE benchmark.
    Returns {provider_name: response_dict | None}.
    Falls back gracefully if the API is unreachable.
    """
    fraud_api_url = os.environ.get("FRAUD_API_URL", "").rstrip("/")
    if not fraud_api_url:
        return {}

    results = {}
    start = data.current_start.isoformat() if data.current_start else None
    end   = data.current_end.isoformat()   if data.current_end   else None
    if not start or not end:
        return {}

    for prov in data.top_providers[:5]:
        name = prov.get("name", "")
        if not name or name == "Unknown":
            continue
        try:
            resp = httpx.post(
                f"{fraud_api_url}/fraud-score",
                json={
                    "provider_name": name,
                    "start_date":    start,
                    "end_date":      end,
                    "include_ai":    False,
                    "group_id":      str(data.group_id),
                },
                timeout=30.0,
            )
            if resp.status_code == 200:
                results[name] = resp.json()
            elif resp.status_code == 404:
                results[name] = {"_status": "NOT_FOUND"}   # provider not in claims DB
            else:
                results[name] = None                        # API error → truly unknown
        except Exception:
            results[name] = None

    return results


def _provider_verdict(fraud_result: dict | None) -> tuple[str, int]:
    """
    Derive a (verdict_string, encounters_this_group) tuple from a fraud API response.

    Guards:
    - None result → fraud API was down → "UNKNOWN" status, flag for manual review
    - encounters_this_group < 10 → LOW CONFIDENCE suffix
    - network_signal = INSUFFICIENT_DATA → state data is insufficient, do NOT infer CLEAN
    - network_signal = GROUP_TARGETED → flag regardless of band score
    """
    if fraud_result is None:
        return "⚠ UNKNOWN (fraud API unavailable — manual review required)", 0
    if fraud_result.get("_status") == "NOT_FOUND":
        return "— NOT IN CLAIMS DB (no matching provider data in period)", 0

    score        = fraud_result.get("total_score", 0) or 0
    alert_status = fraud_result.get("alert_status", "CLEAR")
    signals      = fraud_result.get("network_signals", [])
    net_signal   = signals[0].get("network_signal", "INSUFFICIENT_DATA") if signals else "INSUFFICIENT_DATA"
    cpe_ratio    = signals[0].get("cpe_ratio", 0)    if signals else 0
    encounters   = signals[0].get("encounters_this_group", 0) if signals else 0

    band_verdict = {
        "ALERT":     "🚨 ALERT",
        "WATCHLIST": "⚠ WATCHLIST",
        "CLEAR":     "✅ CLEAN",
    }.get(alert_status, "✅ CLEAN")

    confidence = " [LOW CONFIDENCE — <10 encounters]" if encounters < 10 else ""

    if net_signal == "GROUP_TARGETED":
        return f"{band_verdict} | 🎯 NETWORK: GROUP-TARGETED (CPE ratio {cpe_ratio}×){confidence}", encounters
    if net_signal == "INSUFFICIENT_DATA":
        return f"{band_verdict} | — network: INSUFFICIENT DATA (manual tariff review recommended){confidence}", encounters
    return f"{band_verdict} | ✅ network: CLEAN (CPE ratio {cpe_ratio}×){confidence}", encounters


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
    PMPM pricing, and provider fraud signals to produce specific, data-driven
    renewal recommendations.
    """
    util_rate = round(data.members_utilizing / data.active_members * 100, 1) if data.active_members else 0
    util_class = (
        "LOW (<50%) — scheme underutilised" if util_rate < 50 else
        "NORMAL (50-70%)" if util_rate < 70 else
        "ELEVATED (70-80%) — above industry normal. Combined with high top-5 concentration, this means you have BOTH deep (expensive few) AND broad (many using) utilisation pressure. This is more serious than episodic alone." if util_rate < 80 else
        "HIGH (>80%) — near-universal utilisation. Structural concern: nearly every member is using the scheme, pointing to embedded disease burden or moral hazard."
    )
    
    # Fetch fraud verdicts for top providers (fails silently if API unavailable)
    fraud_scores = get_provider_fraud_scores(data)
    fraud_verdicts_str = ""
    if fraud_scores:
        lines = []
        for prov_name, result in fraud_scores.items():
            verdict, encounters = _provider_verdict(result)
            score = result.get("total_score", "N/A") if result else "N/A"
            lines.append(
                f"  - {prov_name}: {verdict} "
                f"(score: {score}/10, encounters: {encounters})"
            )
        fraud_verdicts_str = "\n".join(lines)
    else:
        fraud_verdicts_str = "  (Fraud API not available — manual review required for all providers)"

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

PROVIDER FRAUD SIGNALS (band-level score + network CPE benchmark):
Verdicts: 🚨 ALERT=≥5/10 | ⚠ WATCHLIST=3-4/10 | ✅ CLEAN=<3/10
Network: 🎯 GROUP-TARGETED=provider charges this group >1.5× their network average CPE
{fraud_verdicts_str}

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
For each flagged provider (NEW_PROVIDER, HIGH_AVG_PA, RAPID_GROWTH, fraud ALERT/WATCHLIST, GROUP-TARGETED): state action clearly.
Actions: NEGOTIATE TARIFF | ADD TO WATCHLIST | REQUIRE PRE-AUTH ESCALATION | CONSIDER NETWORK REMOVAL | FRAUD INVESTIGATION.
Be specific about which providers and why. Per-PA averages above ₦200k are red flags.
For GROUP-TARGETED providers: their CPE for this group is >1.5× their network average — flag for tariff renegotiation.
For ALERT/WATCHLIST fraud scores: recommend pre-auth escalation or claims audit before renewal.
CRITICAL GUARDS — you MUST apply these:
1. LOW CONFIDENCE: If a provider verdict includes [LOW CONFIDENCE — <10 encounters], do NOT recommend network removal or fraud investigation. State the data is insufficient and recommend a 90-day claims audit before any action.
2. INSUFFICIENT DATA: If network_signal = INSUFFICIENT DATA, do NOT infer the provider is clean. State that network benchmarking was not possible (provider serves <3 groups in the network) and recommend manual tariff comparison.
3. UNKNOWN (fraud API down): If verdict = UNKNOWN, state that fraud scoring was unavailable for this provider and flag for manual fraud review before renewal sign-off. Do not infer any risk level.

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
        import traceback; traceback.print_exc()
        print(f"[AI] Executive summary FAILED: {e}")
        narratives["executive_bullets"] = [
            f"Portfolio projected annual MLR: {data.projected_mlr}% (threshold: 75%)",
            f"SRS classification: {data.srs_classification} — Top 5 members = {data.top5_pct}% of claims",
            f"Cash compliance: ₦{data.cash_received:,.0f} received of ₦{data.total_debit:,.0f} billed",
        ]

    try:
        print("[AI] SRS analysis...")
        narratives["srs_narrative"] = generate_srs_analysis(data)["narrative"]
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"[AI] SRS FAILED: {e}")
        narratives["srs_narrative"] = f"SRS classification: {data.srs_classification}. Top 5 concentration: {data.top5_pct}%. Chronic disease load: {data.chronic_pct}%."

    try:
        print("[AI] Premium recommendation...")
        narratives["premium_narrative"] = generate_premium_recommendation(data)["narrative"]
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"[AI] Premium recommendation FAILED: {e}")
        narratives["premium_narrative"] = f"Based on PMPM analysis, the actuarially-sound premium is ₦{data.actuarial_premium:,.0f}/head/year at 70% target MLR."

    try:
        print("[AI] Provider narrative...")
        narratives["provider_narrative"] = generate_provider_narrative(data)["narrative"]
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"[AI] Provider narrative FAILED: {e}")
        narratives["provider_narrative"] = "Provider concentration analysis pending."

    try:
        print("[AI] Renewal strategy (Opus)...")
        narratives["renewal_strategy"] = generate_renewal_strategy(data)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"[AI] Renewal strategy FAILED: {e}")
        narratives["renewal_strategy"] = (
            f"{data.srs_classification} portfolio — {data.top5_pct:.1f}% of claims in top 5 members. "
            f"Projected MLR: {data.projected_mlr}%. "
            f"Utilisation breadth: {round(data.members_utilizing/data.active_members*100,1) if data.active_members else 0}% of members utilizing. "
            f"PMPM trend: ₦{data.prev_pmpm:,.0f} → ₦{data.curr_pmpm:,.0f}/member/month. "
            f"Actuarial premium target: ₦{data.actuarial_premium:,.0f}/head/year."
        )

    print("[AI] All narratives generated.")
    return narratives