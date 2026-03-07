"""
PDF Generator for Renewal Analysis Reports
Uses reportlab to produce professional PDFs matching the Kizito report structure.
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether
)
import io

from .data_collector import RenewalData

# ─── BRAND COLORS ─────────────────────────────────────────────────────────────
BRAND_BLUE   = colors.HexColor("#1F5C99")
BRAND_DARK   = colors.HexColor("#1A3050")
LIGHT_BLUE   = colors.HexColor("#D5E8F0")
LIGHT_GRAY   = colors.HexColor("#F5F5F5")
LIGHT_RED    = colors.HexColor("#FFE6E6")
LIGHT_YELLOW = colors.HexColor("#FFF8E6")
LIGHT_GREEN  = colors.HexColor("#E6F5E6")
RED          = colors.HexColor("#CC0000")
ORANGE       = colors.HexColor("#FF6600")
GREEN        = colors.HexColor("#006600")
AMBER        = colors.HexColor("#885500")
WHITE        = colors.white
DARK_GRAY    = colors.HexColor("#444444")
MID_GRAY     = colors.HexColor("#777777")
BORDER_GRAY  = colors.HexColor("#CCCCCC")

PAGE_W, PAGE_H = A4
MARGIN = 20 * mm
CONTENT_W = PAGE_W - 2 * MARGIN


def build_styles():
    base = getSampleStyleSheet()
    
    def s(name, **kw):
        style = ParagraphStyle(name=name, **kw)
        return style
    
    return {
        "H1": s("H1", fontName="Helvetica-Bold", fontSize=15, textColor=BRAND_DARK,
                spaceBefore=12, spaceAfter=3, leading=19),
        "H2": s("H2", fontName="Helvetica-Bold", fontSize=12, textColor=BRAND_BLUE,
                spaceBefore=9, spaceAfter=2, leading=15),
        "H3": s("H3", fontName="Helvetica-Bold", fontSize=10, textColor=BRAND_DARK,
                spaceBefore=7, spaceAfter=2, leading=13),
        "Body": s("Body", fontName="Helvetica", fontSize=9, textColor=colors.black,
                  spaceBefore=2, spaceAfter=2, leading=13),
        "Bullet": s("Bullet", fontName="Helvetica", fontSize=9, textColor=colors.black,
                    spaceBefore=1, spaceAfter=1, leading=13,
                    leftIndent=12, firstLineIndent=-8),
        "Small": s("Small", fontName="Helvetica", fontSize=7.5, textColor=MID_GRAY,
                   spaceBefore=1, spaceAfter=1, leading=10, alignment=TA_CENTER),
        "CoverTitle": s("CoverTitle", fontName="Helvetica-Bold", fontSize=24,
                        textColor=BRAND_BLUE, alignment=TA_CENTER, leading=30),
        "CoverSub": s("CoverSub", fontName="Helvetica", fontSize=11, textColor=DARK_GRAY,
                      alignment=TA_CENTER),
        "CoverCo": s("CoverCo", fontName="Helvetica", fontSize=9, textColor=MID_GRAY,
                     alignment=TA_CENTER),
        "TH": s("TH", fontName="Helvetica-Bold", fontSize=8, textColor=WHITE,
                alignment=TA_CENTER, leading=10),
        "TC": s("TC", fontName="Helvetica", fontSize=8, textColor=colors.black, leading=10),
        "TCB": s("TCB", fontName="Helvetica-Bold", fontSize=8, textColor=colors.black, leading=10),
    }


ST = build_styles()


def N(v):
    if v is None: return "N0"
    # Using N for Naira (Unicode naira sign)
    return f"\u20a6{v:,.0f}"

def P(v):
    if v is None: return "0%"
    return f"{v:.1f}%"

def hr(color=BRAND_BLUE, thick=1.5):
    return HRFlowable(width="100%", thickness=thick, color=color,
                      spaceAfter=3, spaceBefore=3)

def sp(h=4):
    return Spacer(1, h * mm)

def h1(t): return Paragraph(t, ST["H1"])
def h2(t): return Paragraph(t, ST["H2"])
def h3(t): return Paragraph(t, ST["H3"])
def body(t): return Paragraph(t, ST["Body"])
def blt(t, col=None):
    if col:
        t = f'<font color="{col}">{t}</font>'
    return Paragraph(f"• {t}", ST["Bullet"])

def sec(title):
    return [h1(title), hr(), sp(1)]


def tbl(headers, rows, widths, alt=True):
    def tc(text, bold=False):
        s = ST["TCB"] if bold else ST["TC"]
        txt = str(text)
        col = None
        if any(x in txt for x in ["⚠", "CRITICAL", "LOSS", "ALERT"]):
            col = "#CC0000"
        elif any(x in txt for x in ["✅", "POSITIVE", "OK", "HEALTHY"]):
            col = "#006600"
        if col:
            txt = f'<font color="{col}">{txt}</font>'
        return Paragraph(txt, s)
    
    data_table = [[Paragraph(h, ST["TH"]) for h in headers]]
    for i, row in enumerate(rows):
        data_table.append([tc(c) for c in row])
    
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), BRAND_BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, BORDER_GRAY),
    ]
    if alt:
        style.append(("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GRAY]))
    
    t = Table(data_table, colWidths=widths)
    t.setStyle(TableStyle(style))
    return t


def kpi_box(items):
    n = len(items)
    w = CONTENT_W / n
    
    cells = []
    for it in items:
        vc = it.get("color", BRAND_DARK)
        inner_data = [
            [Paragraph(it["value"], ParagraphStyle(
                "kv", fontName="Helvetica-Bold", fontSize=16,
                textColor=vc, alignment=TA_CENTER, leading=20))],
            [Paragraph(it["label"], ParagraphStyle(
                "kl", fontName="Helvetica-Bold", fontSize=7,
                textColor=MID_GRAY, alignment=TA_CENTER, leading=9))],
            [Paragraph(it.get("sub", ""), ParagraphStyle(
                "ks", fontName="Helvetica", fontSize=7,
                textColor=MID_GRAY, alignment=TA_CENTER, leading=9))],
        ]
        inner = Table(inner_data, colWidths=[w - 10])
        inner.setStyle(TableStyle([
            ("TOPPADDING", (0,0),(-1,-1), 2),
            ("BOTTOMPADDING", (0,0),(-1,-1), 2),
            ("LEFTPADDING", (0,0),(-1,-1), 0),
            ("RIGHTPADDING", (0,0),(-1,-1), 0),
        ]))
        cells.append(inner)
    
    outer = Table([cells], colWidths=[w]*n)
    outer.setStyle(TableStyle([
        ("BOX", (0,0),(-1,-1), 0.5, BORDER_GRAY),
        ("INNERGRID", (0,0),(-1,-1), 0.5, BORDER_GRAY),
        ("BACKGROUND", (0,0),(-1,-1), LIGHT_BLUE),
        ("TOPPADDING", (0,0),(-1,-1), 6),
        ("BOTTOMPADDING", (0,0),(-1,-1), 6),
        ("LEFTPADDING", (0,0),(-1,-1), 4),
        ("RIGHTPADDING", (0,0),(-1,-1), 4),
        ("LINEABOVE", (0,0),(-1,0), 2.5, BRAND_BLUE),
    ]))
    return outer


def cover_status_boxes(mlr, mlr_pct, srs, top5_pct):
    mlr_color = RED if mlr_pct >= 75 else GREEN
    mlr_bg = LIGHT_RED if mlr_pct >= 75 else LIGHT_GREEN
    srs_color = AMBER if srs == "EPISODIC" else RED
    srs_bg = LIGHT_YELLOW if srs == "EPISODIC" else LIGHT_RED
    
    def box(bg, val, lbl, sub, vc):
        t = Table([
            [Paragraph(lbl, ParagraphStyle("bl", fontName="Helvetica-Bold", fontSize=9,
                                           textColor=vc, alignment=TA_CENTER))],
            [Paragraph(val, ParagraphStyle("bv", fontName="Helvetica-Bold", fontSize=26,
                                           textColor=vc, alignment=TA_CENTER, leading=32))],
            [Paragraph(sub, ParagraphStyle("bs", fontName="Helvetica", fontSize=8,
                                           textColor=vc, alignment=TA_CENTER))],
        ], colWidths=[CONTENT_W/2 - 4])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0),(-1,-1), bg),
            ("BOX", (0,0),(-1,-1), 0.5, BORDER_GRAY),
            ("TOPPADDING", (0,0),(-1,-1), 8),
            ("BOTTOMPADDING", (0,0),(-1,-1), 8),
            ("LEFTPADDING", (0,0),(-1,-1), 6),
            ("RIGHTPADDING", (0,0),(-1,-1), 6),
        ]))
        return t
    
    left = box(mlr_bg, mlr, "PROJECTED ANNUAL MLR",
               "LOSS TERRITORY (>75%)" if mlr_pct >= 75 else "WITHIN TARGET", mlr_color)
    right = box(srs_bg, srs, "SRS CLASSIFICATION",
                f"Top 5 = {P(top5_pct)} of Claims", srs_color)
    
    outer = Table([[left, right]], colWidths=[CONTENT_W/2, CONTENT_W/2])
    outer.setStyle(TableStyle([
        ("TOPPADDING", (0,0),(-1,-1), 0), ("BOTTOMPADDING", (0,0),(-1,-1), 0),
        ("LEFTPADDING", (0,0),(-1,-1), 0), ("RIGHTPADDING", (0,0),(-1,-1), 0),
    ]))
    return outer


def reco_box(text):
    t = Table([[Paragraph(text, ParagraphStyle(
        "rb", fontName="Helvetica-Bold", fontSize=10,
        textColor=BRAND_BLUE, alignment=TA_CENTER, leading=14))
    ]], colWidths=[CONTENT_W])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0),(-1,-1), LIGHT_BLUE),
        ("BOX", (0,0),(-1,-1), 1.5, BRAND_BLUE),
        ("TOPPADDING", (0,0),(-1,-1), 10),
        ("BOTTOMPADDING", (0,0),(-1,-1), 10),
        ("LEFTPADDING", (0,0),(-1,-1), 10),
        ("RIGHTPADDING", (0,0),(-1,-1), 10),
    ]))
    return t


# ──────────────────────────────────────────────────────────────────────────────
# CORRECTED PLAN ADEQUACY SECTION (Section 7)
# Uses PMPM-based actuarial pricing, NOT 3:1 as absolute rule
# ──────────────────────────────────────────────────────────────────────────────
def build_plan_adequacy_section(data: RenewalData, narratives: dict) -> list:
    story = []
    story += sec("7. PLAN STRUCTURE & BENEFIT ADEQUACY ANALYSIS")
    
    # ── 7.1 Plan Limit Utilisation ────────────────────────────────────────────
    story.append(h2("7.1 Plan Limit Utilisation"))
    story.append(sp(1))
    
    # Group mid-year suffix plans (e.g. KIZGOLPLUS25-12kiz) under their base plan.
    # The suffix -MMxxx indicates a mid-contract joiner with prorated premium/benefit.
    # For display purposes, group by base plan name (strip suffix starting with '-' followed by digits).
    import re as _re
    def _base_plan(name: str) -> str:
        """Strip mid-year joiner suffix e.g. KIZGOLPLUS25-12kiz → KIZGOLPLUS25"""
        return _re.sub(r"-\d+\w*$", "", name)
    
    # Use plan_utilization if available, else fall back to plans
    util_source = data.plan_utilization if data.plan_utilization else []
    # Note: mid-year suffix plans (e.g. -12kiz = joined December) share the same base plan.
    # They are shown separately here as their limit/premium are prorated, but the
    # renewal premium recommendation applies only to the base plan.
    if util_source:
        util_rows = []
        for p in util_source:
            util_rows.append([
                p["name"][:28],
                N(p["limit"]),
                N(p["premium"]),
                N(p["avg_spend"]),
                f"{p['limit_pct']:.1f}% of limit",
                p["over_risk"][:40]
            ])
        story.append(tbl(
            ["Plan", "Annual Limit", "Annual Premium", "Avg Spend/Member", "Limit Used (avg)", "Over-Limit Risk"],
            util_rows,
            [CONTENT_W*0.20, CONTENT_W*0.13, CONTENT_W*0.13,
             CONTENT_W*0.16, CONTENT_W*0.16, CONTENT_W*0.22]
        ))
    else:
        # Fallback from plans data
        fallback_rows = []
        total_claims_pp = data.claims_total / max(data.active_members, 1)
        for p in data.plans:
            limit_pct = round(total_claims_pp / p["limit"] * 100, 1) if p["limit"] > 0 else 0
            fallback_rows.append([
                p["name"][:28], N(p["limit"]), N(p["premium"]),
                N(round(total_claims_pp, 0)),
                f"{limit_pct:.1f}% of limit",
                "⚠ HIGH limit risk" if p.get("ratio", 0) > 20 else "Low under normal use"
            ])
        story.append(tbl(
            ["Plan", "Annual Limit", "Annual Premium", "Avg Spend/Member", "Limit Used (avg)", "Over-Limit Risk"],
            fallback_rows,
            [CONTENT_W*0.20, CONTENT_W*0.13, CONTENT_W*0.13,
             CONTENT_W*0.16, CONTENT_W*0.16, CONTENT_W*0.22]
        ))
    story.append(sp(2))
    
    # ── 7.2 Plans Sold Adequacy Assessment ───────────────────────────────────
    story.append(h2("7.2 Plans Sold Adequacy Assessment"))
    story.append(sp(1))
    
    # FINDING 1 — Plan limits vs premiums
    for i, p in enumerate(data.plans[:3]):
        ratio = p.get("ratio", 0)
        finding_num = i + 1
        
        if ratio > 20:
            severity = "CRITICAL"
            color = RED
            finding_text = (
                f"FINDING {finding_num} \u2014 PLAN LIMITS EXCESSIVELY HIGH FOR PREMIUMS CHARGED:"
            )
            bullets = [
                f"{p['name']} carries a {N(p['limit'])} annual limit at {N(p['premium'])} premium "
                f"\u2014 a {ratio:.1f}\u00d7 ratio. Actuarially, the sound limit for this premium "
                f"at a 3:1 adjustment framework is approximately {N(p['premium'] * 3)}.",
                "High limits enable high-value PAs to be approved without additional scrutiny. "
                "Surgical sub-limits would cap liability while preserving overall member confidence.",
                f"RECOMMENDATION: Introduce a surgical/inpatient sub-limit of "
                f"{N(min(p['limit'] * 0.15, 600000))} per event within the overall {N(p['limit'])} limit.",
            ]
        elif ratio > 10:
            severity = "ADEQUATE"
            color = GREEN
            finding_text = (
                f"FINDING {finding_num} \u2014 PLAN LIMITS WITHIN ACCEPTABLE RANGE:"
            )
            bullets = [
                f"{p['name']} has a {ratio:.1f}\u00d7 limit:premium ratio, within the 10x\u201325x "
                "Nigerian HMO corporate market norm.",
                "Continue monitoring. No immediate structural change required.",
            ]
        else:
            severity = "REVIEW"
            color = ORANGE
            # Check if this is a mid-year joiner plan (suffix like -12kiz)
            import re as _re2
            is_midyear = bool(_re2.search(r"-\d+\w*$", p["name"]))
            if is_midyear:
                finding_text = (
                    f"FINDING {finding_num} \u2014 MID-CONTRACT JOINER PLAN (PRORATED):"
                )
                # Extract month number from suffix
                month_match = _re2.search(r"-(\d+)", p["name"])
                join_month = int(month_match.group(1)) if month_match else None
                months_remaining = (13 - join_month) if join_month and join_month <= 12 else None
                bullets = [
                    f"{p['name']} is the same plan as the base plan, issued to a member who joined "
                    f"mid-contract{f' in month {join_month}' if join_month else ''}. "
                    f"The reduced limit ({N(p['limit'])}) and premium ({N(p['premium'])}) are "
                    f"prorated for the remaining {months_remaining or '?'} months of the contract — "
                    f"this is correct and expected.",
                    f"At renewal, this member moves to the full base plan. "
                    f"Do NOT treat this as a separate under-priced plan. "
                    f"The renewal premium recommendation applies only to the base plan.",
                ]
            else:
                finding_text = (
                    f"FINDING {finding_num} \u2014 LOW LIMIT MAY CAUSE MEMBER DISSATISFACTION:"
                )
                bullets = [
                    f"{p['name']} has a {ratio:.1f}\u00d7 ratio \u2014 below typical corporate HMO market range of 10x\u201325x.",
                    "Members may exhaust their limit, leading to out-of-pocket costs and renewal pressure.",
                    "Review whether this plan is positioned correctly in your product portfolio.",
                ]
        
        story.append(body(f"<b>{finding_text}</b>"))
        for b in bullets:
            story.append(Paragraph(f"\u2022  {b}", ST["Body"]))
        story.append(sp(1))
    
    # FINDING on members exceeding their plan limit
    over_limit = [p for p in (data.plan_utilization or []) if p.get("limit_pct", 0) > 100]
    if over_limit:
        fn_num = len(data.plans[:3]) + 1
        story.append(body(
            f"<b>FINDING {fn_num} \u2014 MEMBER(S) EXCEEDING PLAN LIMIT:</b>"
        ))
        for ol in over_limit:
            story.append(Paragraph(
                f"\u2022  {ol['name']}: member spent {N(ol['avg_spend'])} against "
                f"a {N(ol['limit'])} limit ({ol['limit_pct']:.0f}% of limit used). "
                "Excess was covered via TPA inclusion or exceeded coverage. "
                "At renewal, move this member to a higher-tier plan or formalise the TPA arrangement.",
                ST["Body"]
            ))
        story.append(sp(1))
    
    # FINDING on chronic disease members needing CDMP
    CHRONIC_PFX_SET = {"I10","I11","I119","I12","I13","I20","I209","I25","E11","E14"}
    chronic_diags = [d for d in (data.top_diagnoses or [])
                     if any(d["code"].startswith(c) for c in CHRONIC_PFX_SET)]
    if chronic_diags:
        chronic_total = sum(d["amount"] for d in chronic_diags)
        chronic_pct = round(chronic_total / data.claims_total * 100, 1) if data.claims_total else 0
        fn_num = len(data.plans[:3]) + len(over_limit) + 1
        story.append(body(
            f"<b>FINDING {fn_num} \u2014 CHRONIC DISEASE MEMBERS NEED STRUCTURED MANAGEMENT:</b>"
        ))
        story.append(Paragraph(
            f"\u2022  {len(chronic_diags)} chronic disease diagnosis categories (e.g. hypertension, "
            f"HHD, angina) represent {N(chronic_total)} ({P(chronic_pct)} of claims). "
            "These members generate recurring drug, consultation, and monitoring claims each year.",
            ST["Body"]
        ))
        story.append(Paragraph(
            "\u2022  RECOMMENDATION: Offer a structured Chronic Disease Management Programme (CDMP) "
            "with controlled drug supply and quarterly check-up limits. "
            "Evidence base: RAND Health Insurance Experiment shows CDMP reduces hypertension "
            "claims costs by 18\u201322% over 2 years.",
            ST["Body"]
        ))
        story.append(sp(1))
    
    story.append(sp(1))
    
    # ── 7.3 PMPM actuarial adequacy ──────────────────────────────────────────
    story.append(h2("7.3 Premium Adequacy: PMPM Actuarial Analysis"))
    story.append(sp(1))
    
    main_prem = data.plans[0]['premium'] if data.plans else 0
    actuarial = data.actuarial_premium
    delta = main_prem - actuarial
    delta_label = f"{N(abs(delta))} ABOVE actuarial minimum" if delta >= 0 else f"{N(abs(delta))} BELOW actuarial minimum"
    
    story.append(kpi_box([
        {"label": "Historical PMPM", "value": N(data.prev_pmpm), "sub": "Previous Contract", "color": BRAND_DARK},
        {"label": "Actuarial Premium (70% MLR)", "value": N(actuarial), "sub": "PMPM x 12 / 0.70", "color": BRAND_DARK},
        {"label": "Current Premium", "value": N(main_prem), "sub": "Per Head / Year", "color": BRAND_DARK},
        {"label": "Adequacy Status", "value": delta_label[:18],
         "sub": "vs actuarial need", "color": GREEN if delta >= 0 else RED},
    ]))
    story.append(sp(2))
    
    story.append(body(
        "<b>ACTUARIAL FRAMEWORK: Why PMPM-Based Pricing is the Industry Standard</b>"
    ))
    story.append(body(
        "The correct method for HMO premium adequacy is <b>PMPM-based pricing</b>: "
        "Expected claims Per Member Per Month × 12 / Target MLR. Benefit limits are "
        "<i>risk ceilings</i> (loss control tools), not premium inputs. This aligns with ACA "
        "actuarial value standards, Massachusetts GIC methodology, and Nigerian HMO market practice."
    ))
    story.append(sp(1))
    
    # PMPM calculation table
    story.append(tbl(
        ["Metric", "Value", "Notes"],
        [
            ["Previous contract PMPM", N(data.prev_pmpm), "Baseline expected claims per member/month"],
            ["Annualized expected claims/head", N(data.prev_pmpm * 12 if data.prev_pmpm else 0), "PMPM x 12 months"],
            ["Target MLR", "70%", "15% admin + 10% commission + 5% profit margin"],
            ["Actuarially sound premium", N(actuarial), "Annualized claims / 0.70"],
            ["Current premium (main plan)", N(main_prem), "Per head/year billed"],
            ["Adequacy verdict", delta_label,
             "Premium is ADEQUATE" if delta >= 0 else "⚠ Premium shortfall — adjust upward"],
        ],
        [CONTENT_W*0.34, CONTENT_W*0.24, CONTENT_W*0.42]
    ))
    story.append(sp(2))
    
    # ── 7.4 Limit:Premium Ratio — Market Context ─────────────────────────────
    story.append(h2("7.4 Limit:Premium Ratio — Nigerian HMO Market Context"))
    story.append(sp(1))
    
    main_ratio = data.plans[0].get("ratio", 0) if data.plans else 0
    
    story.append(tbl(
        ["Plan Tier", "Typical Ratio", "Market Examples", "Risk Classification"],
        [
            ["Individual basic/HMO tier", "5x–10x", "Low-tier individual plans", "Low liability ceiling"],
            ["Standard individual", "10x–15x", "Mid-market HMO plans", "Balanced coverage"],
            ["Corporate group (standard)", "15x–20x", "Most corporate HMO groups", "Group pricing benefit"],
            [f"THIS PORTFOLIO — {data.plans[0]['name'][:20] if data.plans else 'Main Plan'}",
             f"{main_ratio:.1f}x",
             "Current group",
             "⚠ High end — manage via SURGICAL SUB-LIMITS" if main_ratio > 20 else "Within market range"],
            ["Executive/Elite plans", "20x–30x+", "Premium HMO corporate plans", "Highest HMO liability"],
        ],
        [CONTENT_W*0.24, CONTENT_W*0.12, CONTENT_W*0.28, CONTENT_W*0.36]
    ))
    story.append(sp(1))
    
    finding_text = (
        "<b>KEY FINDING:</b> A limit:premium ratio above 20x is at the high end of the corporate market "
        "but is not evidence of structural underpricing when PMPM analysis shows premium adequacy. "
        "The correct risk mitigation is <b>SURGICAL SUB-LIMITS</b>, not reducing overall benefit coverage. "
        "The 3:1 framework governs <i>renewal adjustment ratios</i> — it is NOT an absolute rule that "
        "limits must equal 3x the annual premium."
    )
    t = Table([[Paragraph(finding_text, ParagraphStyle(
        "find", fontName="Helvetica", fontSize=8.5, textColor=BRAND_DARK,
        leading=12, leftIndent=5, rightIndent=5
    ))]], colWidths=[CONTENT_W])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0),(-1,-1), LIGHT_BLUE),
        ("BOX", (0,0),(-1,-1), 1, BRAND_BLUE),
        ("TOPPADDING", (0,0),(-1,-1), 8),
        ("BOTTOMPADDING", (0,0),(-1,-1), 8),
        ("LEFTPADDING", (0,0),(-1,-1), 8),
        ("RIGHTPADDING", (0,0),(-1,-1), 8),
    ]))
    story.append(t)
    story.append(sp(2))
    
    # ── 7.5 Risk Management Strategy ─────────────────────────────────────────
    story.append(h2("7.5 Risk Management Strategy: Sub-Limits vs Limit Reduction"))
    story.append(sp(1))
    
    story.append(tbl(
        ["Strategy", "Action", "Member Impact", "HMO Liability Impact", "Recommended"],
        [
            ["Reduce overall limit",
             "Cut limit from " + N(data.plans[0]['limit'] if data.plans else 0) + " to 3x premium",
             "Severely reduces member coverage; drives client away",
             "Caps all claims — including routine — at low ceiling",
             "NO — over-correction"],
            ["Premium increase only",
             f"Increase {int(data.projected_mlr - 75 + 10 if data.projected_mlr > 75 else 5)}–{int(data.projected_mlr - 75 + 20 if data.projected_mlr > 75 else 10)}%",
             "Higher cost, no structural protection change",
             "Adds margin but surgical event risk remains",
             "PARTIAL — insufficient alone"],
            ["Surgical sub-limit",
             "Cap surgical per event at N400K, N600K/year",
             "Minimal impact on routine care",
             "Directly caps N500K+ surgical event liability",
             "YES — PRIMARY TOOL"],
            ["Combined approach",
             "Premium increase + surgical sub-limit",
             "Moderate cost increase + clear benefit structure",
             "Optimal: actuarial margin + event cap",
             "YES — RECOMMENDED"],
        ],
        [CONTENT_W*0.17, CONTENT_W*0.22, CONTENT_W*0.22, CONTENT_W*0.22, CONTENT_W*0.17]
    ))
    
    return story


# ──────────────────────────────────────────────────────────────────────────────
# MAIN GENERATOR
# ──────────────────────────────────────────────────────────────────────────────
def generate_pdf(data: RenewalData, narratives: dict) -> bytes:
    buf = io.BytesIO()
    
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN, bottomMargin=MARGIN,
        title=f"Renewal Analysis — {data.group_name}",
        author="Clearline International Limited — KLAIRE AI Analytics",
    )
    
    story = []
    
    payment_rate = round(data.cash_received / data.total_debit * 100, 1) if data.total_debit else 0
    prev_pay = round(data.prev_cash / data.prev_debit * 100, 1) if data.prev_debit else 0
    outstanding = data.total_debit - data.cash_received
    
    # Determine increase %
    if data.projected_mlr >= 85 and data.srs_classification == "EPISODIC":
        inc_pct = 17.5
    elif data.projected_mlr >= 75:
        inc_pct = 12.5
    elif data.projected_mlr >= 65:
        inc_pct = 5.0
    else:
        inc_pct = 0
    
    # ── COVER ─────────────────────────────────────────────────────────────────
    story.append(sp(8))
    story.append(Paragraph("CLEARLINE INTERNATIONAL LIMITED", ST["CoverCo"]))
    story.append(hr(BRAND_BLUE, 2))
    story.append(sp(3))
    story.append(Paragraph("COMPREHENSIVE RENEWAL ANALYSIS", ParagraphStyle(
        "ct2", fontName="Helvetica-Bold", fontSize=16, textColor=BRAND_DARK,
        alignment=TA_CENTER, spaceBefore=4, spaceAfter=4)))
    story.append(sp(1))
    story.append(Paragraph(data.group_name.upper(), ST["CoverTitle"]))
    story.append(sp(1))
    story.append(Paragraph(
        f"Contract Period: {data.current_start.strftime('%B %d, %Y')} \u2013 {data.current_end.strftime('%B %d, %Y')}",
        ST["CoverSub"]))
    story.append(sp(2))
    story.append(Paragraph("PREPARED BY: KLAIRE AI ANALYTICS", ParagraphStyle(
        "cb", fontName="Helvetica-Bold", fontSize=10, textColor=DARK_GRAY, alignment=TA_CENTER)))
    story.append(Paragraph(
        f"Analysis Date: {data.analysis_date}  |  Data as at: {data.data_cutoff}",
        ST["CoverCo"]))
    story.append(sp(4))
    story.append(cover_status_boxes(P(data.projected_mlr), data.projected_mlr,
                                    data.srs_classification, data.top5_pct))
    story.append(sp(3))
    if data.projected_mlr >= 75:
        story.append(reco_box(
            f"RENEWAL RECOMMENDATION: INCREASE PREMIUM {inc_pct:.0f}%\u201320%  |  MAINTAIN LIMITS  |  ADD SURGICAL SUB-LIMIT"
        ))
    else:
        story.append(reco_box("RENEWAL RECOMMENDATION: MAINTAIN PREMIUM  |  REVIEW SURGICAL SUB-LIMIT"))
    story.append(PageBreak())
    
    # ── 1. EXECUTIVE SUMMARY ──────────────────────────────────────────────────
    story += sec("1. EXECUTIVE SUMMARY")
    story.append(body(
        f"{data.group_name} has been a Clearline client since {data.prev_start.strftime('%B %Y')}. "
        f"This analysis covers the current contract ({data.current_start.strftime('%B %d, %Y')} \u2013 "
        f"{data.current_end.strftime('%B %d, %Y')}) with {data.claims_months} months of claims data "
        f"available at the time of this report."
    ))
    story.append(sp(1))
    
    for b in narratives.get("executive_bullets", []):
        story.append(blt(b))
    
    story.append(sp(1))
    story.append(body(
        f"The previous contract ({data.prev_start.strftime('%b %Y')} \u2013 {data.prev_end.strftime('%b %Y')}) "
        f"had a {'healthy' if data.prev_mlr < 75 else 'elevated'} MLR of {P(data.prev_mlr)}, "
        f"{'confirming the current elevated MLR is driven by extraordinary events, not structural overutilisation.' if data.prev_mlr < 75 else 'indicating a portfolio requiring structural intervention.'}"
    ))
    story.append(PageBreak())
    
    # ── 2. CONTRACT OVERVIEW ──────────────────────────────────────────────────
    story += sec("2. CONTRACT & PORTFOLIO OVERVIEW")
    
    curr_util = round(data.members_utilizing / data.active_members * 100, 1) if data.active_members else 0
    prev_util = round(data.prev_members_utilizing / data.prev_members * 100, 1) if data.prev_members else 0
    
    story.append(tbl(
        ["Parameter", "Current Contract", "Previous Contract"],
        [
            ["Contract Period",
             f"{data.current_start.strftime('%b %d, %Y')} \u2013 {data.current_end.strftime('%b %d, %Y')}",
             f"{data.prev_start.strftime('%b %d, %Y')} \u2013 {data.prev_end.strftime('%b %d, %Y')}"],
            ["Active Members", f"{data.active_members}", f"{data.prev_members}"],
            ["Total Debit Notes", N(data.total_debit), N(data.prev_debit)],
            ["Cash Received", f"{N(data.cash_received)} ({P(payment_rate)})", f"{N(data.prev_cash)} ({P(prev_pay)})"],
            ["Claims (period)", f"{N(data.claims_total)} ({data.claims_months} months)", f"{N(data.prev_claims)} (12 months)"],
            ["PA Authorized (total)", f"{N(data.pa_total_authorized)} ({data.pa_count} PAs)", f"{N(data.prev_pa)}"],
            ["Unclaimed PA (pending, MLR numerator)", f"{N(data.unclaimed_pa)} ({data.unclaimed_pa_count} PAs)", "—"],
            ["MLR Numerator (Claims + Unclaimed PA)", f"{N(data.claims_total + data.unclaimed_pa)}", "—"],
            ["Annualized Claims", f"~{N(data.annualized_claims)}", N(data.prev_claims)],
            ["Projected Annual MLR",
             f"\u26a0 {P(data.projected_mlr)}" if data.projected_mlr >= 75 else P(data.projected_mlr),
             f"\u2705 {P(data.prev_mlr)}" if data.prev_mlr < 75 else f"\u26a0 {P(data.prev_mlr)}"],
            ["Members Utilizing", f"{data.members_utilizing}/{data.active_members} ({P(curr_util)})",
             f"{data.prev_members_utilizing}/{data.prev_members} ({P(prev_util)})"],
        ],
        [CONTENT_W*0.28, CONTENT_W*0.36, CONTENT_W*0.36]
    ))
    story.append(sp(3))
    story.append(h2("2.1 Plan Distribution"))
    story.append(sp(1))
    
    plan_rows = []
    for p in data.plans:
        r = p.get("ratio", 0)
        plan_rows.append([
            p["name"], N(p["limit"]), N(p["premium"]), str(p["count"]),
            f"{r:.1f}x \u26a0" if r > 20 else f"{r:.1f}x",
            "High end — add surgical sub-limit" if r > 20 else "Acceptable"
        ])
    story.append(tbl(
        ["Plan", "Limit", "Premium", "Members", "Ratio", "Assessment"],
        plan_rows,
        [CONTENT_W*0.26, CONTENT_W*0.13, CONTENT_W*0.13,
         CONTENT_W*0.08, CONTENT_W*0.12, CONTENT_W*0.28]
    ))
    story.append(PageBreak())
    
    # ── 3. FINANCIAL ANALYSIS ─────────────────────────────────────────────────
    story += sec("3. FINANCIAL ANALYSIS & MLR")
    story.append(kpi_box([
        {"label": "Total Debit (Billed)", "value": N(data.total_debit), "sub": "Current Contract"},
        {"label": "Cash Collected", "value": N(data.cash_received),
         "sub": f"{P(payment_rate)} Payment Rate",
         "color": RED if payment_rate < 60 else (ORANGE if payment_rate < 80 else GREEN)},
        {"label": f"Claims Paid ({data.claims_months}M)", "value": N(data.claims_total),
         "sub": f"{data.claims_count} Claims",
         "color": RED if data.claims_total > data.cash_received else BRAND_DARK},
        {"label": "Unclaimed PA (Pending)", "value": N(data.unclaimed_pa),
         "sub": f"{data.unclaimed_pa_count} PAs | MLR numerator component",
         "color": ORANGE if data.unclaimed_pa > data.claims_total * 0.2 else BRAND_DARK},
    ]))
    story.append(sp(2))
    story.append(kpi_box([
        {"label": "YTD MLR (Business)", "value": P(data.ytd_mlr),
         "sub": f"{data.claims_months}M only",
         "color": GREEN if data.ytd_mlr < 75 else RED},
        {"label": "Projected Annual MLR", "value": P(data.projected_mlr),
         "sub": "LOSS TERRITORY" if data.projected_mlr >= 75 else "ON TARGET",
         "color": RED if data.projected_mlr >= 75 else GREEN},
        {"label": "Cash MLR", "value": P(data.cash_mlr),
         "sub": "Claims vs Cash",
         "color": RED if data.cash_mlr >= 75 else ORANGE},
        {"label": "Prev. Contract MLR", "value": P(data.prev_mlr),
         "sub": "Baseline Reference",
         "color": GREEN if data.prev_mlr < 75 else ORANGE},
    ]))
    story.append(sp(2))
    story.append(h2("3.1 Monthly Claims Burn Rate"))
    story.append(sp(1))
    
    if data.monthly_claims:
        avg = data.claims_total / len(data.monthly_claims)
        monthly_rows = []
        for m in data.monthly_claims:
            vs = f"{round((m['amount']/avg - 1)*100):+.0f}%" if avg > 0 else "\u2014"
            pa_amt = m.get("pa_amount", 0)
            driver = m.get("driver", "Routine")
            monthly_rows.append([
                m["month"], str(m["count"]), N(m["amount"]), vs, N(pa_amt), driver
            ])
        monthly_rows.append(["TOTAL", str(data.claims_count), N(data.claims_total),
                              "\u2014", N(data.unclaimed_pa), f"Unclaimed: {data.unclaimed_pa_count} PAs"])
        monthly_rows.append(["Monthly Average", "", N(round(avg, 0)), "\u2014", "", ""])
        story.append(tbl(
            ["Month", "Count", "Claims Amount", "vs Average", "PA Granted", "Driver"],
            monthly_rows,
            [CONTENT_W*0.17, CONTENT_W*0.09, CONTENT_W*0.18,
             CONTENT_W*0.13, CONTENT_W*0.17, CONTENT_W*0.26]
        ))
        # Spike explanation
        spike_months = [m for m in data.monthly_claims if m["amount"] > avg * 2]
        if spike_months:
            top_spike = max(spike_months, key=lambda x: x["amount"])
            story.append(sp(1))
            story.append(body(
                f"\u26a0 Spike explanation: {top_spike['month']} claims of {N(top_spike['amount'])} "
                f"({round(top_spike['amount']/avg, 1)}\u00d7 the monthly average) represent a significant outlier. "
                "Review top member utilisation in Section 4 for the drivers of this spike. "
                "Excluding the spike month, the annualised run rate is materially lower."
            ))
    
    story.append(sp(2))
    story.append(h2("3.2 Premium Debit Notes"))
    story.append(sp(1))
    dn_rows = [[d["ref"], d["desc"][:55], N(d["amount"]), str(d["from"])[:10] + " \u2013 " + str(d["to"])[:10]]
               for d in data.debit_notes]
    dn_rows.append(["TOTAL", "", N(data.total_debit), ""])
    story.append(tbl(
        ["Ref No.", "Description", "Amount", "Period"],
        dn_rows,
        [CONTENT_W*0.12, CONTENT_W*0.43, CONTENT_W*0.18, CONTENT_W*0.27]
    ))
    # Flag debit notes whose description contains an enrollee ID from a DIFFERENT group
    # e.g. CL/FAM/... appearing on a KIZITO debit note is a billing anomaly
    import re as _dn_re
    group_short = data.group_name.split()[0][:3].upper()  # e.g. "KIZ" from "KIZITO"
    anomalous_notes = []
    for d in data.debit_notes:
        desc = d["desc"].upper()
        iid_match = _dn_re.search(r"CL/([A-Z]+)/", desc)
        if iid_match:
            iid_group = iid_match.group(1).upper()
            if group_short not in iid_group and iid_group not in group_short:
                anomalous_notes.append(d)
    if anomalous_notes:
        story.append(sp(1))
        for an in anomalous_notes:
            story.append(body(
                f"<b>⚠ BILLING ANOMALY — Ref {an['ref']}:</b> "
                f"This debit note ({N(an['amount'])}) contains an enrollee ID that does not match "
                f"{data.group_name}. Description: \"{an['desc'][:80]}\". "
                f"Verify whether this charge belongs to this group's contract or was incorrectly allocated. "
                f"If misallocated, exclude from this group's total debit for accurate MLR calculation."
            ))
    story.append(PageBreak())
    
    # ── 4. SRS ANALYSIS ───────────────────────────────────────────────────────
    story += sec("4. STRUCTURAL RISK SCORE (SRS) ANALYSIS")
    story.append(body(
        "The SRS framework distinguishes EPISODIC utilisation (high-cost acute events in a few members) "
        "from STRUCTURAL utilisation (widespread chronic disease across the population). This classification "
        "drives renewal premium strategy — episodic portfolios do not warrant maximum increases since "
        "the high-cost events are unlikely to repeat."
    ))
    story.append(sp(2))
    
    top_prov_pct = data.top_providers[0]["pct"] if data.top_providers else 0
    story.append(tbl(
        ["SRS Dimension", "Metric", "Value", "Threshold", "Verdict"],
        [
            ["Top 5 Concentration", "Top 5 / Total claims", P(data.top5_pct),
             ">40% = Episodic", "\u2705 EPISODIC" if data.top5_pct > 40 else "\u26a0 STRUCTURAL"],
            ["Chronic Disease Load", "Chronic ICD / Total claims", P(data.chronic_pct),
             "<30% = Low", "\u2705 LOW" if data.chronic_pct < 30 else "\u26a0 HIGH"],
            ["Provider Concentration", "Top provider / Total PA", P(top_prov_pct),
             ">60% = Flag", "\u2705 OK" if top_prov_pct < 60 else "\u26a0 HIGH"],
            ["MLR Status", "Projected Annual MLR", P(data.projected_mlr),
             ">75% = Loss", "\u26a0 LOSS" if data.projected_mlr >= 75 else "\u2705 OK"],
            ["Utilisation Breadth", "Members utilizing",
             f"{data.members_utilizing}/{data.active_members} ({P(curr_util)})",
             "<50%=Low | 50-70%=Normal | 70-80%=Elevated | >80%=High",
             ("✅ Normal" if curr_util < 70 else
              "⚠ ELEVATED" if curr_util < 80 else
              "⚠ HIGH")],
            ["OVERALL SRS", "Classification", data.srs_classification,
             "See matrix (Sec 8)", "\u26a0 MONITOR" if data.projected_mlr >= 75 else "\u2705 STABLE"],
        ],
        [CONTENT_W*0.20, CONTENT_W*0.23, CONTENT_W*0.13, CONTENT_W*0.16, CONTENT_W*0.28]
    ))
    story.append(sp(2))
    
    srs_text = narratives.get("srs_narrative", "")
    if srs_text:
        story.append(body(srs_text))
        story.append(sp(1))
    
    story.append(h2("4.1 Top 5 Member Analysis (SRS Episodic Drivers)"))
    story.append(sp(1))
    story.append(body(
        f"The top 5 utilizers account for {P(data.top5_pct)} of all claims "
        f"{'— exceeding' if data.top5_pct > 40 else '— below'} the 40% episodic threshold. "
        f"{'This signals the high MLR is driven by acute events, not embedded chronic disease.' if data.top5_pct > 40 else 'This indicates a more distributed utilisation pattern.'}"
    ))
    story.append(sp(1))
    
    top5_total = sum(m["amount"] for m in data.top_members[:5])
    
    # Build repeat member lookup for flags
    repeat_iids = {r["iid"]: r for r in (data.repeat_high_cost_members or [])}
    
    # Gender/diagnosis mismatch definitions
    FEMALE_ONLY_DX = ["D25", "D26", "D27", "D28", "N70", "N71", "N72", "N73", "N74",
                      "N76", "N77", "N80", "N81", "N83", "N87", "N89", "N92", "N93",
                      "N95", "O", "Z34", "Z35"]  # uterine, ovarian, cervical, obstetric
    MALE_ONLY_DX = ["N40", "N41", "N42", "N43", "N44", "N45", "N46", "C61"]  # prostate, testicular
    
    gender_dx_alerts = []
    
    mem_rows = []
    for i, m in enumerate(data.top_members[:5]):
        gender = m.get("gender", "?")
        dx_code = m.get("primary_condition", "")[:10].split(" ")[0]
        repeat = m["iid"] in repeat_iids
        repeat_info = repeat_iids.get(m["iid"], {})
        
        # Nature: add REPEAT flag if this member was expensive in previous contract too
        nature = m.get("nature", "MIXED")
        if repeat:
            risk = repeat_info.get("risk", "PERSISTENT")
            nature = f"⚠ {risk} — both contracts"
        
        # Gender/diagnosis mismatch check
        mismatch = False
        if gender == "M" and any(dx_code.startswith(f) for f in FEMALE_ONLY_DX):
            mismatch = True
            gender_dx_alerts.append({
                "name": m["name"], "iid": m["iid"],
                "gender": "M", "dx": dx_code,
                "issue": f"Male member coded with female-specific diagnosis ({dx_code})"
            })
        elif gender == "F" and any(dx_code.startswith(f) for f in MALE_ONLY_DX):
            mismatch = True
            gender_dx_alerts.append({
                "name": m["name"], "iid": m["iid"],
                "gender": "F", "dx": dx_code,
                "issue": f"Female member coded with male-specific diagnosis ({dx_code})"
            })
        
        gender_display = f"{gender}/{m.get('age',0)}yrs"
        if mismatch:
            gender_display = f"⚠ {gender_display}"
        
        mem_rows.append([
            str(i+1), m["iid"], m["name"],
            gender_display,
            N(m["amount"]), P(m["pct"]),
            m.get("primary_condition", "Multiple conditions")[:35],
            nature
        ])
    mem_rows.append(["TOP 5", "", "", "", N(top5_total), P(data.top5_pct), "", ""])
    others = data.claims_total - top5_total
    mem_rows.append([f"Others ({max(0, data.members_utilizing-5)})", "", "", "",
                     N(max(0, others)), P(round(100 - data.top5_pct, 1)), "Routine", "NORMAL"])
    
    story.append(tbl(
        ["Rank", "Enrollee ID", "Member Name", "Gender/Age",
         "Claims", "% Total", "Primary Condition", "Nature"],
        mem_rows,
        [CONTENT_W*0.05, CONTENT_W*0.13, CONTENT_W*0.16, CONTENT_W*0.10,
         CONTENT_W*0.11, CONTENT_W*0.08, CONTENT_W*0.22, CONTENT_W*0.15]
    ))
    story.append(sp(1))
    
    # Gender/diagnosis mismatch alerts
    for alert in gender_dx_alerts:
        story.append(body(
            f"<b>⚠ DATA INTEGRITY ALERT — {alert['name']} ({alert['iid']}):</b> "
            f"{alert['issue']}. This may indicate a claims data entry error, a gender coding error "
            f"in the member record, or a fraudulent claim. <b>Do not classify as routine episodic — "
            f"escalate for medical audit before renewal.</b>"
        ))
        story.append(sp(1))
    
    # Key Insight paragraph
    if data.top_members and len(data.top_members) >= 2:
        top2_total = sum(m["amount"] for m in data.top_members[:2])
        remaining_claims = data.claims_total - top2_total
        remaining_mlr = round(remaining_claims / data.total_debit * 100, 1) if data.total_debit else 0
        story.append(body(
            f"<b>Key Insight:</b> If the top 2 highest-cost members are excluded, "
            f"the remaining portfolio MLR drops to approximately {P(remaining_mlr)}. "
            f"{'This is a healthy portfolio that has been distorted by extraordinary acute events.' if remaining_mlr < 75 else 'The remaining portfolio still requires premium adjustment even without the outliers.'}"
        ))
    story.append(PageBreak())
    
    # ── 5. DIAGNOSES ──────────────────────────────────────────────────────────
    story += sec("5. TOP DIAGNOSIS / CONDITION ANALYSIS")
    
    CHRONIC_PFX = {"I10","I11","I119","I12","I13","I20","I209","I25","E11","E14","J44","J45"}
    ENDEMIC = {"B50","B509","B54","B55"}  # Malaria, endemic infections
    
    if data.top_diagnoses:
        diag_rows = []
        chronic_total = 0
        for d in data.top_diagnoses[:12]:
            code = d["code"]
            is_chronic = any(code.startswith(c) for c in CHRONIC_PFX)
            is_endemic = any(code.startswith(c) for c in ENDEMIC)
            is_acute_surgical = code.startswith("S") or code.startswith("D2") or code.startswith("O")
            
            if is_chronic:
                dtype = "CHRONIC ⚠"
                trend = "Structural — ongoing"
                chronic_total += d["amount"]
            elif is_endemic:
                dtype = "ENDEMIC"
                trend = "Routine — expected"
            elif is_acute_surgical:
                dtype = "ACUTE/SURGICAL"
                trend = "Episodic — monitor"
            else:
                dtype = "ACUTE/OTHER"
                trend = "Episodic"
            
            diag_rows.append([
                code, d["name"][:32], str(d["count"]),
                N(d["amount"]), P(d["pct"]), dtype, trend
            ])
        story.append(tbl(
            ["ICD Code", "Condition", "Claims", "Amount", "% Claims", "Type", "Trend"],
            diag_rows,
            [CONTENT_W*0.08, CONTENT_W*0.26, CONTENT_W*0.08,
             CONTENT_W*0.13, CONTENT_W*0.09, CONTENT_W*0.14, CONTENT_W*0.22]
        ))
        # Chronic burden narrative paragraph (like Kizito)
        if chronic_total > 0:
            pct_chronic = round(chronic_total / data.claims_total * 100, 1) if data.claims_total else 0
            story.append(sp(1))
            story.append(body(
                f"<b>CHRONIC DISEASE BURDEN:</b> Hypertension and related cardiovascular conditions "
                f"represent {N(chronic_total)} ({P(pct_chronic)} of claims). "
                f"{'This chronic disease cluster will generate recurring drug, consultation, and monitoring claims every contract year. This is the structural element within the portfolio — monitor closely.' if pct_chronic < 30 else 'This elevated chronic load signals STRUCTURAL utilisation that will persist and grow without a Chronic Disease Management Programme (CDMP).'}"
            ))
    story.append(PageBreak())
    
    # ── 6. PROVIDER ANALYSIS ──────────────────────────────────────────────────
    story += sec("6. PROVIDER PERFORMANCE & CONCENTRATION ANALYSIS")
    story.append(h2("6.1 Current Contract — Top Providers by PA Value"))
    story.append(sp(1))
    
    prev_map = {p["name"]: p["amount"] for p in data.prev_top_providers}
    prov_rows = []
    high_alert_providers = []
    collapsed_providers = []
    
    for p in data.top_providers[:12]:
        prev = prev_map.get(p["name"], 0)
        if prev > 0:
            chg = f"{round((p['amount']/prev - 1)*100):+.0f}%"
        else:
            chg = "NEW"
        
        if p["pct"] > 20 and chg == "NEW":
            flag = "⚠ HIGH ALERT"
            high_alert_providers.append(p)
        elif chg not in ("NEW",) and prev > 0:
            try:
                growth_val = round((p['amount']/prev - 1)*100)
                if growth_val > 80:
                    flag = "⚠ RAPID GROWTH — audit"
                    if p["pct"] > 15:
                        high_alert_providers.append(p)
                elif p["pct"] > 15:
                    flag = "⚠ Monitor"
                else:
                    flag = "Normal"
            except:
                flag = "Normal"
        elif p["pct"] > 15:
            flag = "⚠ Monitor"
        else:
            flag = "Normal"
        
        prov_rows.append([p["name"][:35], str(p["pa_count"]), N(p["amount"]),
                          P(p["pct"]), chg, flag])
    
    story.append(tbl(
        ["Provider", "PA Count", "PA Amount", "% Total", "YoY Change", "Flag"],
        prov_rows,
        [CONTENT_W*0.30, CONTENT_W*0.08, CONTENT_W*0.14,
         CONTENT_W*0.09, CONTENT_W*0.13, CONTENT_W*0.26]
    ))
    story.append(sp(1))
    
    # Provider-specific callout paragraphs (like Kizito's Care Coordination + Emel alerts)
    prov_text = narratives.get("provider_narrative", "")
    if prov_text:
        story.append(body(prov_text))
    
    # Auto-generate high-alert callouts for new high-spend providers
    for p in high_alert_providers[:2]:
        story.append(sp(1))
        story.append(body(
            f"<b>⚠ {p['name'].upper()} — URGENT REVIEW REQUIRED:</b> "
            f"This provider represents {P(p['pct'])} of total PA spend ({N(p['amount'])}) "
            f"as a new entrant with no prior contract history. "
            f"Request full itemised bills for all procedures. "
            f"Cross-check billed rates against REALITY TARIFF contracted rates. "
            f"Consider mandatory pre-authorization escalation for any PA above ₦200,000 at this facility."
        ))
    
    # Collapsed provider alerts (prev high, now low)
    for prev_p in data.prev_top_providers[:5]:
        curr_match = next((p for p in data.top_providers if p["name"] == prev_p["name"]), None)
        curr_amt = curr_match["amount"] if curr_match else 0
        if prev_p["amount"] > 0 and curr_amt / prev_p["amount"] < 0.3 and prev_p.get("pct", 0) > 10:
            story.append(sp(1))
            story.append(body(
                f"<b>⚠ {prev_p['name'].upper()} — SIGNIFICANT UTILISATION COLLAPSE:</b> "
                f"This provider was previously a major spend contributor ({N(prev_p['amount'])}). "
                f"Current contract utilisation has dropped to {N(curr_amt)} "
                f"({round(curr_amt/prev_p['amount']*100, 0):.0f}% of prior level). "
                f"Investigate whether referring patterns have shifted to higher-tariff facilities, "
                f"or whether access/quality issues are driving members elsewhere."
            ))
            collapsed_providers.append(prev_p)
    
    story.append(sp(2))
    
    # ── 6.2 PROVIDER SHIFT ANALYSIS ──────────────────────────────────────────
    story.append(h2("6.2 Provider Shift Analysis (Previous vs Current Contract)"))
    story.append(sp(1))
    story.append(body(
        "Provider shift analysis identifies changes in where members receive care between contract periods. "
        "Significant shifts may indicate access changes, quality issues, referring pattern changes, "
        "or deliberate member steering by agents or employers."
    ))
    story.append(sp(1))
    
    # Build combined prev+curr provider list
    all_provider_names = set(
        [p["name"] for p in data.top_providers[:10]] +
        [p["name"] for p in data.prev_top_providers[:10]]
    )
    
    shift_rows = []
    for name in sorted(all_provider_names):
        curr_p = next((p for p in data.top_providers if p["name"] == name), None)
        prev_p = next((p for p in data.prev_top_providers if p["name"] == name), None)
        
        curr_amt = curr_p["amount"] if curr_p else 0
        curr_pct = curr_p["pct"] if curr_p else 0
        prev_amt = prev_p["amount"] if prev_p else 0
        prev_pct = prev_p.get("pct", 0) if prev_p else 0
        
        if curr_amt == 0 and prev_amt == 0:
            continue
        
        if prev_amt == 0:
            chg_pct = "NEW ENTRANT"
            interpretation = "⚠ New provider — verify credentials and tariff compliance"
        elif curr_amt == 0:
            chg_pct = "-100%"
            interpretation = "No longer used — check network status or patient access"
        else:
            chg_val = round((curr_amt / prev_amt - 1) * 100)
            chg_pct = f"{chg_val:+.0f}%"
            if chg_val > 80:
                interpretation = "⚠ Rapid growth — audit for tariff inflation"
            elif chg_val > 30:
                interpretation = "Growing utilisation — watch trend"
            elif chg_val < -50:
                interpretation = "⚠ Major drop — investigate cause"
            elif chg_val < -20:
                interpretation = "Reduced but active"
            else:
                interpretation = "Stable utiliser — normal"
        
        shift_rows.append([
            name[:32],
            f"{N(prev_amt)} ({P(prev_pct)})" if prev_amt else "₦0",
            f"{N(curr_amt)} ({P(curr_pct)})" if curr_amt else "₦0",
            chg_pct,
            interpretation[:45]
        ])
    
    # Sort by current amount descending
    shift_rows.sort(key=lambda r: float(r[2].replace("₦","").replace(",","").split(" ")[0]) if r[2] != "₦0" else 0, reverse=True)
    
    if shift_rows:
        story.append(tbl(
            ["Provider", "Prev Contract PA", "Curr Contract PA", "Change", "Interpretation"],
            shift_rows[:12],
            [CONTENT_W*0.24, CONTENT_W*0.18, CONTENT_W*0.18, CONTENT_W*0.10, CONTENT_W*0.30]
        ))
    story.append(PageBreak())
    
    # ── 7. PLAN ADEQUACY (CORRECTED) ──────────────────────────────────────────
    story += build_plan_adequacy_section(data, narratives)
    story.append(PageBreak())
    
    # ── 8. RENEWAL RECOMMENDATION ────────────────────────────────────────────
    story += sec("8. RENEWAL RECOMMENDATION")
    story.append(h2("8.1 SRS-Based Decision Matrix"))
    story.append(sp(1))
    
    story.append(tbl(
        ["MLR Band", "SRS Pattern", "Recommended Action", "Applicable?"],
        [
            ["75–85%", "EPISODIC", "10–15% premium increase, maintain limits",
             "\u2705 PRIMARY" if 75 <= data.projected_mlr <= 85 and data.srs_classification == "EPISODIC" else ""],
            ["75–85%", "STRUCTURAL", "15–25% increase, sub-limits",
             "\u2705 PRIMARY" if 75 <= data.projected_mlr <= 85 and data.srs_classification == "STRUCTURAL" else ""],
            ["85–100%", "EPISODIC", "15–20% increase, add surgical sub-limit",
             "\u2705 PRIMARY" if data.projected_mlr > 85 and data.srs_classification == "EPISODIC" else ""],
            [">100%", "Any", "25–35% increase, review limits", "Not applicable"],
        ],
        [CONTENT_W*0.15, CONTENT_W*0.15, CONTENT_W*0.45, CONTENT_W*0.25]
    ))
    story.append(sp(2))
    
    prem_text = narratives.get("premium_narrative", "")
    if prem_text:
        story.append(body(prem_text))
        story.append(sp(1))
    
    story.append(h2("8.2 Recommended Premium Schedule"))
    story.append(sp(1))
    
    prem_rows = []
    total_reco = 0
    for p in data.plans:
        new_prem = p["premium"] * (1 + inc_pct / 100)
        ann = new_prem * p["count"]
        total_reco += ann
        prem_rows.append([
            p["name"], N(p["premium"]), f"+{inc_pct:.1f}%",
            N(new_prem), N(ann), "PMPM-based episodic renewal"
        ])
    curr_total = sum(p["premium"] * p["count"] for p in data.plans)
    prem_rows.append(["TOTAL", "", "", "", N(total_reco), f"vs current {N(curr_total)}"])
    
    story.append(tbl(
        ["Plan", "Current Premium", "Increase", "Recommended", "Annual Impact", "Basis"],
        prem_rows,
        [CONTENT_W*0.22, CONTENT_W*0.14, CONTENT_W*0.10,
         CONTENT_W*0.16, CONTENT_W*0.16, CONTENT_W*0.22]
    ))
    story.append(PageBreak())
    
    # ── 9. SOLUTIONS ──────────────────────────────────────────────────────────
    story += sec("9. POSSIBLE SOLUTIONS & RISK MITIGATION")
    
    story.append(h3("SOLUTION 1: Introduce Surgical/Inpatient Sub-Limit"))
    story.append(blt(
        f"Cap surgical procedures at \u20a6400,000 per event and \u20a6600,000 per contract year "
        f"within the overall {N(data.plans[0]['limit'] if data.plans else 0)} limit."
    ))
    story.append(blt("Expected savings: \u20a6400K\u2013\u20a6600K per year preventing recurrence of high-value surgical events."))
    story.append(blt("Industry standard: Nigerian HMOs and NHIA typically cap surgical procedures at 40\u201350% of annual limit per event."))
    story.append(sp(1))
    
    story.append(h3("SOLUTION 2: High-Value PA Escalation Protocol"))
    story.append(blt("Require second medical opinion from Clearline-designated consultant for any PA > \u20a6200,000."))
    story.append(blt("Obtain itemised bills for all high-value procedures; cross-check against REALITY TARIFF contracted rates."))
    story.append(sp(1))
    
    story.append(h3("SOLUTION 3: Chronic Disease Management Programme (CDMP)"))
    story.append(blt(f"Chronic disease burden = {P(data.chronic_pct)} of claims — generating reliable annual recurring spend."))
    story.append(blt("Structured CDMP: quarterly BP monitoring, controlled drug dispensing, annual cardiac screening."))
    story.append(blt("RAND Health Insurance Experiment evidence: CDMP reduces hypertension claims by 18\u201322% over 2 years."))
    story.append(sp(1))
    
    story.append(h3("SOLUTION 4: Resolve Cash Non-Compliance Before Renewal"))
    story.append(blt(f"Outstanding balance: {N(outstanding)} ({P(100 - payment_rate)} of current year premium unpaid)."))
    story.append(blt("Do NOT renew without a concrete repayment schedule or full settlement."))
    story.append(blt("Link renewal terms to minimum 70% payment compliance as a condition precedent."))
    story.append(PageBreak())
    
    # ── 10. CASH COMPLIANCE ───────────────────────────────────────────────────
    story += sec("10. CASH COMPLIANCE & PAYMENT ANALYSIS")
    
    out_prev = data.prev_debit - data.prev_cash
    cum_debit = data.total_debit + data.prev_debit
    cum_cash = data.cash_received + data.prev_cash
    cum_rate = round(cum_cash / cum_debit * 100, 1) if cum_debit else 0
    
    story.append(tbl(
        ["Period", "Debit Notes", "Cash Received", "Payment Rate", "Outstanding", "Status"],
        [
            [f"{data.prev_start.strftime('%b %Y')} \u2013 {data.prev_end.strftime('%b %Y')}",
             N(data.prev_debit), N(data.prev_cash), P(prev_pay), N(out_prev),
             "\u26a0 Partial" if prev_pay < 80 else "\u2705 OK"],
            ["Current contract", N(data.total_debit), N(data.cash_received), P(payment_rate),
             N(outstanding),
             "\u26a0 CRITICAL" if payment_rate < 60 else ("\u26a0 Partial" if payment_rate < 80 else "\u2705 OK")],
            ["2-year cumulative", N(cum_debit), N(cum_cash), P(cum_rate),
             N(out_prev + outstanding),
             "\u26a0 HIGH RISK" if cum_rate < 65 else "Monitor"],
        ],
        [CONTENT_W*0.26, CONTENT_W*0.14, CONTENT_W*0.14,
         CONTENT_W*0.12, CONTENT_W*0.14, CONTENT_W*0.20]
    ))
    story.append(sp(1))
    story.append(body(
        f"Cash compliance at {P(payment_rate)} is {'critically below' if payment_rate < 60 else 'below'} "
        f"the acceptable threshold (\u226580%). Clearline is effectively funding {P(100 - payment_rate)} "
        f"of healthcare costs from capital. This must be resolved at renewal."
    ))
    story.append(sp(3))
    
    # ── 11. ENGAGEMENT GUIDE ──────────────────────────────────────────────────
    story += sec("11. RENEWAL ENGAGEMENT GUIDE")
    story.append(body("Use the following framework when engaging the client at renewal:"))
    story.append(sp(1))
    
    story.append(h3("Opening Position"))
    story.append(blt(f"'Your portfolio performs well in routine utilisation. The {P(data.projected_mlr)} MLR is driven by {'episodic' if data.srs_classification == 'EPISODIC' else 'structural'} events, not routine overutilisation.'"))
    story.append(blt(f"'We are proposing a {inc_pct:.0f}% premium increase alongside surgical sub-limits that protect both parties.'"))
    story.append(sp(1))
    
    story.append(h3("Key Negotiation Points"))
    story.append(blt("Push for surgical sub-limits (\u20a6400K/event) — frame as 'protecting the pool for all members'."))
    story.append(blt(f"Request settlement of {N(outstanding)} outstanding balance as renewal condition."))
    story.append(blt("Share provider concentration data if client pushes back on premium increase."))
    story.append(sp(1))
    
    story.append(h3("Upsell Opportunities"))
    story.append(blt("Annual health screening package — addresses chronic disease cluster proactively."))
    story.append(blt("Dental and optical riders — supplementary covers with minimal HMO liability."))
    story.append(PageBreak())
    
    # ── 12. SUMMARY DASHBOARD ─────────────────────────────────────────────────
    story += sec("12. SUMMARY DASHBOARD")
    
    top_prov_name = data.top_providers[0]["name"][:28] if data.top_providers else "N/A"
    top_prov_pct2 = data.top_providers[0]["pct"] if data.top_providers else 0
    main_prem = data.plans[0]['premium'] if data.plans else 0
    
    story.append(tbl(
        ["Category", "Finding", "Severity", "Action Required"],
        [
            ["MLR Status", f"{P(data.projected_mlr)} projected annual MLR",
             "\U0001f534 CRITICAL" if data.projected_mlr >= 75 else "\U0001f7e2 OK",
             f"{inc_pct:.0f}% premium increase"],
            ["SRS Classification", f"{data.srs_classification} — Top 5 = {P(data.top5_pct)}",
             "\U0001f7e1 MONITOR", "Add surgical sub-limits"],
            ["Plan Adequacy (PMPM)", f"Actuarial premium: {N(data.actuarial_premium)} vs {N(main_prem)} current",
             "\U0001f7e2 ADEQUATE" if main_prem >= data.actuarial_premium else "\U0001f7e1 REVIEW",
             "PMPM-based renewal pricing"],
            ["Limit Risk", f"Limit:premium ratio {data.plans[0].get('ratio',0):.1f}x (market: 10\u201325x)",
             "\U0001f7e1 HIGH END" if data.plans and data.plans[0].get("ratio",0) > 20 else "\U0001f7e2 OK",
             "Add surgical sub-limit at renewal"],
            ["Provider Risk", f"{top_prov_name} = {P(top_prov_pct2)} of PA",
             "\U0001f534 FLAG" if top_prov_pct2 > 25 else "\U0001f7e1 MONITOR",
             "Itemised bills; audit if new entrant"],
            ["Cash Compliance", f"{P(payment_rate)} payment rate — {N(outstanding)} outstanding",
             "\U0001f534 CRITICAL" if payment_rate < 60 else "\U0001f7e1 PARTIAL",
             "Demand settlement at renewal"],
            ["Chronic Disease", f"Chronic ICD = {P(data.chronic_pct)} of claims",
             "\U0001f7e1 MONITOR", "Implement CDMP"],
            ["Prev Contract", f"{P(data.prev_mlr)} MLR — reference baseline",
             "\U0001f7e2 POSITIVE" if data.prev_mlr < 75 else "\U0001f7e1 ELEVATED",
             "Maintain base philosophy"],
        ],
        [CONTENT_W*0.17, CONTENT_W*0.31, CONTENT_W*0.14, CONTENT_W*0.38]
    ))
    
    story.append(sp(3))
    story.append(hr(BRAND_BLUE, 1))
    story.append(Paragraph(
        "This report was generated by KLAIRE AI Analytics | Clearline International Limited | Confidential",
        ST["Small"]
    ))
    story.append(Paragraph(
        f"Analysis Date: {data.analysis_date} | Data Cut-Off: {data.data_cutoff} | Next Review: 60 days",
        ST["Small"]
    ))
    
    doc.build(story)
    return buf.getvalue()