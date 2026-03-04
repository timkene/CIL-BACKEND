#!/usr/bin/env python3
"""
CLEARLINE AGENT REVIEW PORTAL
================================

Agent-side interface for reviewing pending PA requests:
- Pending queue with AI recommendations
- Full validation details per request
- Confirm AI (stores learning) or Override (no learning)
- Dashboard stats

Run: streamlit run agent_app.py --server.port 8502
Requires: vetting_api.py running on port 8000

Author: Casey's AI Assistant
Date: February 2026
"""

import streamlit as st
import requests
import pandas as pd
from datetime import datetime

API_URL = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Agent Review Portal", page_icon="🛡️", layout="wide")

# ============================================================================
# HELPERS
# ============================================================================

def api_call(method, endpoint, **kwargs):
    try:
        url = f"{API_URL}/{endpoint}"
        if method == "GET":
            resp = requests.get(url, params=kwargs.get('params', {}), timeout=120)
        else:
            resp = requests.post(url, json=kwargs.get('json', {}), timeout=120)
        if resp.status_code == 200:
            return resp.json(), None
        return None, f"API Error {resp.status_code}: {resp.text[:200]}"
    except requests.ConnectionError:
        return None, "❌ Cannot connect to API. Run: python vetting_api.py"
    except Exception as e:
        return None, f"❌ {str(e)}"

STATUS_DISPLAY = {
    "AUTO_APPROVED":  ("✅", "Auto-Approved"),
    "AUTO_DENIED":    ("❌", "Auto-Denied"),
    "PENDING_REVIEW": ("⏳", "Pending Review"),
    "HUMAN_APPROVED": ("✅", "Human Approved"),
    "HUMAN_DENIED":   ("❌", "Human Denied"),
}

# ============================================================================
# MAIN
# ============================================================================

st.title("🛡️ Clearline Agent Review Portal")
st.caption("Review AI-validated PA requests — confirm to build learning, override to correct")

health, err = api_call("GET", "health")
if err:
    st.error(err)
    st.stop()

# ============================================================================
# SIDEBAR: Pending count + guide
# ============================================================================

pending_info, _ = api_call("GET", "pending", params={"limit": 1})
pending_count = pending_info.get('total_pending', 0) if pending_info else 0

st.sidebar.title("🛡️ Agent Portal")
st.sidebar.markdown("---")

if pending_count > 0:
    st.sidebar.error(f"⏳ **{pending_count}** pending request(s)")
else:
    st.sidebar.success("✅ No pending requests")

st.sidebar.markdown("---")
st.sidebar.markdown("""
**Review Guide:**

**CONFIRM** = You agree with AI
→ Stores learning for future use
→ Future similar claims skip AI (saves money)

**OVERRIDE** = You disagree
→ NO learning stored
→ AI was wrong, we don't teach bad knowledge
""")
st.sidebar.markdown("---")
st.sidebar.markdown("""
**Decision States:**
- ✅ **Auto-Approved** — master/learning only
- ❌ **Auto-Denied** — master or learned (≥3x)
- ⏳ **Pending** — AI involved, needs you
- ✅ **Human Approved** — you confirmed
- ❌ **Human Denied** — you confirmed
""")

# ============================================================================
# TABS
# ============================================================================

tab_queue, tab_dash, tab_history = st.tabs(["📋 Pending Queue", "📊 Dashboard", "📜 All History"])

# ============================================================================
# TAB 1: PENDING REVIEW QUEUE
# ============================================================================

with tab_queue:
    pending_data, err = api_call("GET", "pending", params={"limit": 50})
    
    if err:
        st.error(err)
    else:
        total = pending_data.get('total_pending', 0)
        reqs = pending_data.get('requests', [])
        
        st.subheader(f"⏳ Pending Review Queue — {total} request{'s' if total != 1 else ''}")
        
        if not reqs:
            st.success("🎉 All clear — no pending requests!")
            st.balloons()
        else:
            # Summary bar
            approve_recs = sum(1 for r in reqs if r.get('ai_recommendation') == 'APPROVE')
            deny_recs = sum(1 for r in reqs if r.get('ai_recommendation') == 'DENY')
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Pending", total)
            col2.metric("🤖 AI Recommends APPROVE", approve_recs)
            col3.metric("🤖 AI Recommends DENY", deny_recs)
            
            st.markdown("---")
            
            # === REQUEST CARDS ===
            for idx, req in enumerate(reqs):
                ai_rec = req.get('ai_recommendation', '?')
                conf = req.get('confidence', 0)
                proc_name = req.get('procedure_name', req['procedure_code'])
                diag_name = req.get('diagnosis_name', req['diagnosis_code'])
                age = req.get('enrollee_age')
                gender = req.get('enrollee_gender', '')
                patient_info = f"{age}yo {gender}" if age else "Unknown"
                rid = req['request_id']
                
                # Color-coded card
                if ai_rec == "APPROVE":
                    card_icon = "🟢"
                else:
                    card_icon = "🔴"
                
                with st.expander(
                    f"{card_icon} **{proc_name}** + {diag_name} | "
                    f"{req['enrollee_id']} ({patient_info}) | "
                    f"AI: {ai_rec} ({conf}%)",
                    expanded=(idx == 0)  # First one expanded
                ):
                    # -------------------------------------------------------
                    # Fetch full details for this request
                    # -------------------------------------------------------
                    detail, detail_err = api_call("GET", f"requests/{rid}")
                    
                    if detail_err:
                        st.error(detail_err)
                        continue
                    
                    # AI Recommendation banner
                    if ai_rec == "APPROVE":
                        st.success(f"🤖 **AI Recommends: APPROVE** (Confidence: {conf}%)")
                    else:
                        st.error(f"🤖 **AI Recommends: DENY** (Confidence: {conf}%)")
                    
                    st.info(f"**Reasoning:** {detail.get('reasoning', 'N/A')}")
                    
                    # Patient / Procedure info
                    st.markdown("##### 👤 Patient & Procedure")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Enrollee", detail['enrollee_id'])
                    c2.metric("Age / Gender", patient_info)
                    c3.metric("Procedure", f"{detail.get('procedure_name', '?')}")
                    c4.metric("Diagnosis", f"{detail.get('diagnosis_name', '?')}")
                    
                    st.caption(
                        f"Codes: {detail['procedure_code']} + {detail['diagnosis_code']} | "
                        f"Date: {detail['encounter_date']} | "
                        f"Hospital: {detail.get('hospital_name') or 'N/A'}"
                    )
                    
                    # === VALIDATION RULES ===
                    rules = detail.get('rules', [])
                    failed = [r for r in rules if not r['passed']]
                    passed = [r for r in rules if r['passed']]
                    
                    summary = detail.get('summary', {})
                    st.markdown("##### 📋 Validation Rules")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Total", summary.get('total_rules', len(rules)))
                    c2.metric("Passed", summary.get('passed', len(passed)))
                    c3.metric("Failed", summary.get('failed', len(failed)))
                    c4.metric("AI Validated", summary.get('ai_validated', 0))
                    
                    # Failed rules (expanded — these are what the agent needs to review)
                    if failed:
                        st.markdown("###### ❌ Failed Rules")
                        for rule in failed:
                            src = {"master_table": "📖 Master", "ai": "🤖 AI", "learning_table": "📚 Learned"}.get(rule['source'], rule['source'])
                            st.error(f"**{rule['rule_name']}** ({src}, {rule['confidence']}%)")
                            st.write(f"↳ {rule['reasoning']}")
                            # Show PubMed evidence if available
                            details = rule.get('details', {})
                            pubmed_articles = details.get('pubmed_evidence', [])
                            pubmed_count = details.get('pubmed_count', 0)
                            pubmed_triggered = details.get('pubmed_triggered', False)
                            if pubmed_count > 0 and pubmed_articles:
                                with st.expander(f"📚 PubMed Second Opinion ({pubmed_count} article{'s' if pubmed_count > 1 else ''})", expanded=True):
                                    st.caption("🔬 AI confidence was low — PubMed evidence was consulted")
                                    for art in pubmed_articles:
                                        st.markdown(f"**[PMID: {art['pmid']}](https://pubmed.ncbi.nlm.nih.gov/{art['pmid']}/)** ({art['year']})")
                                        st.markdown(f"*{art['title']}*")
                                        st.caption(f"Authors: {art['authors']}")
                            elif rule.get('rule_name') == 'PROC_DIAG_COMPATIBILITY' and rule.get('source') == 'ai':
                                if pubmed_triggered and pubmed_count == 0:
                                    st.caption("🔬 AI confidence was low — PubMed searched but no articles found")
                                elif not pubmed_triggered:
                                    st.caption("⚡ AI confidence ≥ 75% — PubMed not needed")
                            if details:
                                with st.expander("🔍 Raw Details", expanded=False):
                                    st.json(details)
                    
                    # Passed rules (collapsed)
                    if passed:
                        with st.expander(f"✅ {len(passed)} Passed Rules", expanded=False):
                            for rule in passed:
                                src = {"master_table": "📖 Master", "ai": "🤖 AI", "learning_table": "📚 Learned"}.get(rule['source'], rule['source'])
                                st.markdown(f"✅ **{rule['rule_name']}** ({src}, {rule['confidence']}%) — {rule['reasoning'][:120]}")
                                # Show PubMed evidence for AI-validated passed rules
                                details = rule.get('details', {})
                                pubmed_articles = details.get('pubmed_evidence', [])
                                pubmed_triggered = details.get('pubmed_triggered', False)
                                if pubmed_articles:
                                    for art in pubmed_articles:
                                        st.caption(f"🔬 PMID [{art['pmid']}](https://pubmed.ncbi.nlm.nih.gov/{art['pmid']}/) — {art['title'][:100]}")
                                elif pubmed_triggered:
                                    st.caption("🔬 PubMed searched — no articles found")
                    
                    # === LEARNING INFO ===
                    ai_rules = [r for r in rules if r['source'] == 'ai' and r.get('confidence', 0) > 0]
                    if ai_rules:
                        st.markdown("---")
                        st.markdown("##### 📚 Learning Impact")
                        st.info(
                            f"**{len(ai_rules)} AI-validated rule(s)** will be stored if you confirm.\n\n"
                            f"This means future identical requests will be **instant** (no AI call needed)."
                        )
                        for ar in ai_rules:
                            status_icon = "✅" if ar['passed'] else "❌"
                            st.write(f"  {status_icon} **{ar['rule_name']}**: Will learn as {'VALID' if ar['passed'] else 'INVALID'}")
                    
                    # === REVIEW ACTIONS ===
                    st.markdown("---")
                    st.markdown("##### 👤 Your Decision")
                    
                    reviewer = st.text_input("Reviewed by", value="Casey", key=f"rev_{rid}")
                    review_notes = st.text_area("Notes (optional)", height=50, key=f"notes_{rid}")
                    
                    c1, c2, c3 = st.columns(3)
                    
                    with c1:
                        if st.button(
                            f"✅ Confirm AI {ai_rec}",
                            type="primary",
                            use_container_width=True,
                            key=f"confirm_{rid}"
                        ):
                            with st.spinner("Confirming and storing learning..."):
                                res, e = api_call("POST", f"review/{rid}", json={
                                    "action": "CONFIRM",
                                    "reviewed_by": reviewer,
                                    "notes": review_notes or None
                                })
                            if e:
                                st.error(e)
                            else:
                                st.success(res.get('message', 'Done'))
                                stored = res.get('learning_stored', {})
                                if stored:
                                    for k, v in stored.items():
                                        st.write(f"  📚 **{k}**: {'stored' if v else 'skipped'}")
                                st.balloons()
                                st.rerun()
                    
                    with c2:
                        override_to = "APPROVE" if ai_rec == "DENY" else "DENY"
                        if st.button(
                            f"⚠️ Override → {override_to}",
                            use_container_width=True,
                            key=f"override_{rid}"
                        ):
                            with st.spinner("Processing override..."):
                                res, e = api_call("POST", f"review/{rid}", json={
                                    "action": "OVERRIDE",
                                    "override_decision": override_to,
                                    "reviewed_by": reviewer,
                                    "notes": review_notes or f"Agent overrode AI {ai_rec}"
                                })
                            if e:
                                st.error(e)
                            else:
                                st.warning(res.get('message', 'Done'))
                                st.rerun()
                    
                    with c3:
                        if st.button("⏭️ Skip", use_container_width=True, key=f"skip_{rid}"):
                            pass  # Just collapses


# ============================================================================
# TAB 2: DASHBOARD
# ============================================================================

with tab_dash:
    st.subheader("📊 System Dashboard")
    
    stats, err = api_call("GET", "stats")
    if err:
        st.error(err)
    else:
        # Today's activity
        st.markdown("#### 📅 Today's Activity")
        today = stats.get('today', {})
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Auto-Approved", today.get('AUTO_APPROVED', 0))
        c2.metric("Auto-Denied", today.get('AUTO_DENIED', 0))
        c3.metric("Pending", today.get('PENDING_REVIEW', 0))
        c4.metric("Human Approved", today.get('HUMAN_APPROVED', 0))
        c5.metric("Human Denied", today.get('HUMAN_DENIED', 0))
        
        st.markdown("---")
        
        # All-time queue
        st.markdown("#### 📦 All-Time Breakdown")
        queue = stats.get('queue', {})
        if queue:
            q_df = pd.DataFrame([{"Status": f"{STATUS_DISPLAY.get(k, ('',''))[0]} {k}", "Count": v} for k, v in queue.items()])
            st.dataframe(q_df, use_container_width=True, hide_index=True)
        
        auto_rate = stats.get('automation_rate', 0)
        st.metric("🤖 Automation Rate", f"{auto_rate}%", help="% resolved without human review")
        
        st.markdown("---")
        
        # Learning knowledge base
        st.markdown("#### 📚 Learning Knowledge Base")
        ls = stats.get('learning_summary', {})
        c1, c2 = st.columns(2)
        c1.metric("Total Learning Entries", ls.get('total_entries', 0))
        c2.metric("AI Calls Saved (cumulative)", ls.get('total_ai_calls_saved', 0))
        
        learning = stats.get('learning', {})
        if learning:
            l_df = pd.DataFrame([
                {
                    "Table": name.replace('ai_human_', '').replace('_', ' ').title(),
                    "Entries": v['entries'],
                    "Times Used": v['total_usage']
                }
                for name, v in learning.items()
            ])
            st.dataframe(l_df, use_container_width=True, hide_index=True)


# ============================================================================
# TAB 3: ALL HISTORY
# ============================================================================

with tab_history:
    st.subheader("📜 All Request History")
    
    c1, c2 = st.columns([2, 1])
    with c1:
        hist_filter = st.selectbox(
            "Filter", ["All", "AUTO_APPROVED", "AUTO_DENIED", "PENDING_REVIEW", "HUMAN_APPROVED", "HUMAN_DENIED"],
            key="hist_filter"
        )
    with c2:
        st.button("🔄 Refresh", key="refresh_hist")
    
    params = {"limit": 100}
    if hist_filter != "All":
        params["status"] = hist_filter
    
    data, err = api_call("GET", "history", params=params)
    if err:
        st.error(err)
    elif data and data.get('requests'):
        df = pd.DataFrame(data['requests'])
        if not df.empty:
            df['status_display'] = df['status'].map(
                lambda s: f"{STATUS_DISPLAY.get(s, ('',''))[0]} {STATUS_DISPLAY.get(s, ('',''))[1]}"
            )
            cols = ['request_id', 'status_display', 'decision', 'procedure_name', 'diagnosis_name',
                    'enrollee_id', 'confidence', 'reviewed_by', 'created_at']
            available = [c for c in cols if c in df.columns]
            st.dataframe(
                df[available].rename(columns={
                    'request_id': 'ID', 'status_display': 'Status', 'decision': 'Decision',
                    'procedure_name': 'Procedure', 'diagnosis_name': 'Diagnosis',
                    'enrollee_id': 'Enrollee', 'confidence': 'Conf %',
                    'reviewed_by': 'Reviewed By', 'created_at': 'Submitted'
                }),
                use_container_width=True, hide_index=True
            )
    else:
        st.info("No requests found")