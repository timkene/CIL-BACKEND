#!/usr/bin/env python3
"""
HOSPITAL PA REQUEST PORTAL
============================

Hospital-side interface:
- Single PA request submission
- Batch file upload (CSV/Excel) with row-by-row vetting
- Track request status
- Request history

Run: streamlit run hospital_app.py --server.port 8501
Requires: vetting_api.py running on port 8000

Author: Casey's AI Assistant
Date: February 2026
Version: 2.0 — Batch upload
"""

import streamlit as st
import requests
import pandas as pd
import time
import io
from datetime import date, datetime

API_URL = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Hospital PA Portal", page_icon="🏥", layout="wide")

# ============================================================================
# HELPERS
# ============================================================================

def api_call(method, endpoint, **kwargs):
    """Call vetting API with error handling"""
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
        return None, "❌ Cannot connect to API. Start it with: `python vetting_api.py`"
    except Exception as e:
        return None, f"❌ {str(e)}"


STATUS_DISPLAY = {
    "AUTO_APPROVED":  ("✅", "Auto-Approved", "success"),
    "AUTO_DENIED":    ("❌", "Auto-Denied", "error"),
    "PENDING_REVIEW": ("⏳", "Under Review", "warning"),
    "HUMAN_APPROVED": ("✅", "Approved by Agent", "success"),
    "HUMAN_DENIED":   ("❌", "Denied by Agent", "error"),
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names from uploaded file.
    Accepts various naming conventions and maps to standard names.
    """
    col_map = {}
    lower_cols = {c.lower().strip().replace(' ', '_').replace('-', '_'): c for c in df.columns}
    
    # Enrollee ID
    for key in ['enrollee_id', 'enrolleeid', 'enrollee', 'member_id', 'memberid', 'iid', 'id']:
        if key in lower_cols:
            col_map[lower_cols[key]] = 'enrollee_id'
            break
    
    # Procedure code
    for key in ['procedure_code', 'procedurecode', 'procedure', 'proc_code', 'drug_code', 'code']:
        if key in lower_cols:
            col_map[lower_cols[key]] = 'procedure_code'
            break
    
    # Diagnosis code
    for key in ['diagnosis_code', 'diagnosiscode', 'diagnosis', 'diag_code', 'icd_code', 'icd10', 'dx_code']:
        if key in lower_cols:
            col_map[lower_cols[key]] = 'diagnosis_code'
            break
    
    # Date
    for key in ['encounter_date', 'encounterdate', 'date', 'encounter_date_from',
                'encounterdatefrom', 'request_date', 'requestdate', 'visit_date']:
        if key in lower_cols:
            col_map[lower_cols[key]] = 'encounter_date'
            break
    
    # Hospital name (optional)
    for key in ['hospital_name', 'hospitalname', 'hospital', 'provider', 'facility']:
        if key in lower_cols:
            col_map[lower_cols[key]] = 'hospital_name'
            break
    
    df = df.rename(columns=col_map)
    return df


def render_result_card(result: dict, expanded: bool = False):
    """Render a single validation result as an expander card"""
    status = result.get('status', 'UNKNOWN')
    icon, label, _ = STATUS_DISPLAY.get(status, ("❓", status, "info"))
    decision = result.get('decision', '?')
    conf = result.get('confidence', 0)
    proc_name = result.get('procedure_name', result.get('procedure_code', '?'))
    diag_name = result.get('diagnosis_name', result.get('diagnosis_code', '?'))
    rid = result.get('request_id', '?')
    
    header = (
        f"{icon} **{proc_name}** + {diag_name} | "
        f"{result.get('enrollee_id', '?')} | "
        f"{decision} ({conf}%)"
    )
    
    with st.expander(header, expanded=expanded):
        # Status banner
        if status == "AUTO_APPROVED":
            st.success(f"✅ **APPROVED** — Request ID: `{rid}`")
        elif status == "AUTO_DENIED":
            st.error(f"❌ **DENIED** — Request ID: `{rid}`")
        elif status == "PENDING_REVIEW":
            st.warning(f"⏳ **PENDING AGENT REVIEW** — AI recommends {decision} ({conf}%) — Request ID: `{rid}`")
        
        # Reasoning
        st.write(f"**Reasoning:** {result.get('reasoning', 'N/A')}")
        
        # Patient / procedure metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Enrollee", result.get('enrollee_id', 'N/A'))
        with col2:
            age = result.get('enrollee_age')
            gender = result.get('enrollee_gender', '')
            st.metric("Age / Gender", f"{age}yo {gender}" if age else "Unknown")
        with col3:
            st.metric("Procedure", proc_name)
            st.caption(result.get('procedure_code', ''))
        with col4:
            st.metric("Diagnosis", diag_name)
            st.caption(result.get('diagnosis_code', ''))
        
        # Rules summary
        summary = result.get('summary', {})
        if summary:
            col1, col2, col3 = st.columns(3)
            col1.metric("Rules Checked", summary.get('total_rules', 0))
            col2.metric("✅ Passed", summary.get('passed', 0))
            col3.metric("❌ Failed", summary.get('failed', 0))
        
        # Individual rules
        rules = result.get('rules', [])
        if rules:
            failed = [r for r in rules if not r.get('passed')]
            passed = [r for r in rules if r.get('passed')]
            
            if failed:
                st.markdown("**❌ Failed Rules:**")
                for rule in failed:
                    src = {"master_table": "📖", "ai": "🤖", "learning_table": "📚"}.get(rule.get('source', ''), "❓")
                    st.error(f"**{rule['rule_name']}** ({src} {rule['source']}, {rule['confidence']}%) — {rule['reasoning'][:200]}")
                    # Show PubMed evidence if available
                    details = rule.get('details', {})
                    pubmed_articles = details.get('pubmed_evidence', [])
                    pubmed_triggered = details.get('pubmed_triggered', False)
                    if pubmed_articles:
                        for art in pubmed_articles:
                            st.caption(f"🔬 [{art['pmid']}](https://pubmed.ncbi.nlm.nih.gov/{art['pmid']}/) — {art['title'][:120]} ({art['year']})")
                    elif pubmed_triggered:
                        st.caption("🔬 PubMed searched — no articles found")
            
            if passed:
                with st.expander(f"✅ {len(passed)} Passed Rules", expanded=False):
                    for rule in passed:
                        src = {"master_table": "📖", "ai": "🤖", "learning_table": "📚"}.get(rule.get('source', ''), "❓")
                        st.markdown(f"✅ **{rule['rule_name']}** ({src} {rule['source']}, {rule['confidence']}%) — {rule['reasoning'][:150]}")
                        details = rule.get('details', {})
                        pubmed_articles = details.get('pubmed_evidence', [])
                        pubmed_triggered = details.get('pubmed_triggered', False)
                        if pubmed_articles:
                            for art in pubmed_articles:
                                st.caption(f"🔬 [{art['pmid']}](https://pubmed.ncbi.nlm.nih.gov/{art['pmid']}/) — {art['title'][:120]} ({art['year']})")
                        elif pubmed_triggered:
                            st.caption("🔬 PubMed searched — no articles found")


# ============================================================================
# CONNECTION CHECK
# ============================================================================

st.title("🏥 Hospital PA Request Portal")
st.caption("Submit single or batch pre-authorization requests")

health, err = api_call("GET", "health")
if err:
    st.error(err)
    st.stop()

# ============================================================================
# TABS
# ============================================================================

tab_single, tab_batch, tab_track, tab_history = st.tabs([
    "📝 Single Request", "📂 Batch Upload", "🔍 Track Request", "📋 History"
])

# ============================================================================
# TAB 1: SINGLE REQUEST
# ============================================================================

with tab_single:
    st.subheader("Submit Single PA Request")
    
    with st.form("pa_form"):
        col1, col2 = st.columns(2)
        with col1:
            procedure_code = st.text_input("Procedure Code *", placeholder="e.g. DRG1106")
            diagnosis_code = st.text_input("Diagnosis Code *", placeholder="e.g. B509")
        with col2:
            enrollee_id = st.text_input("Enrollee ID *", placeholder="e.g. CL/OCTA/723449/2023-A")
            encounter_date = st.date_input("Encounter Date", value=date.today())
        
        hospital_name = st.text_input("Hospital Name", placeholder="e.g. Lagos University Teaching Hospital")
        notes = st.text_area("Clinical Notes (optional)", height=60)
        
        submitted = st.form_submit_button("🔍 Submit PA Request", type="primary", use_container_width=True)
    
    if submitted:
        if not procedure_code or not diagnosis_code or not enrollee_id:
            st.error("⚠️ Please fill in all required fields")
        else:
            with st.spinner("🔄 Validating..."):
                result, err = api_call("POST", "validate", json={
                    "procedure_code": procedure_code.strip(),
                    "diagnosis_code": diagnosis_code.strip(),
                    "enrollee_id": enrollee_id.strip(),
                    "encounter_date": encounter_date.strftime('%Y-%m-%d'),
                    "hospital_name": hospital_name.strip() or None,
                    "notes": notes.strip() or None
                })
            
            if err:
                st.error(err)
            else:
                st.markdown("---")
                render_result_card(result, expanded=True)


# ============================================================================
# TAB 2: BATCH UPLOAD
# ============================================================================

with tab_batch:
    st.subheader("📂 Batch PA Submission")
    st.markdown("""
    Upload a **CSV or Excel** file with the following columns:
    
    | Column | Required | Example |
    |--------|----------|---------|
    | `enrollee_id` | ✅ | CL/OCTA/723449/2023-A |
    | `procedure_code` | ✅ | DRG1106 |
    | `diagnosis_code` | ✅ | B509 |
    | `encounter_date` | Optional (defaults to today) | 2026-02-18 |
    | `hospital_name` | Optional | LUTH |
    
    *Column names are flexible — the system recognizes common variations.*
    """)
    
    uploaded_file = st.file_uploader(
        "Upload File",
        type=["csv", "xlsx", "xls"],
        help="CSV or Excel file with PA request rows"
    )
    
    hospital_name_batch = st.text_input(
        "Hospital Name (applied to all rows)",
        placeholder="e.g. Lagos University Teaching Hospital",
        key="batch_hospital"
    )
    
    if uploaded_file:
        # Parse file
        try:
            if uploaded_file.name.endswith('.csv'):
                raw_df = pd.read_csv(uploaded_file, dtype=str)
            else:
                raw_df = pd.read_excel(uploaded_file, dtype=str)
            
            raw_df = raw_df.dropna(how='all')  # Drop fully empty rows
            df = normalize_columns(raw_df)
            
        except Exception as e:
            st.error(f"❌ Could not read file: {e}")
            st.stop()
        
        # Validate required columns
        required = ['enrollee_id', 'procedure_code', 'diagnosis_code']
        missing = [c for c in required if c not in df.columns]
        
        if missing:
            st.error(f"❌ Missing required columns: **{', '.join(missing)}**")
            st.write("**Found columns:**", list(df.columns))
            st.write("**Original columns:**", list(raw_df.columns))
            st.stop()
        
        # Clean data
        for col in required:
            df[col] = df[col].astype(str).str.strip()
        
        if 'encounter_date' not in df.columns:
            df['encounter_date'] = date.today().strftime('%Y-%m-%d')
        else:
            # Try to parse dates, fallback to today
            df['encounter_date'] = df['encounter_date'].fillna(date.today().strftime('%Y-%m-%d'))
            df['encounter_date'] = df['encounter_date'].astype(str).str.strip()
        
        if 'hospital_name' not in df.columns:
            df['hospital_name'] = hospital_name_batch or None
        
        # Remove rows with empty required fields
        valid_mask = (df['enrollee_id'] != '') & (df['enrollee_id'] != 'nan') & \
                     (df['procedure_code'] != '') & (df['procedure_code'] != 'nan') & \
                     (df['diagnosis_code'] != '') & (df['diagnosis_code'] != 'nan')
        df = df[valid_mask].reset_index(drop=True)
        
        st.markdown("---")
        st.markdown(f"### 📋 Preview — {len(df)} valid row(s)")
        
        # Show preview
        preview_cols = ['enrollee_id', 'procedure_code', 'diagnosis_code', 'encounter_date']
        if 'hospital_name' in df.columns:
            preview_cols.append('hospital_name')
        st.dataframe(df[preview_cols], use_container_width=True, hide_index=True)
        
        # Process button
        st.markdown("---")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            process_btn = st.button(
                f"🚀 Validate All {len(df)} Requests",
                type="primary",
                use_container_width=True
            )
        with col2:
            st.metric("Total Rows", len(df))
        
        if process_btn:
            # ---------------------------------------------------------------
            # PROCESS ROW BY ROW
            # ---------------------------------------------------------------
            results = []
            errors = []
            
            progress_bar = st.progress(0, text="Starting batch validation...")
            status_text = st.empty()
            
            start_time = time.time()
            
            for idx, row in df.iterrows():
                progress = (idx + 1) / len(df)
                status_text.text(
                    f"Processing {idx + 1}/{len(df)}: "
                    f"{row['procedure_code']} + {row['diagnosis_code']} for {row['enrollee_id']}"
                )
                progress_bar.progress(progress, text=f"Validating {idx + 1}/{len(df)}...")
                
                try:
                    result, err = api_call("POST", "validate", json={
                        "procedure_code": row['procedure_code'],
                        "diagnosis_code": row['diagnosis_code'],
                        "enrollee_id": row['enrollee_id'],
                        "encounter_date": row.get('encounter_date', date.today().strftime('%Y-%m-%d')),
                        "hospital_name": row.get('hospital_name') or hospital_name_batch or None,
                    })
                    
                    if err:
                        errors.append({"row": idx + 1, "error": err, **row.to_dict()})
                    else:
                        result['_row'] = idx + 1
                        results.append(result)
                        
                except Exception as e:
                    errors.append({"row": idx + 1, "error": str(e), **row.to_dict()})
            
            elapsed = time.time() - start_time
            progress_bar.progress(1.0, text="✅ Batch complete!")
            status_text.empty()
            
            # Store results in session state for persistence
            st.session_state.batch_results = results
            st.session_state.batch_errors = errors
            st.session_state.batch_elapsed = elapsed
        
        # ---------------------------------------------------------------
        # DISPLAY RESULTS (from session state so they persist)
        # ---------------------------------------------------------------
        if 'batch_results' in st.session_state and st.session_state.batch_results:
            results = st.session_state.batch_results
            errors = st.session_state.batch_errors
            elapsed = st.session_state.batch_elapsed
            
            st.markdown("---")
            st.markdown("## 📊 Batch Results")
            
            # Group by status
            auto_approved = [r for r in results if r['status'] == 'AUTO_APPROVED']
            auto_denied = [r for r in results if r['status'] == 'AUTO_DENIED']
            pending = [r for r in results if r['status'] == 'PENDING_REVIEW']
            
            # Summary metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Total Processed", len(results))
            col2.metric("✅ Auto-Approved", len(auto_approved))
            col3.metric("❌ Auto-Denied", len(auto_denied))
            col4.metric("⏳ Sent to Agent", len(pending))
            col5.metric("⚠️ Errors", len(errors))
            
            st.caption(f"⏱️ Processed in {elapsed:.1f}s ({elapsed/max(len(results),1):.1f}s per request)")
            
            # Summary table
            st.markdown("---")
            summary_data = []
            for r in results:
                summary_data.append({
                    'Row': r.get('_row', '?'),
                    'Status': f"{STATUS_DISPLAY.get(r['status'], ('','',''))[0]} {r['status']}",
                    'Decision': r['decision'],
                    'Procedure': r.get('procedure_name', r['procedure_code']),
                    'Diagnosis': r.get('diagnosis_name', r['diagnosis_code']),
                    'Enrollee': r.get('enrollee_id', '?'),
                    'Confidence': f"{r.get('confidence', 0)}%",
                    'Request ID': r.get('request_id', '?'),
                })
            
            summary_df = pd.DataFrame(summary_data)
            
            # Color code the summary table
            def highlight_status(row):
                status = row['Status']
                if 'AUTO_APPROVED' in status:
                    return ['background-color: #d4edda'] * len(row)
                elif 'AUTO_DENIED' in status:
                    return ['background-color: #f8d7da'] * len(row)
                elif 'PENDING' in status:
                    return ['background-color: #fff3cd'] * len(row)
                return [''] * len(row)
            
            st.markdown("### 📋 Summary Table")
            st.dataframe(
                summary_df.style.apply(highlight_status, axis=1),
                use_container_width=True,
                hide_index=True
            )
            
            # ===================================================================
            # AUTO-APPROVED SECTION
            # ===================================================================
            if auto_approved:
                st.markdown("---")
                st.markdown(f"### ✅ Auto-Approved ({len(auto_approved)})")
                st.success(f"{len(auto_approved)} request(s) passed all rules and were instantly approved.")
                
                for r in auto_approved:
                    render_result_card(r, expanded=False)
            
            # ===================================================================
            # AUTO-DENIED SECTION
            # ===================================================================
            if auto_denied:
                st.markdown("---")
                st.markdown(f"### ❌ Auto-Denied ({len(auto_denied)})")
                st.error(f"{len(auto_denied)} request(s) failed established validation rules.")
                
                for r in auto_denied:
                    render_result_card(r, expanded=True)
            
            # ===================================================================
            # PENDING REVIEW SECTION
            # ===================================================================
            if pending:
                st.markdown("---")
                st.markdown(f"### ⏳ Sent to Agent for Review ({len(pending)})")
                st.warning(
                    f"{len(pending)} request(s) require agent confirmation. "
                    f"These have been queued and will appear in the Agent Review Portal."
                )
                
                for r in pending:
                    render_result_card(r, expanded=True)
            
            # ===================================================================
            # ERRORS SECTION
            # ===================================================================
            if errors:
                st.markdown("---")
                st.markdown(f"### ⚠️ Errors ({len(errors)})")
                st.error(f"{len(errors)} row(s) could not be processed.")
                
                for e in errors:
                    with st.expander(f"⚠️ Row {e.get('row', '?')}: {e.get('procedure_code', '?')} + {e.get('diagnosis_code', '?')}", expanded=True):
                        st.write(f"**Error:** {e.get('error', 'Unknown error')}")
                        st.write(f"**Enrollee:** {e.get('enrollee_id', '?')}")
                        st.write(f"**Procedure:** {e.get('procedure_code', '?')}")
                        st.write(f"**Diagnosis:** {e.get('diagnosis_code', '?')}")
            
            # ===================================================================
            # DOWNLOAD RESULTS
            # ===================================================================
            st.markdown("---")
            st.markdown("### 📥 Download Results")
            
            # Build download CSV
            download_data = []
            for r in results:
                download_data.append({
                    'request_id': r.get('request_id', ''),
                    'status': r['status'],
                    'decision': r['decision'],
                    'confidence': r.get('confidence', 0),
                    'procedure_code': r['procedure_code'],
                    'procedure_name': r.get('procedure_name', ''),
                    'diagnosis_code': r['diagnosis_code'],
                    'diagnosis_name': r.get('diagnosis_name', ''),
                    'enrollee_id': r.get('enrollee_id', ''),
                    'enrollee_age': r.get('enrollee_age', ''),
                    'enrollee_gender': r.get('enrollee_gender', ''),
                    'reasoning': r.get('reasoning', ''),
                })
            for e in errors:
                download_data.append({
                    'request_id': 'ERROR',
                    'status': 'ERROR',
                    'decision': '',
                    'confidence': '',
                    'procedure_code': e.get('procedure_code', ''),
                    'procedure_name': '',
                    'diagnosis_code': e.get('diagnosis_code', ''),
                    'diagnosis_name': '',
                    'enrollee_id': e.get('enrollee_id', ''),
                    'enrollee_age': '',
                    'enrollee_gender': '',
                    'reasoning': e.get('error', ''),
                })
            
            download_df = pd.DataFrame(download_data)
            csv_buffer = download_df.to_csv(index=False)
            
            st.download_button(
                "📥 Download Results CSV",
                data=csv_buffer,
                file_name=f"batch_vetting_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )


# ============================================================================
# TAB 3: TRACK REQUEST
# ============================================================================

with tab_track:
    st.subheader("🔍 Track a Request")
    
    track_id = st.text_input("Enter Request ID", placeholder="e.g. a1b2c3d4e5f6")
    
    if st.button("🔍 Check Status", use_container_width=True) and track_id:
        result, err = api_call("GET", f"requests/{track_id.strip()}")
        
        if err:
            st.error(err)
        else:
            st.markdown("---")
            render_result_card(result, expanded=True)
            
            if result.get('reviewed_by'):
                st.info(f"📝 Reviewed by **{result['reviewed_by']}** at {result.get('reviewed_at', 'N/A')}")


# ============================================================================
# TAB 4: HISTORY
# ============================================================================

with tab_history:
    st.subheader("📋 Request History")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        status_filter = st.selectbox(
            "Filter by Status",
            ["All", "AUTO_APPROVED", "AUTO_DENIED", "PENDING_REVIEW", "HUMAN_APPROVED", "HUMAN_DENIED"]
        )
    with col2:
        st.button("🔄 Refresh", key="refresh_history")
    
    params = {"limit": 100}
    if status_filter != "All":
        params["status"] = status_filter
    
    data, err = api_call("GET", "history", params=params)
    
    if err:
        st.error(err)
    elif data and data.get('requests'):
        df = pd.DataFrame(data['requests'])
        
        if not df.empty:
            df['status_display'] = df['status'].map(
                lambda s: f"{STATUS_DISPLAY.get(s, ('❓','',''))[0]} {STATUS_DISPLAY.get(s, ('','',''))[1]}"
            )
            
            cols = ['request_id', 'status_display', 'procedure_name', 'diagnosis_name',
                    'enrollee_id', 'decision', 'confidence', 'created_at']
            available = [c for c in cols if c in df.columns]
            
            st.dataframe(
                df[available].rename(columns={
                    'request_id': 'ID', 'status_display': 'Status',
                    'procedure_name': 'Procedure', 'diagnosis_name': 'Diagnosis',
                    'enrollee_id': 'Enrollee', 'decision': 'Decision',
                    'confidence': 'Conf %', 'created_at': 'Submitted'
                }),
                use_container_width=True, hide_index=True
            )
    else:
        st.info("No requests found")


# ============================================================================
# SIDEBAR
# ============================================================================

st.sidebar.title("🏥 Hospital Portal")
st.sidebar.markdown("---")
st.sidebar.markdown("""
**Submission Modes:**

**📝 Single** — One PA at a time

**📂 Batch** — Upload CSV/Excel:
- `enrollee_id` (required)
- `procedure_code` (required)
- `diagnosis_code` (required)
- `encounter_date` (optional)
- `hospital_name` (optional)
""")
st.sidebar.markdown("---")
st.sidebar.markdown("""
**Decision States:**
- ✅ **Auto-Approved** — instant
- ❌ **Auto-Denied** — instant
- ⏳ **Pending Review** — agent decides
""")
st.sidebar.markdown("---")
st.sidebar.caption(f"API: {API_URL}")