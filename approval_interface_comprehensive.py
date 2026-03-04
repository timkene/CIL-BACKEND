#!/usr/bin/env python3
"""
COMPREHENSIVE APPROVAL INTERFACE WITH 30-DAY HISTORY
====================================================

Shows ALL validation rules including 30-day duplicate checking with
complete history display (procedures + diagnoses from PA and Claims)

Features:
- Comprehensive validation report
- 30-day duplicate detection (exact + therapeutic class)
- Full history table with all procedures/diagnoses used in last 30 days
- Source tracking (master table vs AI)
- Store AI-approved rules

Author: Casey
Date: February 2026
Version: 3.0 - 30-Day Validation
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date

# Import engines
try:
    from vetting_learning_engine import LearningVettingEngine
    from vetting_comprehensive import ComprehensiveVettingEngine, ComprehensiveValidation, RuleResult
    from vetting_30day_validation import ThirtyDayValidationEngine
    COMPREHENSIVE_AVAILABLE = True
except ImportError as e:
    COMPREHENSIVE_AVAILABLE = False
    st.error(f"⚠️ Could not import engines: {e}")

st.set_page_config(
    page_title="PA Approval - 30-Day Validation",
    page_icon="✅",
    layout="wide"
)

# Initialize engine
if 'engine' not in st.session_state:
    if COMPREHENSIVE_AVAILABLE:
        st.session_state.engine = ComprehensiveVettingEngine()
    else:
        st.error("Comprehensive engine not available")
        st.stop()

st.title("🥼 PA Pre-Authorization - Comprehensive Validation with 30-Day Check")
st.markdown("---")

# Input section
st.subheader("📋 PA Request Details")

col1, col2 = st.columns(2)

with col1:
    procedure_code = st.text_input(
        "Procedure Code *",
        placeholder="e.g., DRG1958",
        help="Enter the procedure/drug code"
    )
    
    diagnosis_code = st.text_input(
        "Diagnosis Code *",
        placeholder="e.g., B373",
        help="Enter the ICD-10 diagnosis code"
    )

with col2:
    enrollee_id = st.text_input(
        "Enrollee ID *",
        placeholder="e.g., CL/OCTA/723449/2023-A",
        help="Required for 30-day duplicate check"
    )
    
    encounter_date = st.date_input(
        "Encounter Date *",
        value=date.today(),
        help="Date of the medical encounter"
    )

# Show enrollee context
if enrollee_id:
    with st.expander("👤 Enrollee Information", expanded=False):
        try:
            context = st.session_state.engine.base_engine.get_enrollee_context(
                enrollee_id, 
                encounter_date.strftime('%Y-%m-%d')
            )
            
            if context.age or context.gender:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Enrollee ID", context.enrollee_id)
                with col2:
                    st.metric("Age", f"{context.age} years" if context.age else "Unknown")
                with col3:
                    st.metric("Gender", context.gender if context.gender else "Unknown")
            else:
                st.warning(f"⚠️ Enrollee {enrollee_id} not found or incomplete data")
        except Exception as e:
            st.error(f"Error fetching enrollee data: {e}")

st.markdown("---")

# Vet button
if st.button("🔍 Run Full Validation (Including 30-Day Check)", type="primary", use_container_width=True):
    if not procedure_code or not diagnosis_code or not enrollee_id:
        st.error("⚠️ Please enter procedure code, diagnosis code, and enrollee ID")
    else:
        with st.spinner("Running ALL validation rules including 30-day duplicate check..."):
            # Get comprehensive validation
            validation = st.session_state.engine.validate_comprehensive(
                procedure_code=procedure_code,
                diagnosis_code=diagnosis_code,
                enrollee_id=enrollee_id,
                encounter_date=encounter_date.strftime('%Y-%m-%d')
            )
            
            st.session_state.current_validation = validation
            st.session_state.current_proc = procedure_code
            st.session_state.current_diag = diagnosis_code
            st.session_state.current_enrollee = enrollee_id
            st.session_state.current_date = encounter_date

# Display comprehensive validation results
if 'current_validation' in st.session_state:
    validation = st.session_state.current_validation
    
    st.markdown("---")
    
    # ===================================================================
    # OVERALL DECISION
    # ===================================================================
    st.subheader("🎯 Overall Decision")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if validation.overall_decision == "APPROVE":
            st.success(f"✅ {validation.overall_decision}")
        else:
            st.error(f"❌ {validation.overall_decision}")
    
    with col2:
        st.metric("Confidence", f"{validation.overall_confidence}%")
    
    with col3:
        if validation.auto_deny:
            st.error("🚫 Auto-Denied (Learned)")
        elif validation.requires_human_review:
            st.warning("⚠️ Requires Human Review")
        else:
            st.success("✅ Auto-Decision")
    
    st.info(f"**Reasoning:** {validation.overall_reasoning}")
    
    # ===================================================================
    # VALIDATION SUMMARY
    # ===================================================================
    st.markdown("---")
    st.subheader("📊 Validation Summary")
    
    summary = validation.get_summary()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Rules", summary['total_rules'])
    col2.metric("Passed", summary['passed'], delta="✅")
    col3.metric("Failed", summary['failed'], delta="❌" if summary['failed'] > 0 else "")
    col4.metric("Master Validated", summary['master_validated'])
    col5.metric("AI Validated", summary['ai_validated'])
    
    # ===================================================================
    # 30-DAY HISTORY DISPLAY
    # ===================================================================
    st.markdown("---")
    st.subheader("📅 30-Day Procedure History Analysis")
    
    # Extract 30-day validation results
    proc_30day_rule = next((r for r in validation.rule_results if r.rule_name == "PROCEDURE_30DAY_DUPLICATE"), None)
    
    if proc_30day_rule:
        st.markdown("### Procedure 30-Day Analysis")
        
        # Input procedure info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Input Code", proc_30day_rule.details.get('input_code', 'N/A'))
        with col2:
            st.metric("Therapeutic Class", proc_30day_rule.details.get('input_class', 'N/A'))
        with col3:
            status_icon = "✅ PASSED" if proc_30day_rule.passed else "❌ DENIED"
            st.metric("Status", status_icon)
        
        # History table
        history_items = proc_30day_rule.details.get('history_items', [])
        
        if history_items:
            st.markdown(f"**Found {len(history_items)} procedure(s) in last 30 days:**")
            
            # Convert to DataFrame for nice display
            history_df = pd.DataFrame(history_items)
            
            # Highlight exact duplicates and class duplicates
            def highlight_duplicates(row):
                input_code = proc_30day_rule.details.get('input_code', '').lower()
                input_class = proc_30day_rule.details.get('input_class', '')
                
                if row['code'].lower() == input_code:
                    return ['background-color: #ffcccc; font-weight: bold'] * len(row)
                elif row['class'] == input_class and input_class != 'Unknown':
                    return ['background-color: #fff3cd'] * len(row)
                return [''] * len(row)
            
            styled_df = history_df.style.apply(highlight_duplicates, axis=1)
            st.dataframe(styled_df, use_container_width=True)
            
            # Legend
            st.markdown("""
            **Legend:**
            - 🔴 Red highlight = Exact duplicate (same code)
            - 🟡 Yellow highlight = Same therapeutic class (different code)
            """)
        else:
            st.success("✅ No procedures found in last 30 days - CLEAR")
        
        # ---------------------------------------------------------------
        # ALWAYS show verification details so Casey can manually check
        # ---------------------------------------------------------------
        with st.expander("🔍 Manual Verification — All Recent Procedures (PA + Claims)", expanded=not bool(history_items)):
            try:
                conn = st.session_state.engine.conn
                
                enrollee = st.session_state.current_enrollee
                enc_date = st.session_state.current_date.strftime('%Y-%m-%d') if hasattr(st.session_state.current_date, 'strftime') else str(st.session_state.current_date)
                
                # PA DATA uses IID and 'code' columns (matching 30-day engine)
                pa_query = f"""
                SELECT 
                    pa.code,
                    COALESCE(p.procedure_name, 'Not in master') AS description,
                    pa.requestdate AS date,
                    'PA' AS source,
                    CASE 
                        WHEN CAST(pa.requestdate AS DATE) >= CAST('{enc_date}' AS DATE) - INTERVAL 30 DAY 
                        THEN '⬅️ Within 30 days' 
                        ELSE '📅 31-90 days ago' 
                    END AS window
                FROM "AI DRIVEN DATA"."PA DATA" pa
                LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" p 
                    ON LOWER(TRIM(pa.code)) = LOWER(TRIM(p.procedure_code))
                WHERE pa.IID = '{enrollee}'
                  AND CAST(pa.requestdate AS DATE) >= CAST('{enc_date}' AS DATE) - INTERVAL 90 DAY
                  AND CAST(pa.requestdate AS DATE) < CAST('{enc_date}' AS DATE)
                  AND pa.code IS NOT NULL AND TRIM(pa.code) != ''
                ORDER BY pa.requestdate DESC
                """
                
                # CLAIMS DATA uses enrollee_id and 'code' columns
                claims_query = f"""
                SELECT 
                    c.code,
                    COALESCE(p.procedure_name, 'Not in master') AS description,
                    c.encounterdatefrom AS date,
                    'Claims' AS source,
                    CASE 
                        WHEN CAST(c.encounterdatefrom AS DATE) >= CAST('{enc_date}' AS DATE) - INTERVAL 30 DAY 
                        THEN '⬅️ Within 30 days' 
                        ELSE '📅 31-90 days ago' 
                    END AS window
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" p 
                    ON LOWER(TRIM(c.code)) = LOWER(TRIM(p.procedure_code))
                WHERE c.enrollee_id = '{enrollee}'
                  AND CAST(c.encounterdatefrom AS DATE) >= CAST('{enc_date}' AS DATE) - INTERVAL 90 DAY
                  AND CAST(c.encounterdatefrom AS DATE) < CAST('{enc_date}' AS DATE)
                  AND c.code IS NOT NULL AND TRIM(c.code) != ''
                ORDER BY c.encounterdatefrom DESC
                """
                
                try:
                    pa_df = conn.execute(pa_query).fetchdf()
                except:
                    pa_df = pd.DataFrame()
                
                try:
                    claims_df = conn.execute(claims_query).fetchdf()
                except:
                    claims_df = pd.DataFrame()
                
                # Combine
                all_procs = pd.concat([pa_df, claims_df], ignore_index=True) if not pa_df.empty or not claims_df.empty else pd.DataFrame()
                
                if not all_procs.empty:
                    all_procs = all_procs.sort_values('date', ascending=False)
                    
                    # Split by window
                    within_30 = all_procs[all_procs['window'].str.contains('Within 30')]
                    outside_30 = all_procs[all_procs['window'].str.contains('31-90')]
                    
                    st.markdown(f"**Enrollee:** {enrollee} | **Encounter Date:** {enc_date}")
                    st.markdown(f"**Total procedures found (90-day lookback):** {len(all_procs)}")
                    
                    if not within_30.empty:
                        st.markdown(f"##### ⬅️ Within 30 days ({len(within_30)} procedures)")
                        st.dataframe(within_30[['code', 'description', 'date', 'source']].reset_index(drop=True), use_container_width=True)
                    else:
                        st.info("No procedures found within 30 days")
                    
                    if not outside_30.empty:
                        st.markdown(f"##### 📅 31-90 days ago ({len(outside_30)} procedures)")
                        st.dataframe(outside_30[['code', 'description', 'date', 'source']].reset_index(drop=True), use_container_width=True)
                else:
                    st.info(f"No procedures found for {enrollee} in the last 90 days (PA or Claims)")
                    
            except Exception as e:
                st.warning(f"⚠️ Could not load verification data: {e}")
        
        # Show reasoning
        if not proc_30day_rule.passed:
            st.error(f"**Denial Reason:**\n{proc_30day_rule.reasoning}")
    else:
        st.info("30-day procedure check not performed (enrollee ID or encounter date missing)")
    
    # ===================================================================
    # DETAILED RULE RESULTS
    # ===================================================================
    st.markdown("---")
    st.subheader("📋 All Validation Rules")
    
    # Group rules by status
    passed_rules = [r for r in validation.rule_results if r.passed]
    failed_rules = [r for r in validation.rule_results if not r.passed]
    
    # Show failed rules first
    if failed_rules:
        st.markdown("### ❌ Failed Rules")
        for rule in failed_rules:
            with st.expander(f"❌ {rule.rule_name} - FAILED", expanded=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**Source:** {rule.source}")
                with col2:
                    st.write(f"**Confidence:** {rule.confidence}%")
                with col3:
                    st.write(f"**Status:** ❌ FAILED")
                
                st.write(f"**Reasoning:** {rule.reasoning}")
                
                if rule.details and rule.rule_name not in ["PROCEDURE_30DAY_DUPLICATE"]:
                    with st.expander("🔍 Details", expanded=False):
                        st.json(rule.details)
    
    # Show passed rules
    if passed_rules:
        st.markdown("### ✅ Passed Rules")
        for rule in passed_rules:
            # Color code by source
            if rule.source == "master_table":
                icon = "📖"
                source_label = "Master Table"
            elif rule.source == "ai":
                icon = "🤖"
                source_label = "AI Validation"
            else:
                icon = "📚"
                source_label = "Learning Table"
            
            with st.expander(f"✅ {rule.rule_name} - PASSED ({source_label})", expanded=False):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**Source:** {icon} {rule.source}")
                with col2:
                    st.write(f"**Confidence:** {rule.confidence}%")
                with col3:
                    st.write(f"**Status:** ✅ PASSED")
                
                st.write(f"**Reasoning:** {rule.reasoning}")
                
                if rule.details and rule.rule_name not in ["PROCEDURE_30DAY_DUPLICATE"]:
                    with st.expander("🔍 Details", expanded=False):
                        st.json(rule.details)
    
    # ===================================================================
    # AUTO-DENY HANDLING
    # ===================================================================
    if validation.auto_deny:
        st.markdown("---")
        st.subheader("🚫 AUTO-DENIED — Learned Institutional Knowledge")
        
        st.error(
            "**This claim was automatically denied** based on previously validated medical rules. "
            "No human review required — these denials have been confirmed multiple times."
        )
        
        # Show which rules triggered auto-deny
        for rule in validation.auto_deny_rules:
            usage = rule.details.get('usage_count', 0)
            approver = rule.details.get('approved_by', 'Unknown')
            with st.expander(f"🚫 {rule.rule_name} — Used {usage}x (approved by {approver})", expanded=True):
                st.write(f"**Reasoning:** {rule.reasoning}")
                st.write(f"**Confidence:** {rule.confidence}%")
                st.write(f"**Times used:** {usage}")
                st.write(f"**Originally approved by:** {approver}")
        
        # Show passed rules too (for context)
        passed_rules = [r for r in validation.rule_results if r.passed]
        if passed_rules:
            with st.expander(f"✅ {len(passed_rules)} rule(s) passed (for context)"):
                for rule in passed_rules:
                    st.write(f"  - ✅ {rule.rule_name}: {rule.reasoning[:80]}...")
        
        # Override button for edge cases
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            st.success("✅ Auto-deny processed. No action needed.")
        with col2:
            if st.button("⚠️ Override — I Disagree (Review Manually)", type="secondary", use_container_width=True):
                # Force human review mode
                validation.auto_deny = False
                validation.requires_human_review = True
                st.session_state.current_validation = validation
                st.rerun()
    
    # ===================================================================
    # APPROVAL ACTIONS (only if NOT auto-denied)
    # ===================================================================
    elif validation.requires_human_review:
        st.markdown("---")
        st.subheader("👤 Your Decision")
        
        # Get ALL AI-validated rules (both passed and failed)
        ai_validated_rules = validation.get_ai_validated_rules()
        
        # Separate valid AI results from error results
        valid_ai_rules = [r for r in ai_validated_rules if r.confidence > 0 and "AI error" not in r.reasoning]
        error_ai_rules = [r for r in ai_validated_rules if r.confidence == 0 or "AI error" in r.reasoning]
        
        # Show AI errors as warnings (won't be stored)
        if error_ai_rules:
            for rule in error_ai_rules:
                st.warning(f"⚠️ **{rule.rule_name}** — AI call failed and will NOT be stored: {rule.reasoning[:100]}...")
        
        if valid_ai_rules:
            # Show what will be learned
            st.info(f"📚 **Learning Opportunity:** {len(valid_ai_rules)} AI-validated rule(s) can be stored")
            
            # Group by passed/failed for clarity
            passed_ai = [r for r in valid_ai_rules if r.passed]
            failed_ai = [r for r in valid_ai_rules if not r.passed]
            
            if passed_ai:
                st.markdown("**✅ Rules that PASSED (will learn as VALID):**")
                for rule in passed_ai:
                    if rule.rule_name == "PROCEDURE_30DAY_DUPLICATE":
                        st.write(f"  - ✅ {rule.rule_name}: No duplicates found (will remember this is clear)")
                    else:
                        st.write(f"  - ✅ {rule.rule_name}: {rule.reasoning[:100]}...")
            
            if failed_ai:
                st.markdown("**❌ Rules that FAILED (will learn as INVALID):**")
                for rule in failed_ai:
                    if rule.rule_name == "PROCEDURE_30DAY_DUPLICATE":
                        # Show pairwise relationships that will be learned
                        input_code = rule.details.get('input_code', '').upper()
                        input_class = rule.details.get('input_class', '')
                        history_items = rule.details.get('history_items', [])
                        
                        # Count class matches
                        class_matches = [
                            item.get('code', '').upper()
                            for item in history_items
                            if item.get('class') == input_class 
                            and input_class != 'Unknown'
                            and item.get('code', '').lower() != input_code.lower()
                        ]
                        
                        if class_matches:
                            st.write(f"  - ❌ {rule.rule_name}: Will learn {len(class_matches)} class relationship(s)")
                            for match_code in class_matches[:3]:  # Show first 3
                                st.write(f"      • {input_code} + {match_code} = SAME CLASS ({input_class})")
                            if len(class_matches) > 3:
                                st.write(f"      • ... and {len(class_matches) - 3} more")
                    else:
                        st.write(f"  - ❌ {rule.rule_name}: {rule.reasoning[:100]}...")
            
            # Show what approving means
            if validation.overall_decision == "APPROVE":
                st.success("✅ **Overall Decision: APPROVE**")
                st.write("Clicking 'Confirm & Learn' will:")
                st.write("- ✅ Store the AI validations for future use")
                st.write("- ✅ Make future similar requests faster (no AI needed)")
                button_label = "✅ Confirm Approval & Store Learning"
                button_type = "primary"
            else:
                st.error("❌ **Overall Decision: DENY**")
                st.write("Clicking 'Confirm & Learn' will:")
                st.write("- ✅ Store WHY this was denied")
                st.write("- ✅ Automatically deny similar requests in the future")
                st.write("- ⚠️ This DENY will be enforced - only confirm if you agree!")
                button_label = "✅ Confirm Denial & Store Learning"
                button_type = "primary"
        else:
            if error_ai_rules:
                st.info("ℹ️ No valid AI results to store (AI errors will not be learned)")
            else:
                st.info("ℹ️ No AI validations to learn from (all rules used master table or learning table)")
            button_label = "✅ Acknowledge"
            button_type = "secondary"
        
        # Action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button(button_label, type=button_type, use_container_width=True):
                if validation.can_store_ai_approvals and ai_validated_rules:
                    stored = st.session_state.engine.store_ai_validated_rules(
                        procedure_code=st.session_state.current_proc,
                        diagnosis_code=st.session_state.current_diag,
                        validation=validation,
                        approved_by='Casey'
                    )
                    
                    if stored:
                        # Count total items stored
                        total_stored = 0
                        for key, value in stored.items():
                            if isinstance(value, list):
                                total_stored += len(value)
                            elif value:
                                total_stored += 1
                        
                        st.success(f"✅ Stored {total_stored} learning item(s)!")
                        st.markdown("**Details:**")
                        for rule_name, value in stored.items():
                            if isinstance(value, list):
                                st.write(f"  - {rule_name}: {len(value)} pairwise relationship(s)")
                            elif value:
                                st.write(f"  - {rule_name}: Stored successfully")
                        st.balloons()
                    else:
                        st.warning("⚠️ No AI rules to store")
                else:
                    st.info("✅ Acknowledged")
        
        with col2:
            if st.button("❌ Reject (Don't Learn)", use_container_width=True):
                st.warning("⚠️ Decision rejected. No learning will be stored.")
                st.write("The AI validation will NOT be added to the learning system.")
        
        with col3:
            if st.button("🔄 Clear", use_container_width=True):
                for key in list(st.session_state.keys()):
                    if key.startswith('current_'):
                        del st.session_state[key]
                st.rerun()

# Sidebar
st.sidebar.title("📊 System Statistics")
st.sidebar.markdown("---")

if 'current_validation' in st.session_state:
    validation = st.session_state.current_validation
    summary = validation.get_summary()
    
    st.sidebar.subheader("📈 Current Validation")
    st.sidebar.metric("Pass Rate", f"{summary['pass_rate']}%")
    st.sidebar.metric("Total Rules", summary['total_rules'])
    st.sidebar.metric("AI Validated", summary['ai_validated'])
    
    # Auto-deny indicator
    if validation.auto_deny:
        st.sidebar.error(f"🚫 Auto-denied ({len(validation.auto_deny_rules)} learned rule(s))")
    
    # Learning table hits
    learning_count = sum(1 for r in validation.rule_results if r.source == "learning_table")
    if learning_count:
        st.sidebar.success(f"📚 {learning_count} rule(s) from learning (saved AI calls)")
    
    # Cost estimate
    ai_cost = summary['ai_validated'] * 0.015
    st.sidebar.metric("Estimated AI Cost", f"${ai_cost:.3f}")

# Help section
st.sidebar.markdown("---")
with st.sidebar.expander("ℹ️ How to Use"):
    st.markdown("""
    **30-Day Validation System**
    
    This system checks for duplicate procedures/diagnoses in the last 30 days:
    
    **1. Exact Duplicates**
    - Same procedure/diagnosis code used within 30 days
    
    **2. Therapeutic Class Duplicates**
    - Different code but same therapeutic class
    - Example: Two different antibiotics
    
    **3. History Display**
    - Shows ALL procedures/diagnoses from last 30 days
    - Source: PA or Claims
    - Highlights duplicates in red/yellow
    
    **4. Classification**
    - Uses master table `procedure_class` when available
    - Falls back to AI classification if any item not in master
    - Ensures consistency across all items
    """)

st.sidebar.markdown("---")
st.sidebar.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.sidebar.caption("Powered by Claude Sonnet 4 + 30-Day Validation")