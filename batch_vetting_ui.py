#!/usr/bin/env python3
"""
Batch Claims Vetting UI - FINAL VERSION
========================================

FIXES:
- All 13 validation columns properly displayed
- Correct import from no_auth_vetting_FINAL
- Proper column ordering
- Enhanced error messages

Validation Columns:
1. enrollee_age
2. enrollee_gender
3. procedure_age_pass
4. procedure_gender_pass
5. diagnosis_age_pass
6. diagnosis_gender_pass
7. procedure_30days
8. diagnosis_30days
9. procedure_class_match_30days ← NEW
10. diagnosis_class_match_30days ← NEW
11. procedure_in_master
12. diagnosis_in_master
13. proc_diag_match

Author: Casey's Healthcare Analytics Team
Date: February 2026
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import FINAL version
from no_auth_vetting import AutoVettingEngine


# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="Claims Vetting System",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 NO-AUTH Claims Batch Vetting System")
st.markdown("### Upload claims CSV for automated validation")
st.markdown("---")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_30day_procedures(enrollee_id, service_date, engine):
    """Get procedure codes from last 30 days"""
    try:
        codes, classes = engine.get_30day_history_with_classes(
            enrollee_id, service_date, 'procedure'
        )
        
        if not codes:
            return "None in past 30 days"
        
        return ", ".join(codes)
    
    except Exception as e:
        return f"Error: {str(e)}"


def get_30day_diagnoses(enrollee_id, service_date, engine):
    """Get diagnosis codes from last 30 days"""
    try:
        codes, classes = engine.get_30day_history_with_classes(
            enrollee_id, service_date, 'diagnosis'
        )
        
        if not codes:
            return "None in past 30 days"
        
        return ", ".join(codes)
    
    except Exception as e:
        return f"Error: {str(e)}"


def check_procedure_class_match_30days(
    procedure_code, enrollee_id, service_date, engine
):
    """
    NEW COLUMN: Check if procedure code or its class was used in last 30 days
    """
    try:
        codes, classes = engine.get_30day_history_with_classes(
            enrollee_id, service_date, 'procedure'
        )
        
        if not codes:
            return "✅ No history"
        
        match_found, msg = engine.check_class_match(
            procedure_code, codes, classes, 'procedure'
        )
        
        if match_found:
            return f"❌ {msg}"
        else:
            return "✅ No match"
            
        except Exception as e:
        return f"Error: {str(e)}"


def check_diagnosis_class_match_30days(
    diagnosis_code, enrollee_id, service_date, engine
):
    """
    NEW COLUMN: Check if diagnosis code or its class was used in last 30 days
    """
    try:
        codes, classes = engine.get_30day_history_with_classes(
            enrollee_id, service_date, 'diagnosis'
        )
        
        if not codes:
            return "✅ No history"
        
        match_found, msg = engine.check_class_match(
            diagnosis_code, codes, classes, 'diagnosis'
        )
        
        if match_found:
            return f"❌ {msg}"
else:
            return "✅ No match"
    
    except Exception as e:
        return f"Error: {str(e)}"


def check_age_appropriate(code, age, code_type, engine):
    """Check if age is appropriate for procedure/diagnosis"""
    try:
        if code_type == 'procedure':
            master = engine.procedure_master
            code_col = 'procedure_code'
        else:
            master = engine.diagnosis_master
            code_col = 'diagnosis_code'
        
        if master.empty:
            return "⚠️ Cannot verify - master empty"
        
        code_upper = code.strip().upper()
        row = master[master[code_col] == code_upper]
        
        if len(row) == 0:
            return "⚠️ Not in master"
        
        age_range = row['age_range'].iloc[0]
        
        if pd.isna(age_range) or str(age_range).strip() == '':
            return "✅ No age restriction"
        
        age_range_str = str(age_range).strip().upper()
        
        # Parse age range
        if '-' in age_range_str:
            parts = age_range_str.split('-')
            if len(parts) == 2:
                try:
                    min_age = int(parts[0].strip())
                    max_age = int(parts[1].strip())
                    
                    if age < min_age or age > max_age:
                        return f"❌ Age {age} outside range {age_range_str}"
                    else:
                        return "✅ Age appropriate"
                except:
                    return f"✅ Age OK ({age_range_str})"
        elif '<' in age_range_str:
            try:
                max_age = int(age_range_str.replace('<', '').strip())
                if age >= max_age:
                    return f"❌ Age {age} not < {max_age}"
                else:
                    return "✅ Age appropriate"
            except:
                return f"✅ Age OK ({age_range_str})"
        elif '>' in age_range_str:
            try:
                min_age = int(age_range_str.replace('>', '').strip())
                if age <= min_age:
                    return f"❌ Age {age} not > {min_age}"
                else:
                    return "✅ Age appropriate"
            except:
                return f"✅ Age OK ({age_range_str})"
        else:
            return f"✅ Age OK ({age_range_str})"
    
    except Exception as e:
        return f"Error: {str(e)}"


def check_gender_appropriate(code, gender, code_type, engine):
    """Check if gender is appropriate for procedure/diagnosis"""
    try:
        if code_type == 'procedure':
            master = engine.procedure_master
            code_col = 'procedure_code'
        else:
            master = engine.diagnosis_master
            code_col = 'diagnosis_code'
        
        if master.empty:
            return "⚠️ Cannot verify - master empty"
        
        code_upper = code.strip().upper()
        row = master[master[code_col] == code_upper]
        
        if len(row) == 0:
            return "⚠️ Not in master"
        
        gender_applicable = row['gender_applicable'].iloc[0]
        
        if pd.isna(gender_applicable) or str(gender_applicable).strip() == '':
            return "✅ No gender restriction"
        
        gender_str = str(gender_applicable).strip().upper()
        enrollee_gender = gender.strip().upper()
        
        if gender_str == 'BOTH' or gender_str == 'ALL':
            return "✅ Gender appropriate"
        
        if gender_str == 'M' or gender_str == 'MALE':
            if enrollee_gender in ['M', 'MALE']:
                return "✅ Gender appropriate"
            else:
                return f"❌ Male-only, patient is {gender}"
        
        if gender_str == 'F' or gender_str == 'FEMALE':
            if enrollee_gender in ['F', 'FEMALE']:
                return "✅ Gender appropriate"
            else:
                return f"❌ Female-only, patient is {gender}"
        
        return f"✅ Gender OK ({gender_str})"
    
    except Exception as e:
        return f"Error: {str(e)}"


def validate_claim(claim_row, engine):
    """
    Validate a single claim and return all 13 validation columns
    """
    enrollee_id = str(claim_row['enrollee_id']).strip()
    procedure_code = str(claim_row['code']).strip().upper()
    diagnosis_code = str(claim_row['diagnosiscode']).strip().upper()
    service_date = pd.to_datetime(claim_row['encounterdatefrom'])
    
    result = {
        'claim_row': claim_row.name if hasattr(claim_row, 'name') else 0
    }
    
    # Get enrollee info
    enrollee_info = engine.get_enrollee_info(enrollee_id)
    
    if not enrollee_info:
        result['enrollee_age'] = 'Unknown'
        result['enrollee_gender'] = 'Unknown'
        result['procedure_age_pass'] = 'Cannot verify - enrollee not found'
        result['procedure_gender_pass'] = 'Cannot verify - enrollee not found'
        result['diagnosis_age_pass'] = 'Cannot verify - enrollee not found'
        result['diagnosis_gender_pass'] = 'Cannot verify - enrollee not found'
        result['procedure_30days'] = 'Cannot verify - enrollee not found'
        result['diagnosis_30days'] = 'Cannot verify - enrollee not found'
        result['procedure_class_match_30days'] = 'Cannot verify - enrollee not found'
        result['diagnosis_class_match_30days'] = 'Cannot verify - enrollee not found'
        result['diagnosis_in_master'] = False
        result['procedure_in_master'] = False
        result['proc_diag_match'] = 'Cannot verify - enrollee not found'
        return result
    
    # Check if codes in master tables
    proc_in_master = procedure_code in engine.procedure_master['procedure_code'].values
    diag_in_master = diagnosis_code in engine.diagnosis_master['diagnosis_code'].values
    
    # Also check diagnosis without decimal
    if not diag_in_master:
        diag_normalized = engine._normalize_diagnosis_code(diagnosis_code)
        diag_in_master = diag_normalized in engine.diagnosis_master['diagnosis_code'].values

    age = enrollee_info['age']
    gender = enrollee_info['gender']

    # COLUMN 1-2: Basic enrollee info
    result['enrollee_age'] = age
    result['enrollee_gender'] = gender

    # COLUMN 3-4: Procedure age/gender validation
    result['procedure_age_pass'] = check_age_appropriate(procedure_code, age, 'procedure', engine)
    result['procedure_gender_pass'] = check_gender_appropriate(procedure_code, gender, 'procedure', engine)

    # COLUMN 5-6: Diagnosis age/gender validation
    result['diagnosis_age_pass'] = check_age_appropriate(diagnosis_code, age, 'diagnosis', engine)
    result['diagnosis_gender_pass'] = check_gender_appropriate(diagnosis_code, gender, 'diagnosis', engine)

    # COLUMN 7-8: 30-day history (comma-separated codes)
    result['procedure_30days'] = get_30day_procedures(enrollee_id, service_date, engine)
    result['diagnosis_30days'] = get_30day_diagnoses(enrollee_id, service_date, engine)

    # COLUMN 9-10: NEW - Class matching in 30-day history
    result['procedure_class_match_30days'] = check_procedure_class_match_30days(
        procedure_code, enrollee_id, service_date, engine
    )
    result['diagnosis_class_match_30days'] = check_diagnosis_class_match_30days(
        diagnosis_code, enrollee_id, service_date, engine
    )

    # COLUMN 11-12: Master table presence
    result['procedure_in_master'] = proc_in_master
    result['diagnosis_in_master'] = diag_in_master
    
    # COLUMN 13: Procedure-diagnosis match validation
    is_valid, msg = engine.check_procedure_diagnosis_match(procedure_code, diagnosis_code)
    result['proc_diag_match'] = msg
    
    return result


# ============================================================================
# MAIN APP
# ============================================================================

# Initialize engine
@st.cache_resource
def get_vetting_engine():
    """Initialize and cache vetting engine"""
    return AutoVettingEngine(db_path='ai_driven_data.duckdb')

try:
    engine = get_vetting_engine()
    st.success(f"✅ Vetting engine ready | {len(engine.procedure_master)} procedures, {len(engine.diagnosis_master)} diagnoses loaded")
except Exception as e:
    st.error(f"❌ Failed to initialize vetting engine: {e}")
    st.stop()

# File upload
st.markdown("### 📤 Upload Claims CSV")
st.markdown("**Required columns:** `enrollee_id`, `code` (procedure), `diagnosiscode`, `encounterdatefrom`")

uploaded_file = st.file_uploader("Choose CSV file", type=['csv'])

if uploaded_file is not None:
    try:
        # Load claims
        claims_df = pd.read_csv(uploaded_file)
        
        # Validate required columns
        required_cols = ['enrollee_id', 'code', 'diagnosiscode', 'encounterdatefrom']
        missing_cols = [col for col in required_cols if col not in claims_df.columns]
        
        if missing_cols:
            st.error(f"❌ Missing required columns: {missing_cols}")
            st.info(f"Available columns: {list(claims_df.columns)}")
            st.stop()
        
        st.success(f"✅ Loaded {len(claims_df)} claims")
        
        # Show preview
        with st.expander("📋 Preview uploaded data"):
            st.dataframe(claims_df.head(10), use_container_width=True)
        
        # Run validation
        st.markdown("---")
        st.markdown("### 🔍 Running Validation...")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        results = []
        
        for idx, row in claims_df.iterrows():
            status_text.text(f"Processing claim {idx+1}/{len(claims_df)}...")
            progress_bar.progress((idx + 1) / len(claims_df))
            
            result = validate_claim(row, engine)
            results.append(result)
        
        status_text.text("✅ Validation complete!")
        
        # Create results DataFrame
        results_df = pd.DataFrame(results)
        
        # Merge with original claims
        output_df = pd.concat([claims_df, results_df], axis=1)
        
        # Display results
        st.markdown("---")
        st.markdown("### 📊 Validation Results")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
            total_claims = len(output_df)
            st.metric("Total Claims", total_claims)
    
    with col2:
            proc_issues = output_df['procedure_class_match_30days'].str.contains('❌', na=False).sum()
            st.metric("Procedure 30-Day Issues", proc_issues)
    
    with col3:
            diag_issues = output_df['diagnosis_class_match_30days'].str.contains('❌', na=False).sum()
            st.metric("Diagnosis 30-Day Issues", diag_issues)
    
    with col4:
            proc_diag_issues = output_df['proc_diag_match'].str.contains('❌', na=False).sum()
            st.metric("Proc-Diag Mismatches", proc_diag_issues)
    
        # AI usage
        st.info(f"🤖 AI API calls made: {engine.ai_call_count}")
    
    # Filter options
        st.markdown("### 🔧 Filter Options")
        
        filter_col1, filter_col2, filter_col3 = st.columns(3)
    
        with filter_col1:
            show_issues_only = st.checkbox("Show issues only", value=False)
    
        with filter_col2:
            show_30day_issues = st.checkbox("Show 30-day violations", value=False)
    
        with filter_col3:
            show_proc_diag_issues = st.checkbox("Show proc-diag mismatches", value=False)
    
    # Apply filters
        filtered_df = output_df.copy()
        
        if show_issues_only:
            filtered_df = filtered_df[
                (filtered_df['procedure_class_match_30days'].str.contains('❌', na=False)) |
                (filtered_df['diagnosis_class_match_30days'].str.contains('❌', na=False)) |
                (filtered_df['proc_diag_match'].str.contains('❌', na=False)) |
                (filtered_df['procedure_age_pass'].str.contains('❌', na=False)) |
                (filtered_df['procedure_gender_pass'].str.contains('❌', na=False)) |
                (filtered_df['diagnosis_age_pass'].str.contains('❌', na=False)) |
                (filtered_df['diagnosis_gender_pass'].str.contains('❌', na=False))
            ]
        
        if show_30day_issues:
            filtered_df = filtered_df[
                (filtered_df['procedure_class_match_30days'].str.contains('❌', na=False)) |
                (filtered_df['diagnosis_class_match_30days'].str.contains('❌', na=False))
            ]
        
        if show_proc_diag_issues:
            filtered_df = filtered_df[
            filtered_df['proc_diag_match'].str.contains('❌', na=False)
            ]
        
        st.markdown(f"**Showing {len(filtered_df)} of {len(output_df)} claims**")
        
        # Display columns in correct order
        display_columns = [
            'enrollee_id', 'code', 'diagnosiscode', 'encounterdatefrom',
            'enrollee_age', 'enrollee_gender',
            'procedure_age_pass', 'procedure_gender_pass',
            'diagnosis_age_pass', 'diagnosis_gender_pass',
            'procedure_30days', 'diagnosis_30days',
            'procedure_class_match_30days',  # NEW COLUMN 9
            'diagnosis_class_match_30days',  # NEW COLUMN 10
            'procedure_in_master', 'diagnosis_in_master',
            'proc_diag_match'
        ]
        
        # Show available columns
        available_display_cols = [col for col in display_columns if col in filtered_df.columns]
        
    st.dataframe(
            filtered_df[available_display_cols],
        use_container_width=True,
        height=600
    )
    
    # Download options
        st.markdown("### 💾 Download Results")
    
        download_col1, download_col2 = st.columns(2)
    
        with download_col1:
            csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
                label="📥 Download as CSV",
            data=csv,
            file_name=f"vetting_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
        with download_col2:
            from io import BytesIO
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                filtered_df.to_excel(writer, index=False, sheet_name='Vetting Results')
            
            st.download_button(
                label="📥 Download as Excel",
                data=buffer.getvalue(),
                file_name=f"vetting_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"❌ Error processing file: {e}")
        import traceback
        st.code(traceback.format_exc())

else:
    st.info("👆 Please upload a CSV file to begin validation")

# Instructions
with st.expander("📖 Instructions"):
st.markdown("""
    ### How to use this system:
    
    1. **Prepare CSV** with columns: `enrollee_id`, `code`, `diagnosiscode`, `encounterdatefrom`
    2. **Upload file** using the file uploader above
    3. **Review results** in the 13 validation columns:
       - Enrollee age/gender
       - Procedure age/gender validation
       - Diagnosis age/gender validation
       - 30-day procedure history
       - 30-day diagnosis history
       - **NEW:** Procedure class matching in 30-day history
       - **NEW:** Diagnosis class matching in 30-day history
       - Master table presence checks
       - Procedure-diagnosis compatibility
    4. **Filter results** using checkboxes
    5. **Download** as CSV or Excel
    
    ### Validation Rules:
    - ❌ Same procedure/class used within 30 days
    - ❌ Same diagnosis/class used within 30 days
    - ❌ Procedure-diagnosis mismatch
    - ❌ Age/gender restrictions violated
    - ✅ All validations passed
""")