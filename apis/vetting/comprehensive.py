#!/usr/bin/env python3
"""
COMPREHENSIVE VALIDATION ENGINE - FIXED
=========================================

CRITICAL FIX: All AI validation prompts now resolve procedure/diagnosis codes
to their actual names from the database BEFORE calling the AI. This prevents
the AI from hallucinating what "DRG1106" means (e.g., confusing it with
standard DRG codes instead of recognizing it as "Amlodipine 10mg").

The fix applies the same name-resolution pattern already used in
vetting_learning_engine.py's validate_procedure_diagnosis() method,
but extends it to all 5 individual rule validators.

Changes from v2.0:
- Added _resolve_procedure_info() helper
- Added _resolve_diagnosis_info() helper  
- Rewrote all 5 AI prompt methods to include resolved names
- Added system prompt establishing internal code context
- AI now receives: code + resolved name + category + patient context

Runs ALL validation rules and provides complete report showing:
- Which rules passed/failed
- Which source validated each (master vs AI)
- Overall decision (ALL must pass for APPROVE)
- Stores AI-approved rules in learning tables

New Learning Tables:
- ai_human_procedure_age
- ai_human_procedure_gender
- ai_human_diagnosis_age
- ai_human_diagnosis_gender

Author: Casey
Date: February 2026
Version: 3.0 - Fixed AI Code Resolution
"""

import os
import duckdb
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)

# Import 30-day validation engine
try:
    from .thirty_day import ThirtyDayValidationEngine, ThirtyDayValidation
    THIRTY_DAY_AVAILABLE = True
except ImportError:
    THIRTY_DAY_AVAILABLE = False
    print("Warning: 30-day validation engine not available")

@dataclass
class RuleResult:
    """Individual rule validation result"""
    rule_name: str  # "PROCEDURE_AGE", "DIAGNOSIS_GENDER", etc.
    passed: bool
    source: str  # "master_table" or "ai"
    confidence: int  # 0-100
    reasoning: str
    details: Dict = field(default_factory=dict)

@dataclass
class ComprehensiveValidation:
    """Complete validation report"""
    overall_decision: str  # "APPROVE" or "DENY"
    overall_confidence: int
    overall_reasoning: str
    rule_results: List[RuleResult]
    requires_human_review: bool
    can_store_ai_approvals: bool  # True if any AI approvals to store
    auto_deny: bool = False  # True if all failed rules are high-confidence learned denials
    auto_deny_rules: List[RuleResult] = field(default_factory=list)  # Which rules triggered it
    
    def get_failed_rules(self) -> List[RuleResult]:
        """Get all failed rules"""
        return [r for r in self.rule_results if not r.passed]
    
    def get_ai_approved_rules(self) -> List[RuleResult]:
        """Get AI-approved rules for storage (DEPRECATED - use get_ai_validated_rules)"""
        return [r for r in self.rule_results if r.passed and r.source == "ai"]
    
    def get_ai_validated_rules(self) -> List[RuleResult]:
        """
        Get ALL AI-validated rules for learning (both passed AND failed)
        
        This is critical for learning from both approvals AND denials:
        - PASSED rules: Learn what IS valid (e.g., DRG9999 IS valid for 45yo)
        - FAILED rules: Learn what IS NOT valid (e.g., DRG2216+DRG1081 ARE same class)
        """
        return [r for r in self.rule_results if r.source == "ai"]
    
    def get_summary(self) -> Dict:
        """Get summary statistics"""
        total = len(self.rule_results)
        passed = sum(1 for r in self.rule_results if r.passed)
        failed = total - passed
        master_validated = sum(1 for r in self.rule_results if r.source == "master_table")
        # Count both fresh AI validations AND learning table hits (past AI approvals)
        ai_validated = sum(1 for r in self.rule_results if r.source in ["ai", "learning_table"])
        
        return {
            'total_rules': total,
            'passed': passed,
            'failed': failed,
            'master_validated': master_validated,
            'ai_validated': ai_validated,
            'pass_rate': round(passed / total * 100, 1) if total > 0 else 0
        }


class ComprehensiveVettingEngine:
    """
    Validation engine that runs ALL rules and provides complete report
    """
    
    # =====================================================================
    # SYSTEM PROMPT: Establishes context for ALL AI validation calls
    # This prevents the AI from misinterpreting internal codes like DRG1106
    # =====================================================================
    AI_SYSTEM_PROMPT = """You are a medical validation AI for Clearline International, a Health Maintenance Organization (HMO) in Nigeria.

CRITICAL CONTEXT:
- Procedure codes starting with "DRG" are INTERNAL codes mapped to specific drugs/procedures in our database.
- They are NOT standard DRG (Diagnosis Related Group) codes. Never interpret them as DRG classifications.
- When a procedure name is provided alongside a code, ALWAYS use the procedure name for your medical assessment.
- Diagnosis codes follow ICD-10 standards.

YOUR ROLE:
- Validate medical appropriateness based on the RESOLVED NAME provided, not the code format.
- Never comment on code format or validity — codes have already been verified in our database.
- Focus purely on clinical/medical judgment.

Respond ONLY in valid JSON format. No markdown, no backticks, no extra text."""

    def __init__(self, db_path: str = "ai_driven_data.duckdb"):
        """Initialize engine"""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path, read_only=False)
        
        # Import from existing engine
        from .learning_engine import LearningVettingEngine
        self.base_engine = LearningVettingEngine(db_path)
        
        # Initialize 30-day validation engine
        if THIRTY_DAY_AVAILABLE:
            # Pass our connection to prevent multiple connection conflicts
            self.thirty_day_engine = ThirtyDayValidationEngine(db_path, conn=self.conn, learning_engine=self.base_engine)
        else:
            self.thirty_day_engine = None
        
        # Create new learning tables
        self._create_learning_tables()
    
    def _create_learning_tables(self):
        """Create all 6 learning tables with correct schemas.
        
        FIXED v3.3: Force-drops tables with known schema issues:
        - Age tables: old single-column PK → new composite PK
        - procedure_diagnosis: external `id` NOT NULL column → no id column
        - Also creates procedure_class table (was missing)
        """
        
        self.conn.execute('CREATE SCHEMA IF NOT EXISTS "PROCEDURE_DIAGNOSIS"')
        
        # Force-drop tables with known schema issues
        # Safe because no data was successfully stored in previous versions
        tables_to_reset = [
            'ai_human_procedure_age',
            'ai_human_diagnosis_age', 
            'ai_human_procedure_diagnosis',
            'ai_human_procedure_class'
        ]
        
        for table in tables_to_reset:
            try:
                # Check if table exists AND has wrong schema
                exists = self.conn.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'PROCEDURE_DIAGNOSIS' 
                    AND table_name = '{table}'
                """).fetchone()[0]
                
                if exists:
                    # Test if our INSERT pattern works
                    if table == 'ai_human_procedure_age':
                        test = """INSERT INTO "PROCEDURE_DIAGNOSIS"."ai_human_procedure_age"
                            (procedure_code, min_age, max_age, is_valid_for_age, reason, confidence, ai_reasoning, approved_by, usage_count)
                            VALUES ('__TEST__', 0, 0, false, '', 0, '', '', 0)
                            ON CONFLICT (procedure_code, min_age, max_age) DO NOTHING"""
                    elif table == 'ai_human_diagnosis_age':
                        test = """INSERT INTO "PROCEDURE_DIAGNOSIS"."ai_human_diagnosis_age"
                            (diagnosis_code, min_age, max_age, is_valid_for_age, reason, confidence, ai_reasoning, approved_by, usage_count)
                            VALUES ('__TEST__', 0, 0, false, '', 0, '', '', 0)
                            ON CONFLICT (diagnosis_code, min_age, max_age) DO NOTHING"""
                    elif table == 'ai_human_procedure_class':
                        test = """INSERT INTO "PROCEDURE_DIAGNOSIS"."ai_human_procedure_class"
                            (procedure_code_1, procedure_code_2, shared_class, same_class, ai_confidence, ai_reasoning, approved_by, usage_count)
                            VALUES ('__TEST__', '__TEST2__', '', false, 0, '', '', 0)
                            ON CONFLICT (procedure_code_1, procedure_code_2) DO NOTHING"""
                    else:  # procedure_diagnosis
                        test = """INSERT INTO "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis"
                            (procedure_code, diagnosis_code, is_valid_match, match_reason, ai_confidence, ai_reasoning, approved_by, usage_count)
                            VALUES ('__TEST__', '__TEST__', false, '', 0, '', '', 0)
                            ON CONFLICT (procedure_code, diagnosis_code) DO NOTHING"""
                    
                    try:
                        self.conn.execute(test)
                        # Worked — clean up test row
                        if table == 'ai_human_procedure_class':
                            self.conn.execute(f'DELETE FROM "PROCEDURE_DIAGNOSIS"."{table}" WHERE procedure_code_1 = \'__TEST__\'')
                        elif 'diagnosis' in table and 'procedure' not in table:
                            self.conn.execute(f'DELETE FROM "PROCEDURE_DIAGNOSIS"."{table}" WHERE diagnosis_code = \'__TEST__\'')
                        elif 'procedure_diagnosis' in table:
                            self.conn.execute(f'DELETE FROM "PROCEDURE_DIAGNOSIS"."{table}" WHERE procedure_code = \'__TEST__\' AND diagnosis_code = \'__TEST__\'')
                        else:
                            self.conn.execute(f'DELETE FROM "PROCEDURE_DIAGNOSIS"."{table}" WHERE procedure_code = \'__TEST__\'')
                    except Exception as e:
                        # Schema is wrong — drop it
                        logger.info(f"Dropping {table} due to schema mismatch: {e}")
                        self.conn.execute(f'DROP TABLE "PROCEDURE_DIAGNOSIS"."{table}"')
            except Exception as e:
                logger.warning(f"Migration check failed for {table}: {e}")
        
        # --- TABLE 1: PROCEDURE AGE ---
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_procedure_age" (
                procedure_code VARCHAR,
                min_age INTEGER,
                max_age INTEGER,
                is_valid_for_age BOOLEAN,
                reason TEXT,
                confidence INTEGER,
                ai_reasoning TEXT,
                approved_by VARCHAR,
                approved_date TIMESTAMP,
                usage_count INTEGER DEFAULT 0,
                last_used TIMESTAMP,
                PRIMARY KEY (procedure_code, min_age, max_age)
            )
        ''')
        
        # --- TABLE 2: PROCEDURE GENDER ---
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_procedure_gender" (
                procedure_code VARCHAR,
                gender VARCHAR,
                is_valid_for_gender BOOLEAN,
                reason TEXT,
                confidence INTEGER,
                ai_reasoning TEXT,
                approved_by VARCHAR,
                approved_date TIMESTAMP,
                usage_count INTEGER DEFAULT 0,
                last_used TIMESTAMP,
                PRIMARY KEY (procedure_code, gender)
            )
        ''')
        
        # --- TABLE 3: DIAGNOSIS AGE ---
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_diagnosis_age" (
                diagnosis_code VARCHAR,
                min_age INTEGER,
                max_age INTEGER,
                is_valid_for_age BOOLEAN,
                reason TEXT,
                confidence INTEGER,
                ai_reasoning TEXT,
                approved_by VARCHAR,
                approved_date TIMESTAMP,
                usage_count INTEGER DEFAULT 0,
                last_used TIMESTAMP,
                PRIMARY KEY (diagnosis_code, min_age, max_age)
            )
        ''')
        
        # --- TABLE 4: DIAGNOSIS GENDER ---
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_diagnosis_gender" (
                diagnosis_code VARCHAR,
                gender VARCHAR,
                is_valid_for_gender BOOLEAN,
                reason TEXT,
                confidence INTEGER,
                ai_reasoning TEXT,
                approved_by VARCHAR,
                approved_date TIMESTAMP,
                usage_count INTEGER DEFAULT 0,
                last_used TIMESTAMP,
                PRIMARY KEY (diagnosis_code, gender)
            )
        ''')
        
        # --- TABLE 5: PROCEDURE-DIAGNOSIS COMPATIBILITY ---
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis" (
                procedure_code VARCHAR,
                diagnosis_code VARCHAR,
                is_valid_match BOOLEAN,
                match_reason TEXT,
                ai_confidence INTEGER,
                ai_reasoning TEXT,
                approved_by VARCHAR,
                approved_date TIMESTAMP,
                usage_count INTEGER DEFAULT 0,
                last_used_date TIMESTAMP,
                PRIMARY KEY (procedure_code, diagnosis_code)
            )
        ''')
        
        # --- TABLE 6: PROCEDURE CLASS (for 30-day duplicate learning) ---
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_procedure_class" (
                procedure_code_1 VARCHAR,
                procedure_code_2 VARCHAR,
                shared_class VARCHAR,
                same_class BOOLEAN,
                ai_confidence INTEGER,
                ai_reasoning TEXT,
                approved_by VARCHAR,
                approved_date TIMESTAMP,
                usage_count INTEGER DEFAULT 0,
                last_used_date TIMESTAMP,
                PRIMARY KEY (procedure_code_1, procedure_code_2)
            )
        ''')
    
    # ==================================================================
    # CODE RESOLUTION HELPERS (THE CRITICAL FIX)
    # ==================================================================
    # These methods resolve internal codes to their actual names before
    # the AI ever sees them. This is what was missing in v2.0.
    # ==================================================================
    
    def _resolve_procedure_info(self, procedure_code: str) -> Dict:
        """
        Resolve a procedure code to its actual name and category.
        
        Checks: PROCEDURE_MASTER → PROCEDURE DATA (comprehensive) → returns raw code
        
        This is THE critical fix — without this, the AI sees "DRG1106" and 
        hallucinates it as a standard DRG code instead of recognizing it as 
        Amlodipine 10mg.
        
        Returns:
            Dict with keys: code, name, category, source, found
        """
        # Try master table first (has curated clinical info)
        proc_master = self.base_engine.check_procedure_master(procedure_code)
        if proc_master:
            return {
                'code': procedure_code,
                'name': proc_master.get('name', procedure_code),
                'category': proc_master.get('class', 'Unknown'),
                'source': 'master_table',
                'found': True
            }
        
        # Try comprehensive table (PROCEDURE DATA)
        proc_comp = self.base_engine._get_procedure_from_comprehensive(procedure_code)
        if proc_comp:
            return {
                'code': procedure_code,
                'name': proc_comp.get('name', procedure_code),
                'category': proc_comp.get('category', 'Unknown'),
                'source': 'comprehensive_table',
                'found': True
            }
        
        # Not found anywhere — return raw code with warning
        return {
            'code': procedure_code,
            'name': procedure_code,  # Raw code as fallback
            'category': 'Unknown',
            'source': 'not_found',
            'found': False
        }
    
    def _resolve_diagnosis_info(self, diagnosis_code: str) -> Dict:
        """
        Resolve a diagnosis code to its actual name and category.
        
        Checks: DIAGNOSIS_MASTER → DIAGNOSIS (comprehensive) → returns raw code
        
        Returns:
            Dict with keys: code, name, category, source, found
        """
        # Try master table first
        diag_master = self.base_engine.check_diagnosis_master(diagnosis_code)
        if diag_master:
            return {
                'code': diagnosis_code,
                'name': diag_master.get('name', diagnosis_code),
                'category': diag_master.get('class', 'Unknown'),
                'source': 'master_table',
                'found': True
            }
        
        # Try comprehensive table
        diag_comp = self.base_engine._get_diagnosis_from_comprehensive(diagnosis_code)
        if diag_comp:
            return {
                'code': diagnosis_code,
                'name': diag_comp.get('name', diagnosis_code),
                'category': diag_comp.get('category', 'Unknown'),
                'source': 'comprehensive_table',
                'found': True
            }
        
        # Not found
        return {
            'code': diagnosis_code,
            'name': diagnosis_code,
            'category': 'Unknown',
            'source': 'not_found',
            'found': False
        }
    
    # ==================================================================
    # AI VALIDATION METHODS (ALL FIXED WITH NAME RESOLUTION)
    # ==================================================================
    
    def _ai_validate_procedure_age(self, procedure_code: str, age: int, 
                                    proc_info: Optional[Dict] = None) -> Dict:
        """
        Call AI to validate procedure age appropriateness.
        
        FIXED: Now resolves procedure code to actual name before calling AI.
        """
        import json
        
        # Resolve procedure name if not already provided
        if not proc_info:
            proc_info = self._resolve_procedure_info(procedure_code)
        
        proc_name = proc_info['name']
        proc_category = proc_info['category']
        
        prompt = f"""Validate if this procedure/medication is appropriate for this patient's age.

Procedure Code: {procedure_code}
Procedure Name: {proc_name}
Therapeutic Category: {proc_category}
Patient Age: {age} years

IMPORTANT: Base your assessment on the PROCEDURE NAME "{proc_name}", not the code format.

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "Brief explanation referencing the actual drug/procedure name"
}}

Rules:
- APPROVE if {proc_name} is safe and appropriate for a {age}-year-old patient
- DENY only if there is a clear age contraindication (e.g., adult-only drug for a child, pediatric formulation for elderly)
- Age restrictions: pediatric formulations <12yo, adult dosing standard, geriatric caution >65yo
- If age is within normal adult range and no specific restriction exists → APPROVE"""

        try:
            result = self.base_engine._call_claude_for_validation(prompt)
            return {
                'is_valid': result.get('suggested_action') == 'APPROVE',
                'confidence': result.get('confidence', 70),
                'reasoning': result.get('reasoning', 'AI validation completed')
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}'
            }
    
    def _ai_validate_procedure_gender(self, procedure_code: str, gender: str,
                                       proc_info: Optional[Dict] = None) -> Dict:
        """
        Call AI to validate procedure gender appropriateness.
        
        FIXED: Now resolves procedure code to actual name before calling AI.
        """
        if not proc_info:
            proc_info = self._resolve_procedure_info(procedure_code)
        
        proc_name = proc_info['name']
        proc_category = proc_info['category']
        
        prompt = f"""Validate if this procedure/medication is appropriate for this patient's gender.

Procedure Code: {procedure_code}
Procedure Name: {proc_name}
Therapeutic Category: {proc_category}
Patient Gender: {gender}

IMPORTANT: Base your assessment on the PROCEDURE NAME "{proc_name}", not the code format.

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "Brief explanation referencing the actual drug/procedure name"
}}

Rules:
- APPROVE if {proc_name} can be used by a {gender} patient
- DENY only if there is an ANATOMICAL impossibility (e.g., prostate medication for female, pregnancy drug for male)
- Most medications are appropriate for both genders → APPROVE
- Only flag gender-specific drugs: pregnancy/ovarian/cervical/uterine = female only, prostate/testicular = male only"""

        try:
            result = self.base_engine._call_claude_for_validation(prompt)
            return {
                'is_valid': result.get('suggested_action') == 'APPROVE',
                'confidence': result.get('confidence', 70),
                'reasoning': result.get('reasoning', 'AI validation completed')
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}'
            }
    
    def _ai_validate_diagnosis_age(self, diagnosis_code: str, age: int,
                                    diag_info: Optional[Dict] = None) -> Dict:
        """
        Call AI to validate diagnosis age appropriateness.
        
        FIXED: Now resolves diagnosis code to actual name before calling AI.
        Also handles unresolved codes by instructing AI to interpret ICD-10.
        """
        if not diag_info:
            diag_info = self._resolve_diagnosis_info(diagnosis_code)
        
        diag_name = diag_info['name']
        diag_category = diag_info['category']
        code_resolved = diag_info.get('found', False)
        
        # Build name context
        if code_resolved and diag_name != diagnosis_code:
            name_instruction = f"""Diagnosis Code: {diagnosis_code}
Diagnosis Name: {diag_name}
Category: {diag_category}

IMPORTANT: Base your assessment on the DIAGNOSIS NAME "{diag_name}", not the code format."""
        else:
            name_instruction = f"""Diagnosis Code: {diagnosis_code}

NOTE: This code was not found in our internal database.
You MUST interpret this as an ICD-10 diagnosis code and identify the condition it represents.
First state what condition {diagnosis_code} represents, then assess age appropriateness.
If you cannot identify the ICD-10 code, DENY with low confidence."""
        
        prompt = f"""Validate if this diagnosis is medically plausible for this patient's age.

{name_instruction}
Patient Age: {age} years

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "First identify the condition, then explain age appropriateness"
}}

Rules:
- APPROVE if the condition can plausibly occur in a {age}-year-old patient
- Even if a condition is UNCOMMON at this age, APPROVE unless medically virtually impossible
- DENY only if the condition is near-impossible at this age (e.g., Alzheimer's in a 5-year-old, menopause in a child)
- Age-unlikely ≠ Age-impossible. Unlikely conditions should still APPROVE"""

        try:
            result = self.base_engine._call_claude_for_validation(prompt)
            return {
                'is_valid': result.get('suggested_action') == 'APPROVE',
                'confidence': result.get('confidence', 70),
                'reasoning': result.get('reasoning', 'AI validation completed')
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}'
            }
    
    def _ai_validate_diagnosis_gender(self, diagnosis_code: str, gender: str,
                                       diag_info: Optional[Dict] = None) -> Dict:
        """
        Call AI to validate diagnosis gender appropriateness.
        
        FIXED: Now resolves diagnosis code to actual name before calling AI.
        Also handles unresolved codes by instructing AI to interpret ICD-10.
        """
        if not diag_info:
            diag_info = self._resolve_diagnosis_info(diagnosis_code)
        
        diag_name = diag_info['name']
        diag_category = diag_info['category']
        code_resolved = diag_info.get('found', False)
        
        # Build name context - if name wasn't resolved, tell AI to interpret the code
        if code_resolved and diag_name != diagnosis_code:
            name_instruction = f"""Diagnosis Code: {diagnosis_code}
Diagnosis Name: {diag_name}
Category: {diag_category}

IMPORTANT: Base your assessment on the DIAGNOSIS NAME "{diag_name}", not the code format."""
        else:
            name_instruction = f"""Diagnosis Code: {diagnosis_code}

NOTE: This code was not found in our internal database. 
You MUST interpret this as an ICD-10 diagnosis code and identify the condition it represents.
First state what condition {diagnosis_code} represents, then assess gender appropriateness.
If you cannot identify the ICD-10 code, DENY with low confidence."""
        
        prompt = f"""Validate if this diagnosis is appropriate for this patient's gender.

{name_instruction}
Patient Gender: {gender}

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "First identify the condition, then explain gender appropriateness"
}}

STRICT GENDER RULES:
- Female-only conditions (DENY if patient is Male): pregnancy, ovarian cancer, cervical cancer, uterine conditions, vaginitis, endometriosis
- Male-only conditions (DENY if patient is Female): prostate cancer, testicular cancer, penile conditions
- A {gender} patient diagnosed with a condition of the OPPOSITE gender's anatomy = DENY (anatomically impossible)
- Most conditions can occur in both genders → APPROVE
- Only DENY for anatomical impossibilities"""

        try:
            result = self.base_engine._call_claude_for_validation(prompt)
            return {
                'is_valid': result.get('suggested_action') == 'APPROVE',
                'confidence': result.get('confidence', 70),
                'reasoning': result.get('reasoning', 'AI validation completed')
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}'
            }
    
    def _ai_validate_proc_diag_compatibility(self, procedure_code: str, diagnosis_code: str, 
                                            age: Optional[int] = None, gender: Optional[str] = None,
                                            proc_info: Optional[Dict] = None,
                                            diag_info: Optional[Dict] = None) -> Dict:
        """
        Two-phase AI validation for procedure-diagnosis compatibility.
        
        Phase 1: Fast Haiku call (no PubMed) — handles ~80% of cases
        Phase 2: If Haiku confidence < 75%, search PubMed and re-evaluate
                 with real clinical evidence — handles edge cases
        """
        if not proc_info:
            proc_info = self._resolve_procedure_info(procedure_code)
        if not diag_info:
            diag_info = self._resolve_diagnosis_info(diagnosis_code)
        
        proc_name = proc_info['name']
        proc_category = proc_info['category']
        diag_name = diag_info['name']
        diag_category = diag_info['category']
        proc_resolved = proc_info.get('found', False)
        diag_resolved = diag_info.get('found', False)
        
        context_str = ""
        if age:
            context_str += f"\nPatient Age: {age} years"
        if gender:
            context_str += f"\nPatient Gender: {gender}"
        
        # Build procedure section
        if proc_resolved and proc_name != procedure_code:
            proc_section = f"""Procedure Code: {procedure_code}
Procedure Name: {proc_name}
Therapeutic Category: {proc_category}"""
        else:
            proc_section = f"""Procedure Code: {procedure_code}
(Not found in internal database - interpret as standard procedure/drug code)"""
        
        # Build diagnosis section
        if diag_resolved and diag_name != diagnosis_code:
            diag_section = f"""Diagnosis Code: {diagnosis_code}
Diagnosis Name: {diag_name}
Diagnosis Category: {diag_category}"""
        else:
            diag_section = f"""Diagnosis Code: {diagnosis_code}
(Not found in internal database - interpret as ICD-10 diagnosis code. You MUST identify the condition first.)"""
        
        # Use resolved names in the prompt, fall back to codes
        proc_display = proc_name if (proc_resolved and proc_name != procedure_code) else procedure_code
        diag_display = diag_name if (diag_resolved and diag_name != diagnosis_code) else diagnosis_code
        
        # Build the core validation prompt (shared by both phases)
        validation_instructions = f"""
IMPORTANT: "Procedure" covers THREE types — each has different validation logic:

1. MEDICATION → Is it used IN THE TREATMENT of this diagnosis?
   This includes ALL of the following:
   - Primary treatment (directly targets the disease)
   - Symptomatic relief (treats symptoms that accompany the condition)
   - Supportive care (aids recovery, boosts immunity, prevents complications)
   - Adjunctive therapy (commonly co-prescribed alongside primary treatment)
   
   ✅ Artemether for Malaria (primary — kills the parasite)
   ✅ Paracetamol for Malaria (symptomatic — treats fever/body pain)
   ✅ Vitamin C for Malaria (supportive — immune support during recovery)
   ✅ Omeprazole for Peptic Ulcer (primary — reduces acid)
   ✅ Antacid for Gastritis (symptomatic — neutralizes acid)
   ❌ Amlodipine for Ovarian Cancer (not part of cancer treatment at all)
   ❌ Metformin for Fracture (diabetes drug, no role in fracture treatment)

2. LABORATORY TEST → Does it INVESTIGATE, CONFIRM, or MONITOR this diagnosis?
   ✅ Full Blood Count for Anaemia (confirms with haemoglobin levels)
   ✅ Malaria Thick/Thin Film for Plasmodium Falciparum (confirms parasites)
   ✅ Full Blood Count for URTI (checks WBC for infection)
   ✅ Full Blood Count for Malaria (monitors haemoglobin, platelet count)
   ✅ Liver Function Test for Hepatitis (confirms liver damage)
   ❌ Malaria Film for Fracture (irrelevant investigation)

3. SURGICAL PROCEDURE → Is it an appropriate INTERVENTION for this condition?
   ✅ Myomectomy for Uterine Fibroids (removes the fibroids)
   ✅ Appendectomy for Acute Appendicitis (removes inflamed appendix)
   ❌ Appendectomy for Malaria (wrong intervention entirely)

VALIDATION RULES:
- First identify the PROCEDURE TYPE (medication, lab, or surgery)
- For MEDICATIONS: APPROVE if it plays ANY role in treating the patient with this diagnosis — primary treatment, symptom management, supportive care, or standard co-prescription. Ask: "Would a doctor reasonably prescribe this for a patient with this diagnosis?"
- For LAB TESTS: APPROVE if it is a clinically relevant investigation that helps diagnose, monitor, or rule out the condition
- For SURGICAL PROCEDURES: APPROVE if it is an appropriate intervention for the condition
- DENY only if there is NO clinical reason to use this procedure for a patient with this diagnosis"""

        # ==================================================================
        # PHASE 1: Fast Haiku call (no PubMed)
        # ==================================================================
        phase1_prompt = f"""Validate if this procedure is CLINICALLY APPROPRIATE for a patient diagnosed with this condition.

{proc_section}

{diag_section}{context_str}
{validation_instructions}

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "procedure_type": "medication" or "lab_test" or "surgical",
  "reasoning": "State the procedure type and whether {proc_display} is clinically appropriate for a patient with {diag_display}. If approving, state the role (primary/symptomatic/supportive). If denying, state what {proc_display} is actually used for."
}}"""

        try:
            phase1_result = self.base_engine._call_claude_for_validation(phase1_prompt)
            phase1_confidence = phase1_result.get('confidence', 0)
            phase1_action = phase1_result.get('suggested_action', 'DENY')
            
            # ==============================================================
            # PHASE 2: PubMed second opinion (ONLY if confidence < 75%)
            # ==============================================================
            PUBMED_THRESHOLD = 75
            pubmed_evidence = {'articles': [], 'count': 0, 'query': ''}
            evidence_summary = []
            
            if phase1_confidence < PUBMED_THRESHOLD:
                print(f"🔬 Phase 1 confidence {phase1_confidence}% < {PUBMED_THRESHOLD}% — searching PubMed for evidence...")
                
                pubmed_evidence = self.base_engine.search_pubmed_evidence(proc_name, diag_name)
                evidence_section = self.base_engine.format_pubmed_for_prompt(pubmed_evidence)
                
                if pubmed_evidence.get('count', 0) > 0 and evidence_section:
                    # Re-ask Haiku WITH evidence
                    phase2_prompt = f"""Validate if this procedure is CLINICALLY APPROPRIATE for a patient diagnosed with this condition.

{proc_section}

{diag_section}{context_str}

{evidence_section}
{validation_instructions}
- If PubMed evidence supports the combination, cite the PMID(s) in your reasoning
- If the evidence contradicts the combination, explain why

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "procedure_type": "medication" or "lab_test" or "surgical",
  "reasoning": "State the procedure type and whether {proc_display} is clinically appropriate for a patient with {diag_display}. Cite any relevant PubMed PMID(s)."
}}"""
                    
                    phase2_result = self.base_engine._call_claude_for_validation(phase2_prompt)
                    
                    # Use Phase 2 result (evidence-informed)
                    final_result = phase2_result
                    final_reasoning = phase2_result.get('reasoning', '') + f" [PubMed: {pubmed_evidence['count']} article(s) reviewed]"
                    print(f"🔬 Phase 2 complete — confidence now {phase2_result.get('confidence', 0)}% (was {phase1_confidence}%)")
                else:
                    # No PubMed results found — stick with Phase 1
                    final_result = phase1_result
                    final_reasoning = phase1_result.get('reasoning', '') + " [PubMed: no articles found]"
                    print(f"🔬 PubMed returned no results — keeping Phase 1 decision")
            else:
                # Phase 1 was confident enough — no PubMed needed
                final_result = phase1_result
                final_reasoning = phase1_result.get('reasoning', '')
            
            # Build evidence summary for UI
            for article in pubmed_evidence.get('articles', []):
                evidence_summary.append({
                    'pmid': article['pmid'],
                    'title': article['title'],
                    'year': article['year'],
                    'authors': article['authors']
                })
            
            return {
                'is_valid': final_result.get('suggested_action') == 'APPROVE',
                'confidence': final_result.get('confidence', 70),
                'reasoning': final_reasoning,
                'pubmed_evidence': evidence_summary,
                'pubmed_query': pubmed_evidence.get('query', ''),
                'pubmed_count': pubmed_evidence.get('count', 0),
                'pubmed_triggered': phase1_confidence < PUBMED_THRESHOLD
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}',
                'pubmed_evidence': [],
                'pubmed_count': 0,
                'pubmed_triggered': False
            }
    
    def validate_comprehensive(
        self,
        procedure_code: str,
        diagnosis_code: str,
        enrollee_id: Optional[str] = None,
        encounter_date: Optional[str] = None
    ) -> ComprehensiveValidation:
        """
        Run ALL validation rules and return comprehensive report
        
        FIXED: Resolves procedure and diagnosis names ONCE upfront,
        then passes them to all individual rule validators.
        
        Validation Rules:
        1. Procedure Age (if enrollee age known)
        2. Procedure Gender (if enrollee gender known)
        3. Diagnosis Age (if enrollee age known)
        4. Diagnosis Gender (if enrollee gender known)
        5. Procedure-Diagnosis Compatibility
        6. Procedure 30-Day Duplicate Check (exact + therapeutic class)
        
        Returns comprehensive report with all rule results
        """
        rule_results = []
        
        # Get enrollee context
        enrollee_context = None
        if enrollee_id:
            enrollee_context = self.base_engine.get_enrollee_context(enrollee_id, encounter_date)
        
        # Get master table entries
        proc_master = self.base_engine.check_procedure_master(procedure_code)
        diag_master = self.base_engine.check_diagnosis_master(diagnosis_code)
        
        # ==============================================================
        # CRITICAL FIX: Resolve names ONCE, reuse across all AI calls
        # This is the single most important change in this file.
        # ==============================================================
        proc_info = self._resolve_procedure_info(procedure_code)
        diag_info = self._resolve_diagnosis_info(diagnosis_code)
        
        # ===================================================================
        # RULE 1: PROCEDURE AGE VALIDATION
        # ===================================================================
        if enrollee_context and enrollee_context.age is not None:
            if proc_master:
                # Master table validation
                proc_age_result = self.base_engine.validate_age_for_procedure(
                    procedure_code, enrollee_context.age
                )
                rule_results.append(RuleResult(
                    rule_name="PROCEDURE_AGE",
                    passed=proc_age_result.get("is_valid", True),
                    source="master_table",
                    confidence=100,
                    reasoning=proc_age_result.get("reasoning", "Age validated by master table"),
                    details=proc_age_result
                ))
            else:
                # Check learning table first
                age_learning = self.base_engine.check_procedure_age_learning(
                    procedure_code, enrollee_context.age
                )
                if age_learning:
                    # Format reasoning based on whether it's approval or denial
                    is_valid = age_learning['is_valid']
                    stored_reason = age_learning.get('reason', '')
                    min_age = age_learning.get('min_age')
                    max_age = age_learning.get('max_age')
                    
                    if is_valid:
                        reasoning = f"✅ Valid for age {enrollee_context.age} (learned approval: {stored_reason})"
                    else:
                        reasoning = f"❌ AGE RESTRICTION: {procedure_code} is invalid for age {enrollee_context.age} (learned denial: {stored_reason})"
                    
                    rule_results.append(RuleResult(
                        rule_name="PROCEDURE_AGE",
                        passed=is_valid,
                        source="learning_table",
                        confidence=age_learning.get('confidence', 95),
                        reasoning=reasoning,
                        details={
                            **age_learning,
                            "enrollee_age": enrollee_context.age,
                            "learned_from_previous": True
                        }
                    ))
                else:
                    # Not in master or learning - CALL AI with RESOLVED NAME
                    ai_result = self._ai_validate_procedure_age(
                        procedure_code, enrollee_context.age, proc_info=proc_info
                    )
                    
                    rule_results.append(RuleResult(
                        rule_name="PROCEDURE_AGE",
                        passed=ai_result['is_valid'],
                        source="ai",
                        confidence=ai_result['confidence'],
                        reasoning=ai_result['reasoning'],
                        details={
                            "enrollee_age": enrollee_context.age,
                            "ai_validated": True,
                            "validation_type": "procedure_age",
                            "resolved_name": proc_info['name'],
                            "resolved_category": proc_info['category']
                        }
                    ))
        
        # ===================================================================
        # RULE 2: PROCEDURE GENDER VALIDATION
        # ===================================================================
        if enrollee_context and enrollee_context.gender:
            if proc_master:
                # Master table validation
                proc_gender_result = self.base_engine.validate_gender_for_procedure(
                    procedure_code, enrollee_context.gender
                )
                rule_results.append(RuleResult(
                    rule_name="PROCEDURE_GENDER",
                    passed=proc_gender_result.get("is_valid", True),
                    source="master_table",
                    confidence=100 if not proc_gender_result.get("is_valid", True) else 95,
                    reasoning=proc_gender_result.get("reasoning", "Gender validated by master table"),
                    details=proc_gender_result
                ))
            else:
                # Check learning table first
                gender_learning = self.base_engine.check_procedure_gender_learning(
                    procedure_code, enrollee_context.gender
                )
                if gender_learning:
                    is_valid = gender_learning['is_valid']
                    stored_reason = gender_learning.get('reason', '')
                    
                    if is_valid:
                        reasoning = f"✅ Valid for {enrollee_context.gender} (learned approval: {stored_reason})"
                    else:
                        reasoning = f"❌ GENDER RESTRICTION: {procedure_code} is invalid for {enrollee_context.gender} (learned denial: {stored_reason})"
                    
                    rule_results.append(RuleResult(
                        rule_name="PROCEDURE_GENDER",
                        passed=is_valid,
                        source="learning_table",
                        confidence=gender_learning.get('confidence', 95),
                        reasoning=reasoning,
                        details={
                            **gender_learning,
                            "enrollee_gender": enrollee_context.gender,
                            "learned_from_previous": True
                        }
                    ))
                else:
                    # Not in master or learning - CALL AI with RESOLVED NAME
                    ai_result = self._ai_validate_procedure_gender(
                        procedure_code, enrollee_context.gender, proc_info=proc_info
                    )
                    
                    rule_results.append(RuleResult(
                        rule_name="PROCEDURE_GENDER",
                        passed=ai_result['is_valid'],
                        source="ai",
                        confidence=ai_result['confidence'],
                        reasoning=ai_result['reasoning'],
                        details={
                            "enrollee_gender": enrollee_context.gender,
                            "ai_validated": True,
                            "validation_type": "procedure_gender",
                            "resolved_name": proc_info['name'],
                            "resolved_category": proc_info['category']
                        }
                    ))
        
        # ===================================================================
        # RULE 3: DIAGNOSIS AGE VALIDATION
        # ===================================================================
        if enrollee_context and enrollee_context.age is not None:
            if diag_master:
                # Master table validation
                diag_age_result = self.base_engine.validate_age_for_diagnosis(
                    diagnosis_code, enrollee_context.age
                )
                rule_results.append(RuleResult(
                    rule_name="DIAGNOSIS_AGE",
                    passed=diag_age_result.get("is_valid", True),
                    source="master_table",
                    confidence=95,
                    reasoning=diag_age_result.get("reasoning", "Age validated by master table"),
                    details=diag_age_result
                ))
            else:
                # Check learning table first
                age_learning = self.base_engine.check_diagnosis_age_learning(
                    diagnosis_code, enrollee_context.age
                )
                if age_learning:
                    is_valid = age_learning['is_valid']
                    stored_reason = age_learning.get('reason', '')
                    
                    if is_valid:
                        reasoning = f"✅ Valid for age {enrollee_context.age} (learned approval: {stored_reason})"
                    else:
                        reasoning = f"❌ AGE RESTRICTION: {diagnosis_code} is invalid for age {enrollee_context.age} (learned denial: {stored_reason})"
                    
                    rule_results.append(RuleResult(
                        rule_name="DIAGNOSIS_AGE",
                        passed=is_valid,
                        source="learning_table",
                        confidence=age_learning.get('confidence', 90),
                        reasoning=reasoning,
                        details={
                            **age_learning,
                            "enrollee_age": enrollee_context.age,
                            "learned_from_previous": True
                        }
                    ))
                else:
                    # Not in master or learning - CALL AI with RESOLVED NAME
                    ai_result = self._ai_validate_diagnosis_age(
                        diagnosis_code, enrollee_context.age, diag_info=diag_info
                    )
                    
                    rule_results.append(RuleResult(
                        rule_name="DIAGNOSIS_AGE",
                        passed=ai_result['is_valid'],
                        source="ai",
                        confidence=ai_result['confidence'],
                        reasoning=ai_result['reasoning'],
                        details={
                            "enrollee_age": enrollee_context.age,
                            "ai_validated": True,
                            "validation_type": "diagnosis_age",
                            "resolved_name": diag_info['name'],
                            "resolved_category": diag_info['category']
                        }
                    ))
        
        # ===================================================================
        # RULE 4: DIAGNOSIS GENDER VALIDATION
        # ===================================================================
        if enrollee_context and enrollee_context.gender:
            if diag_master:
                # Master table validation
                diag_gender_result = self.base_engine.validate_gender_for_diagnosis(
                    diagnosis_code, enrollee_context.gender
                )
                rule_results.append(RuleResult(
                    rule_name="DIAGNOSIS_GENDER",
                    passed=diag_gender_result.get("is_valid", True),
                    source="master_table",
                    confidence=100 if not diag_gender_result.get("is_valid", True) else 95,
                    reasoning=diag_gender_result.get("reasoning", "Gender validated by master table"),
                    details=diag_gender_result
                ))
            else:
                # Check learning table first
                gender_learning = self.base_engine.check_diagnosis_gender_learning(
                    diagnosis_code, enrollee_context.gender
                )
                if gender_learning:
                    is_valid = gender_learning['is_valid']
                    stored_reason = gender_learning.get('reason', '')
                    
                    if is_valid:
                        reasoning = f"✅ Valid for {enrollee_context.gender} (learned approval: {stored_reason})"
                    else:
                        reasoning = f"❌ GENDER RESTRICTION: {diagnosis_code} is invalid for {enrollee_context.gender} (learned denial: {stored_reason})"
                    
                    rule_results.append(RuleResult(
                        rule_name="DIAGNOSIS_GENDER",
                        passed=is_valid,
                        source="learning_table",
                        confidence=gender_learning.get('confidence', 90),
                        reasoning=reasoning,
                        details={
                            **gender_learning,
                            "enrollee_gender": enrollee_context.gender,
                            "learned_from_previous": True
                        }
                    ))
                else:
                    # Not in master or learning - CALL AI with RESOLVED NAME
                    ai_result = self._ai_validate_diagnosis_gender(
                        diagnosis_code, enrollee_context.gender, diag_info=diag_info
                    )
                    
                    rule_results.append(RuleResult(
                        rule_name="DIAGNOSIS_GENDER",
                        passed=ai_result['is_valid'],
                        source="ai",
                        confidence=ai_result['confidence'],
                        reasoning=ai_result['reasoning'],
                        details={
                            "enrollee_gender": enrollee_context.gender,
                            "ai_validated": True,
                            "validation_type": "diagnosis_gender",
                            "resolved_name": diag_info['name'],
                            "resolved_category": diag_info['category']
                        }
                    ))
        
        # ===================================================================
        # RULE 5: PROCEDURE-DIAGNOSIS COMPATIBILITY
        # ===================================================================
        # NEW FLOW (v2.0):
        #   1. Learning table → if found, use it
        #   2. Universal consultation codes (CONS021/CONS022) → auto-approve
        #   3. PROCEDURE_DIAGNOSIS_COMP table → if pair exists, approve
        #   4. Pair NOT in COMP → AI validates (human confirms → learning)
        #
        # REMOVED: typical_diagnoses matching (columns now empty)
        # REMOVED: typical_symptoms / implied_symptoms overlap matching
        #          (flawed: shared symptoms like "fever" caused false approvals)
        # ===================================================================
        
        # Step 1: Check learning table
        learning_result = self.base_engine.check_procedure_diagnosis_learning(
            procedure_code, diagnosis_code
        )
        
        if learning_result:
            is_valid = learning_result.get("is_valid", False)
            stored_reason = learning_result.get("reasoning", '')
            
            if is_valid:
                reasoning = f"✅ Valid combination (learned approval: {stored_reason})"
            else:
                reasoning = f"❌ INCOMPATIBLE: {procedure_code} + {diagnosis_code} mismatch (learned denial: {stored_reason})"
            
            rule_results.append(RuleResult(
                rule_name="PROC_DIAG_COMPATIBILITY",
                passed=is_valid,
                source="learning_table",
                confidence=learning_result.get("confidence", 100),
                reasoning=reasoning,
                details={
                    **learning_result,
                    "learned_from_previous": True
                }
            ))
        
        # Step 2: Universal consultation codes
        elif self.base_engine.is_universal_procedure(procedure_code):
            proc_name = proc_info.get('name', procedure_code)
            diag_name = diag_info.get('name', diagnosis_code)
            rule_results.append(RuleResult(
                rule_name="PROC_DIAG_COMPATIBILITY",
                passed=True,
                source="master_table",
                confidence=100,
                reasoning=f"✅ {proc_name} is a universal consultation — valid for all diagnoses including {diag_name}",
                details={"match_type": "UNIVERSAL_CONSULTATION"}
            ))
        
        # Step 3: PROCEDURE_DIAGNOSIS_COMP table lookup
        else:
            comp_result = self.base_engine.check_procedure_diagnosis_comp(
                procedure_code, diagnosis_code
            )
            
            if comp_result:
                rule_results.append(RuleResult(
                    rule_name="PROC_DIAG_COMPATIBILITY",
                    passed=True,
                    source="master_table",
                    confidence=100,
                    reasoning=f"✅ {comp_result['procedure_name']} is a validated match for {comp_result['diagnosis_name']}",
                    details={"match_type": "COMP_TABLE_MATCH", "comp_result": comp_result}
                ))
            else:
                # Step 4: Pair NOT in COMP → AI validates
                # NOT being in COMP does NOT mean invalid — just not curated yet
                ai_result = self._ai_validate_proc_diag_compatibility(
                    procedure_code, 
                    diagnosis_code,
                    age=enrollee_context.age if enrollee_context else None,
                    gender=enrollee_context.gender if enrollee_context else None,
                    proc_info=proc_info,
                    diag_info=diag_info
                )
                
                rule_results.append(RuleResult(
                    rule_name="PROC_DIAG_COMPATIBILITY",
                    passed=ai_result['is_valid'],
                    source="ai",
                    confidence=ai_result['confidence'],
                    reasoning=ai_result['reasoning'],
                    details={
                        "ai_validated": True,
                        "not_in_comp_table": True,
                        "enrollee_age": enrollee_context.age if enrollee_context else None,
                        "enrollee_gender": enrollee_context.gender if enrollee_context else None,
                        "resolved_procedure": proc_info['name'],
                        "resolved_diagnosis": diag_info['name'],
                        "pubmed_triggered": ai_result.get('pubmed_triggered', False),
                        "pubmed_count": ai_result.get('pubmed_count', 0),
                        "pubmed_query": ai_result.get('pubmed_query', ''),
                        "pubmed_evidence": ai_result.get('pubmed_evidence', [])
                    }
                ))
        
        # ===================================================================
        # RULE 6: PROCEDURE 30-DAY DUPLICATE CHECK
        # ===================================================================
        if enrollee_id and encounter_date and self.thirty_day_engine:
            proc_30day = self.thirty_day_engine.validate_procedure_30_day(
                procedure_code=procedure_code,
                enrollee_id=enrollee_id,
                encounter_date=encounter_date
            )
            
            rule_results.append(RuleResult(
                rule_name="PROCEDURE_30DAY_DUPLICATE",
                passed=proc_30day.passed,
                source="ai" if proc_30day.used_ai_for_classification else "master_table",
                confidence=100 if not proc_30day.passed else 95,
                reasoning=proc_30day.reasoning if not proc_30day.passed else f"✅ No duplicate procedures in last 30 days (checked {len(proc_30day.history_items)} items)",
                details={
                    "input_code": proc_30day.input_code,
                    "input_class": proc_30day.input_therapeutic_class,
                    "history_count": len(proc_30day.history_items),
                    "exact_duplicates": len(proc_30day.exact_duplicate_items),
                    "class_duplicates": len(proc_30day.class_duplicate_items),
                    "history_items": [
                        {
                            "code": item.code,
                            "description": item.description,
                            "class": item.therapeutic_class,
                            "source": item.source,
                            "date": item.date,
                            "classification_source": item.classification_source
                        }
                        for item in proc_30day.history_items
                    ]
                }
            ))
        
        # ===================================================================
        # DETERMINE OVERALL DECISION
        # ===================================================================
        all_passed = all(r.passed for r in rule_results)
        failed_rules = [r for r in rule_results if not r.passed]
        
        if all_passed:
            overall_decision = "APPROVE"
            overall_confidence = min(r.confidence for r in rule_results) if rule_results else 100
            overall_reasoning = "✅ All validation rules passed"
        else:
            overall_decision = "DENY"
            overall_confidence = max(r.confidence for r in failed_rules) if failed_rules else 100
            failed_names = [r.rule_name for r in failed_rules]
            overall_reasoning = f"❌ Failed rules: {', '.join(failed_names)}"
        
        # Determine if AI validations can be stored (BOTH approvals AND denials)
        ai_validated_rules = [r for r in rule_results if r.source == "ai"]
        can_store = len(ai_validated_rules) > 0
        
        # Requires human review if ANY AI validation
        requires_review = any(r.source == "ai" for r in rule_results)
        
        # ===================================================================
        # AUTO-DENY DETECTION
        # ===================================================================
        # If ALL failed rules come from learning table with:
        #   - is_valid = False (learned denial)
        #   - usage_count >= 3 (confirmed enough times to trust)
        #   - approved_by exists (was human-approved at least once)
        # Then auto-deny without human review.
        #
        # Safe because: human validated → used 3+ times without correction
        # → represents trusted institutional medical knowledge.
        # Override button still available in UI for edge cases.
        # ===================================================================
        AUTO_DENY_MIN_USAGE = 3
        
        auto_deny = False
        auto_deny_rules = []
        
        if overall_decision == "DENY" and failed_rules:
            all_failed_qualify = True
            qualifying_rules = []
            
            for rule in failed_rules:
                is_learned_denial = (
                    rule.source == "learning_table" 
                    and not rule.passed
                )
                usage = rule.details.get('usage_count', 0)
                approver = rule.details.get('approved_by', '')
                
                if is_learned_denial and usage >= AUTO_DENY_MIN_USAGE and approver:
                    qualifying_rules.append(rule)
                else:
                    all_failed_qualify = False
                    break
            
            if all_failed_qualify and qualifying_rules:
                auto_deny = True
                auto_deny_rules = qualifying_rules
                requires_review = False  # Skip human review
                overall_reasoning = (
                    f"🚫 AUTO-DENIED: {len(qualifying_rules)} learned denial(s) — " +
                    ", ".join(
                        f"{r.rule_name} (used {r.details.get('usage_count', 0)}x)"
                        for r in qualifying_rules
                    )
                )
        
        return ComprehensiveValidation(
            overall_decision=overall_decision,
            overall_confidence=overall_confidence,
            overall_reasoning=overall_reasoning,
            rule_results=rule_results,
            requires_human_review=requires_review,
            can_store_ai_approvals=can_store,
            auto_deny=auto_deny,
            auto_deny_rules=auto_deny_rules
        )
    
    def store_ai_validated_rules(
        self,
        procedure_code: str,
        diagnosis_code: str,
        validation: ComprehensiveValidation,
        approved_by: str = "Casey"
    ) -> Dict[str, bool]:
        """
        Store AI-validated rules (BOTH approvals AND denials) for learning
        
        This is the core of the learning system:
        - PASSED rules: Store as is_valid=TRUE
        - FAILED rules: Store as is_valid=FALSE
        
        Examples:
        - ✅ PROCEDURE_AGE passed: Store min_age=45, max_age=45, is_valid=TRUE
        - ❌ PROCEDURE_AGE failed: Store min_age=5, max_age=5, is_valid=FALSE
        - ❌ 30DAY_DUPLICATE failed: Extract pairwise class relationships
        
        Returns dict indicating which rules were stored
        """
        stored = {}
        
        if not validation.can_store_ai_approvals:
            return stored
        
        # Get ALL AI-validated rules (both passed and failed)
        ai_rules = validation.get_ai_validated_rules()
        
        for rule in ai_rules:
            try:
                # Guard: Never store AI error results as learned decisions
                if rule.confidence == 0 or (rule.reasoning and "AI error" in rule.reasoning):
                    logger.warning(f"Skipping storage of {rule.rule_name} - AI error/zero confidence")
                    continue
                
                if rule.rule_name == "PROCEDURE_AGE":
                    enrollee_age = rule.details.get("enrollee_age")
                    if enrollee_age is not None:
                        success = self.base_engine.store_procedure_age_decision(
                            procedure_code=procedure_code,
                            min_age=enrollee_age,
                            max_age=enrollee_age,
                            is_valid=rule.passed,
                            reason=rule.reasoning,
                            confidence=rule.confidence,
                            ai_reasoning=rule.reasoning,
                            approved_by=approved_by
                        )
                        if success:
                            stored["PROCEDURE_AGE"] = True
                
                elif rule.rule_name == "PROCEDURE_GENDER":
                    enrollee_gender = rule.details.get("enrollee_gender", "Unknown")
                    success = self.base_engine.store_procedure_gender_decision(
                        procedure_code=procedure_code,
                        allowed_gender=enrollee_gender,
                        is_valid=rule.passed,
                        reason=rule.reasoning,
                        confidence=rule.confidence,
                        ai_reasoning=rule.reasoning,
                        approved_by=approved_by
                    )
                    if success:
                        stored["PROCEDURE_GENDER"] = True
                
                elif rule.rule_name == "DIAGNOSIS_AGE":
                    enrollee_age = rule.details.get("enrollee_age")
                    if enrollee_age is not None:
                        success = self.base_engine.store_diagnosis_age_decision(
                            diagnosis_code=diagnosis_code,
                            min_age=enrollee_age,
                            max_age=enrollee_age,
                            is_valid=rule.passed,
                            reason=rule.reasoning,
                            confidence=rule.confidence,
                            ai_reasoning=rule.reasoning,
                            approved_by=approved_by
                        )
                        if success:
                            stored["DIAGNOSIS_AGE"] = True
                
                elif rule.rule_name == "DIAGNOSIS_GENDER":
                    enrollee_gender = rule.details.get("enrollee_gender", "Unknown")
                    success = self.base_engine.store_diagnosis_gender_decision(
                        diagnosis_code=diagnosis_code,
                        allowed_gender=enrollee_gender,
                        is_valid=rule.passed,
                        reason=rule.reasoning,
                        confidence=rule.confidence,
                        ai_reasoning=rule.reasoning,
                        approved_by=approved_by
                    )
                    if success:
                        stored["DIAGNOSIS_GENDER"] = True
                
                elif rule.rule_name == "PROC_DIAG_COMPATIBILITY":
                    # Guard: Don't store AI errors as learned decisions
                    if rule.confidence == 0 or (rule.reasoning and rule.reasoning.startswith("AI error")):
                        logger.warning(f"Skipping storage of PROC_DIAG_COMPATIBILITY - AI error result")
                        continue
                    
                    success = self.base_engine.store_approved_decision(
                        procedure_code=procedure_code,
                        diagnosis_code=diagnosis_code,
                        is_valid=rule.passed,
                        reason=rule.reasoning,
                        confidence=rule.confidence,
                        ai_reasoning=rule.reasoning,
                        approved_by=approved_by
                    )
                    if success:
                        stored["PROC_DIAG_COMPATIBILITY"] = True
                
                elif rule.rule_name == "PROCEDURE_30DAY_DUPLICATE":
                    # Store ALL AI-classified pairs so future lookups skip AI
                    if rule.details:
                        input_code = rule.details.get('input_code', '').lower()
                        input_class = rule.details.get('input_class', '')
                        history_items = rule.details.get('history_items', [])
                        
                        for item in history_items:
                            item_code = item.get('code', '').lower()
                            item_class = item.get('class', '')
                            classification_source = item.get('classification_source', '')
                            
                            # Only store AI-classified items (master/learning already known)
                            if classification_source != 'ai':
                                continue
                            
                            # Skip exact duplicates (same code)
                            if item_code == input_code:
                                continue
                            
                            # Determine if same class
                            is_same_class = (
                                item_class == input_class 
                                and input_class != 'Unknown' 
                                and item_class != 'Unknown'
                            )
                            
                            success = self.base_engine.store_procedure_class_decision(
                                procedure_code_1=input_code.upper(),
                                procedure_code_2=item_code.upper(),
                                shared_class=input_class if is_same_class else "",
                                same_class=is_same_class,
                                ai_confidence=rule.confidence,
                                ai_reasoning=f"Both classified as {input_class}" if is_same_class else f"Input: {input_class}, History: {item_class}",
                                approved_by=approved_by
                            )
                            if success:
                                if "PROCEDURE_30DAY_DUPLICATE" not in stored:
                                    stored["PROCEDURE_30DAY_DUPLICATE"] = []
                                stored["PROCEDURE_30DAY_DUPLICATE"].append(
                                    f"{input_code.upper()}+{item_code.upper()} ({'SAME' if is_same_class else 'DIFF'})"
                                )
            
            except Exception as e:
                print(f"Error storing {rule.rule_name}: {e}")
                stored[rule.rule_name] = False
        
        return stored
    
    # Backward compatibility
    def store_ai_approved_rules(self, *args, **kwargs):
        """Deprecated: Use store_ai_validated_rules instead"""
        return self.store_ai_validated_rules(*args, **kwargs)


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("Testing Comprehensive Validation Engine (FIXED v3.0)...\n")
    
    engine = ComprehensiveVettingEngine()
    
    # Test case: DRG1106 + C593 + Male (44yo) — the case that exposed the bug
    print("TEST CASE: DRG1106 (Amlodipine 10mg) + C593 (Malignant neoplasm of ovary)")
    print("Patient: Male, 44yo")
    print("Expected: PROCEDURE_AGE=PASS, PROCEDURE_GENDER=PASS, DIAGNOSIS_AGE=PASS,")
    print("          DIAGNOSIS_GENDER=FAIL, PROC_DIAG_COMPATIBILITY=FAIL")
    print()
    
    # First, show what the codes resolve to
    proc_info = engine._resolve_procedure_info("DRG1106")
    diag_info = engine._resolve_diagnosis_info("C593")
    print(f"DRG1106 resolves to: {proc_info['name']} ({proc_info['source']})")
    print(f"C593 resolves to: {diag_info['name']} ({diag_info['source']})")
    print()
    
    validation = engine.validate_comprehensive(
        procedure_code="DRG1106",
        diagnosis_code="C593",
        enrollee_id="CL/OCTA/723449/2023-A"
    )
    
    print("="*70)
    print(f"OVERALL DECISION: {validation.overall_decision}")
    print(f"Confidence: {validation.overall_confidence}%")
    print(f"Reasoning: {validation.overall_reasoning}")
    print("="*70)
    
    print("\nRULE RESULTS:")
    print("-"*70)
    for rule in validation.rule_results:
        status = "✅ PASS" if rule.passed else "❌ FAIL"
        print(f"{status} | {rule.rule_name:<30} | {rule.source:<15} | {rule.confidence}%")
        print(f"     {rule.reasoning}")
        if rule.details.get('resolved_name'):
            print(f"     [Resolved: {rule.details.get('resolved_name')}]")
        print()
    
    summary = validation.get_summary()
    print("="*70)
    print("SUMMARY:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    print(f"\nCan store AI approvals: {validation.can_store_ai_approvals}")
    print(f"Requires human review: {validation.requires_human_review}")