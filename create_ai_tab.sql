-- ============================================================================
-- LEARNING TABLES SCHEMA - Human-in-Loop Vetting System
-- ============================================================================
-- Purpose: Store human-approved AI decisions to reduce future AI calls
-- Author: Casey's AI Assistant
-- Date: February 2026
-- Version: 1.0
--
-- Usage: Run this in DuckDB or via duckdb:query tool
--
-- Expected cost reduction:
--   Week 1: 100% AI calls
--   Month 1: ~70% AI calls (30% learning table hits)
--   Month 6: ~20% AI calls (80% learning table hits)
-- ============================================================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS "PROCEDURE_DIAGNOSIS";

-- ============================================================================
-- TABLE 1: Procedure-Diagnosis Match Learning
-- ============================================================================
-- Purpose: Learn which procedure-diagnosis pairs are valid/invalid
-- Example: DRG1958 (Fluconazole) + B373 (Vaginal candidiasis) = VALID
-- ============================================================================

CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis" (
    id INTEGER PRIMARY KEY,
    procedure_code VARCHAR NOT NULL,
    diagnosis_code VARCHAR NOT NULL,
    is_valid_match BOOLEAN NOT NULL,  -- TRUE=appropriate, FALSE=inappropriate
    match_reason VARCHAR,              -- Why they match/don't match
    ai_confidence INTEGER,             -- Original AI confidence (0-100)
    ai_reasoning TEXT,                 -- AI's explanation
    approved_by VARCHAR,               -- Agent who approved (e.g., 'Casey')
    approved_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usage_count INTEGER DEFAULT 0,    -- Times this rule was reused
    last_used_date TIMESTAMP,          -- Last time rule was applied
    UNIQUE(procedure_code, diagnosis_code)
);

-- ============================================================================
-- TABLE 2: Procedure Class Learning (Pairwise)
-- ============================================================================
-- Purpose: Learn which procedure pairs share therapeutic class (30-day rule)
-- Example: DRG1958 + DRG1959 both ANTIFUNGAL class
-- Note: Pairwise storage allows granular control vs flat classification
-- ============================================================================

CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_procedure_class" (
    id INTEGER PRIMARY KEY,
    procedure_code_1 VARCHAR NOT NULL,
    procedure_code_2 VARCHAR NOT NULL,
    shared_class VARCHAR,              -- Class name if same_class=TRUE
    same_class BOOLEAN NOT NULL,       -- TRUE=same class, FALSE=different
    ai_confidence INTEGER,
    ai_reasoning TEXT,
    approved_by VARCHAR,
    approved_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usage_count INTEGER DEFAULT 0,
    last_used_date TIMESTAMP,
    -- Ensure ordered pair (avoid duplicate reverse pairs)
    -- (DRG1958, DRG1776) stored, (DRG1776, DRG1958) prevented
    CONSTRAINT ordered_pair_proc CHECK (procedure_code_1 <= procedure_code_2),
    UNIQUE(procedure_code_1, procedure_code_2)
);

-- ============================================================================
-- TABLE 3: Diagnosis Class Learning (Pairwise)
-- ============================================================================
-- Purpose: Learn which diagnosis pairs share category (30-day rule)
-- Example: J180 (Bronchopneumonia) + J189 (Pneumonia) both PNEUMONIA class
-- ============================================================================

CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."ai_human_diagnosis_class" (
    id INTEGER PRIMARY KEY,
    diagnosis_code_1 VARCHAR NOT NULL,
    diagnosis_code_2 VARCHAR NOT NULL,
    shared_class VARCHAR,
    same_class BOOLEAN NOT NULL,
    ai_confidence INTEGER,
    ai_reasoning TEXT,
    approved_by VARCHAR,
    approved_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usage_count INTEGER DEFAULT 0,
    last_used_date TIMESTAMP,
    CONSTRAINT ordered_pair_diag CHECK (diagnosis_code_1 <= diagnosis_code_2),
    UNIQUE(diagnosis_code_1, diagnosis_code_2)
);

-- ============================================================================
-- PERFORMANCE INDEXES
-- ============================================================================
-- These indexes optimize lookup speed (target <100ms per query)
-- ============================================================================

-- Procedure-Diagnosis lookup
CREATE INDEX IF NOT EXISTS idx_proc_diag_lookup 
ON "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis"(procedure_code, diagnosis_code);

CREATE INDEX IF NOT EXISTS idx_proc_diag_valid 
ON "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis"(is_valid_match);

CREATE INDEX IF NOT EXISTS idx_proc_diag_usage 
ON "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis"(usage_count DESC);

-- Procedure Class lookup
CREATE INDEX IF NOT EXISTS idx_proc_class_lookup 
ON "PROCEDURE_DIAGNOSIS"."ai_human_procedure_class"(procedure_code_1, procedure_code_2);

CREATE INDEX IF NOT EXISTS idx_proc_class_same 
ON "PROCEDURE_DIAGNOSIS"."ai_human_procedure_class"(same_class);

-- Diagnosis Class lookup
CREATE INDEX IF NOT EXISTS idx_diag_class_lookup 
ON "PROCEDURE_DIAGNOSIS"."ai_human_diagnosis_class"(diagnosis_code_1, diagnosis_code_2);

CREATE INDEX IF NOT EXISTS idx_diag_class_same 
ON "PROCEDURE_DIAGNOSIS"."ai_human_diagnosis_class"(same_class);

-- ============================================================================
-- ANALYTICS VIEW
-- ============================================================================
-- Real-time statistics on learning system performance
-- ============================================================================

CREATE OR REPLACE VIEW "PROCEDURE_DIAGNOSIS"."learning_stats" AS
SELECT 
    'Procedure-Diagnosis' as table_name,
    COUNT(*) as total_rules,
    SUM(usage_count) as total_reuses,
    ROUND(AVG(usage_count), 2) as avg_reuses_per_rule,
    SUM(CASE WHEN is_valid_match THEN 1 ELSE 0 END) as valid_matches,
    SUM(CASE WHEN NOT is_valid_match THEN 1 ELSE 0 END) as invalid_matches,
    MAX(usage_count) as max_usage_count
FROM "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis"
UNION ALL
SELECT 
    'Procedure-Class',
    COUNT(*),
    SUM(usage_count),
    ROUND(AVG(usage_count), 2),
    SUM(CASE WHEN same_class THEN 1 ELSE 0 END),
    SUM(CASE WHEN NOT same_class THEN 1 ELSE 0 END),
    MAX(usage_count)
FROM "PROCEDURE_DIAGNOSIS"."ai_human_procedure_class"
UNION ALL
SELECT 
    'Diagnosis-Class',
    COUNT(*),
    SUM(usage_count),
    ROUND(AVG(usage_count), 2),
    SUM(CASE WHEN same_class THEN 1 ELSE 0 END),
    SUM(CASE WHEN NOT same_class THEN 1 ELSE 0 END),
    MAX(usage_count)
FROM "PROCEDURE_DIAGNOSIS"."ai_human_diagnosis_class";

-- ============================================================================
-- SAMPLE TEST DATA (Optional - uncomment to test)
-- ============================================================================
-- These are example approved decisions from previous sessions
-- ============================================================================

-- Example 1: Fluconazole + Vaginal Candidiasis = VALID
-- INSERT INTO "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis" 
-- (id, procedure_code, diagnosis_code, is_valid_match, match_reason, 
--  ai_confidence, ai_reasoning, approved_by, usage_count)
-- VALUES 
-- (1, 'DRG1958', 'B373', TRUE, 
--  'Fluconazole is standard treatment for vaginal candidiasis',
--  95, 'Antifungal indicated for vaginal yeast infection',
--  'Casey', 0);

-- Example 2: Fluconazole + Pneumonia = INVALID
-- INSERT INTO "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis" 
-- (id, procedure_code, diagnosis_code, is_valid_match, match_reason, 
--  ai_confidence, ai_reasoning, approved_by, usage_count)
-- VALUES 
-- (2, 'DRG1958', 'J180', FALSE, 
--  'Fluconazole is antifungal, not appropriate for bacterial pneumonia',
--  92, 'Wrong therapeutic class for condition',
--  'Casey', 0);

-- Example 3: Two antifungals share same class
-- INSERT INTO "PROCEDURE_DIAGNOSIS"."ai_human_procedure_class" 
-- (id, procedure_code_1, procedure_code_2, shared_class, same_class,
--  ai_confidence, ai_reasoning, approved_by, usage_count)
-- VALUES 
-- (1, 'DRG1958', 'DRG1959', 'ANTIFUNGAL', TRUE,
--  90, 'Both are azole antifungals',
--  'Casey', 0);

-- Example 4: Two pneumonia diagnoses share same class
-- INSERT INTO "PROCEDURE_DIAGNOSIS"."ai_human_diagnosis_class" 
-- (id, diagnosis_code_1, diagnosis_code_2, shared_class, same_class,
--  ai_confidence, ai_reasoning, approved_by, usage_count)
-- VALUES 
-- (1, 'J180', 'J189', 'PNEUMONIA', TRUE,
--  88, 'Both are pneumonia variants',
--  'Casey', 0);

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
-- Run these to verify successful setup
-- ============================================================================

-- Check table creation
SELECT 'Table Check' as test_name, 
       COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema = 'PROCEDURE_DIAGNOSIS' 
  AND table_name LIKE 'ai_human%';

-- Check index creation
SELECT 'Index Check' as test_name,
       COUNT(*) as index_count
FROM information_schema.tables
WHERE table_schema = 'PROCEDURE_DIAGNOSIS'
  AND table_name LIKE 'ai_human%';

-- Check analytics view
SELECT * FROM "PROCEDURE_DIAGNOSIS"."learning_stats";

-- ============================================================================
-- MAINTENANCE QUERIES
-- ============================================================================

-- Find low-usage rules (candidates for review after 6 months)
-- SELECT * FROM "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis"
-- WHERE usage_count < 5 
--   AND approved_date < CURRENT_TIMESTAMP - INTERVAL 6 MONTHS;

-- Find high-value rules (saved most AI calls)
-- SELECT procedure_code, diagnosis_code, usage_count, 
--        usage_count * 0.003 as dollars_saved
-- FROM "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis"
-- ORDER BY usage_count DESC
-- LIMIT 20;

-- Calculate total AI cost savings
-- SELECT SUM(usage_count) as total_reuses,
--        SUM(usage_count) * 0.003 as total_dollars_saved
-- FROM "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis";

-- ============================================================================
-- END OF SETUP SCRIPT
-- ============================================================================