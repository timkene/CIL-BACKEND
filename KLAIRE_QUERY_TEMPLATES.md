# KLAIRE Query Templates

**Purpose:** Reusable SQL query templates for common KLAIRE questions

**Last Updated:** 2025-11-06

---

## Template 1: Medical History Analysis for Enrollee

### Purpose
Get complete medical history including procedures, diagnoses, and medical trends for a specific enrollee over a specified period.

### Parameters
- `enrollee_id`: Enrollee ID (can be IID, enrollee_id, memberid, or legacycode)
- `period_start`: Start date (e.g., '2025-08-01' or calculated from "last 6 months")
- `period_end`: End date (e.g., '2025-11-06' or CURRENT_DATE)

### Query Structure

```sql
-- Step 1: Get all PAs for the enrollee in the period
WITH enrollee_pas AS (
    SELECT DISTINCT 
        pa.panumber,
        pa.requestdate,
        pa.pastatus,
        pa.providerid,
        p.providername,
        pa.groupname
    FROM "AI DRIVEN DATA"."PA DATA" pa
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
    WHERE (pa.IID = '{enrollee_id}' 
           OR pa.enrollee_id = '{enrollee_id}'
           OR pa.memberid = '{enrollee_id}')
      AND pa.requestdate >= '{period_start}'
      AND pa.requestdate <= '{period_end}'
),

-- Step 2: Get all diagnoses for these PAs
enrollee_diagnoses AS (
    SELECT 
        td.panumber,
        td.code as diagnosiscode,
        td."desc" as diagnosisdesc,
        d.diagnosisdesc as full_description,
        pa.requestdate
    FROM "AI DRIVEN DATA"."TBPADIAGNOSIS" td
    INNER JOIN enrollee_pas pa ON CAST(td.panumber AS VARCHAR) = CAST(pa.panumber AS VARCHAR)
    LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON td.code = d.diagnosiscode
),

-- Step 3: Get all procedures for these PAs
enrollee_procedures AS (
    SELECT 
        pa.panumber,
        pa.requestdate,
        pa.code as procedurecode,
        pd.proceduredesc,
        pa.requested,
        pa.granted,
        pa.benefitcode,
        pa.pastatus,
        p.providername
    FROM "AI DRIVEN DATA"."PA DATA" pa
    INNER JOIN enrollee_pas ep ON pa.panumber = ep.panumber
    LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON pa.code = pd.procedurecode
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
),

-- Step 4: Get all claims for this enrollee
enrollee_claims AS (
    SELECT 
        c.panumber,
        c.datesubmitted,
        c.encounterdatefrom,
        c.code as procedurecode,
        c.chargeamount,
        c.approvedamount,
        c.deniedamount,
        c.diagnosiscode,
        p.providername
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON CAST(c.providerid AS VARCHAR) = p.providerid
    WHERE (c.enrollee_id = '{enrollee_id}' 
           OR CAST(c.panumber AS VARCHAR) IN (SELECT panumber FROM enrollee_pas))
      AND (c.datesubmitted >= '{period_start}' OR c.encounterdatefrom >= '{period_start}')
      AND (c.datesubmitted <= '{period_end}' OR c.encounterdatefrom <= '{period_end}')
)

-- Main Results
SELECT 
    'PA' as record_type,
    ep.panumber,
    ep.requestdate as date,
    ep.providername,
    ep.pastatus,
    NULL as diagnosiscode,
    NULL as diagnosisdesc,
    proc.procedurecode,
    proc.proceduredesc,
    proc.requested,
    proc.granted,
    NULL as chargeamount,
    NULL as approvedamount,
    NULL as deniedamount
FROM enrollee_pas ep
LEFT JOIN enrollee_procedures proc ON ep.panumber = proc.panumber
UNION ALL
SELECT 
    'CLAIM' as record_type,
    ec.panumber,
    ec.datesubmitted as date,
    ec.providername,
    NULL as pastatus,
    ec.diagnosiscode,
    d.diagnosisdesc,
    ec.procedurecode,
    pd.proceduredesc,
    NULL as requested,
    NULL as granted,
    ec.chargeamount,
    ec.approvedamount,
    ec.deniedamount
FROM enrollee_claims ec
LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON ec.diagnosiscode = d.diagnosiscode
LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON ec.procedurecode = pd.procedurecode
ORDER BY date DESC;
```

### Medical Trend Analysis Queries

```sql
-- Diagnosis Frequency
SELECT 
    ed.diagnosiscode,
    ed.diagnosisdesc,
    COUNT(*) as frequency,
    MIN(ed.requestdate) as first_occurrence,
    MAX(ed.requestdate) as last_occurrence
FROM enrollee_diagnoses ed
GROUP BY ed.diagnosiscode, ed.diagnosisdesc
ORDER BY frequency DESC;

-- Procedure Frequency
SELECT 
    proc.procedurecode,
    proc.proceduredesc,
    COUNT(*) as frequency,
    SUM(proc.granted) as total_cost,
    AVG(proc.granted) as avg_cost
FROM enrollee_procedures proc
GROUP BY proc.procedurecode, proc.proceduredesc
ORDER BY frequency DESC;

-- Antibiotic Usage
SELECT 
    proc.procedurecode,
    proc.proceduredesc,
    COUNT(*) as frequency,
    SUM(proc.granted) as total_cost
FROM enrollee_procedures proc
WHERE proc.proceduredesc ILIKE '%amoxicillin%'
   OR proc.proceduredesc ILIKE '%azithromycin%'
   OR proc.proceduredesc ILIKE '%ceftriaxone%'
   OR proc.proceduredesc ILIKE '%ciprofloxacin%'
   OR proc.proceduredesc ILIKE '%metronidazole%'
   OR proc.proceduredesc ILIKE '%antibiotic%'
GROUP BY proc.procedurecode, proc.proceduredesc;

-- Antimalaria Usage
SELECT 
    proc.procedurecode,
    proc.proceduredesc,
    COUNT(*) as frequency,
    SUM(proc.granted) as total_cost
FROM enrollee_procedures proc
WHERE proc.proceduredesc ILIKE '%artemether%'
   OR proc.proceduredesc ILIKE '%lumefantrine%'
   OR proc.proceduredesc ILIKE '%artesunate%'
   OR proc.proceduredesc ILIKE '%antimalaria%'
   OR proc.proceduredesc ILIKE '%malaria%'
GROUP BY proc.procedurecode, proc.proceduredesc;

-- Provider Usage Pattern
SELECT 
    ep.providername,
    COUNT(DISTINCT ep.panumber) as visit_count,
    SUM(proc.granted) as total_cost
FROM enrollee_pas ep
LEFT JOIN enrollee_procedures proc ON ep.panumber = proc.panumber
GROUP BY ep.providername
ORDER BY visit_count DESC;
```

---

## Template 2: MLR Comparison Between Contract Periods

### Purpose
Compare a company's MLR between last contract and current contract, identifying drivers of change.

### Parameters
- `company_name`: Company/group name (e.g., 'CDK INDUSTRIES')
- Current contract: `iscurrent = 1` in GROUP_CONTRACT
- Last contract: `iscurrent = 0` in GROUP_CONTRACT, ordered by enddate DESC LIMIT 1

### Query Structure

```sql
-- Step 1: Get contract periods
WITH contracts AS (
    SELECT 
        groupname,
        startdate,
        enddate,
        iscurrent,
        ROW_NUMBER() OVER (PARTITION BY groupname, iscurrent ORDER BY enddate DESC) as rn
    FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
    WHERE groupname = '{company_name}'
      AND iscurrent IN (0, 1)
),
current_contract AS (
    SELECT * FROM contracts WHERE iscurrent = 1 AND rn = 1
),
last_contract AS (
    SELECT * FROM contracts WHERE iscurrent = 0 AND rn = 1
),

-- Step 2: Get premium received (cash received)
current_premium AS (
    SELECT SUM(ABS(assets_amount)) as total_premium
    FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
    WHERE groupname = '{company_name}'
      AND Date >= (SELECT startdate FROM current_contract)
      AND Date <= (SELECT enddate FROM current_contract)
),
last_premium AS (
    SELECT SUM(ABS(assets_amount)) as total_premium
    FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
    WHERE groupname = '{company_name}'
      AND Date >= (SELECT startdate FROM last_contract)
      AND Date <= (SELECT enddate FROM last_contract)
),

-- Step 3: Get medical spending (claims + unclaimed PA)
current_claims AS (
    SELECT SUM(approvedamount) as total_claims
    FROM "AI DRIVEN DATA"."CLAIMS DATA"
    WHERE groupid = (SELECT groupid FROM current_contract)
      AND datesubmitted >= (SELECT startdate FROM current_contract)
      AND datesubmitted <= (SELECT enddate FROM current_contract)
),
current_unclaimed_pa AS (
    SELECT SUM(pa.granted) as total_unclaimed
    FROM "AI DRIVEN DATA"."PA DATA" pa
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM current_contract)
      AND pa.requestdate <= (SELECT enddate FROM current_contract)
      AND CAST(pa.panumber AS VARCHAR) NOT IN (
          SELECT DISTINCT CAST(panumber AS VARCHAR) 
          FROM "AI DRIVEN DATA"."CLAIMS DATA"
          WHERE panumber IS NOT NULL
      )
),
last_claims AS (
    SELECT SUM(approvedamount) as total_claims
    FROM "AI DRIVEN DATA"."CLAIMS DATA"
    WHERE groupid = (SELECT groupid FROM last_contract)
      AND datesubmitted >= (SELECT startdate FROM last_contract)
      AND datesubmitted <= (SELECT enddate FROM last_contract)
),
last_unclaimed_pa AS (
    SELECT SUM(pa.granted) as total_unclaimed
    FROM "AI DRIVEN DATA"."PA DATA" pa
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM last_contract)
      AND pa.requestdate <= (SELECT enddate FROM last_contract)
      AND CAST(pa.panumber AS VARCHAR) NOT IN (
          SELECT DISTINCT CAST(panumber AS VARCHAR) 
          FROM "AI DRIVEN DATA"."CLAIMS DATA"
          WHERE panumber IS NOT NULL
      )
)

-- MLR Comparison
SELECT 
    'Current Contract' as period,
    (SELECT startdate FROM current_contract) as start_date,
    (SELECT enddate FROM current_contract) as end_date,
    (SELECT total_premium FROM current_premium) as premium_received,
    (SELECT total_claims FROM current_claims) + (SELECT total_unclaimed FROM current_unclaimed_pa) as medical_spending,
    ((SELECT total_claims FROM current_claims) + (SELECT total_unclaimed FROM current_unclaimed_pa)) / 
    NULLIF((SELECT total_premium FROM current_premium), 0) * 100 as mlr_percentage
UNION ALL
SELECT 
    'Last Contract' as period,
    (SELECT startdate FROM last_contract) as start_date,
    (SELECT enddate FROM last_contract) as end_date,
    (SELECT total_premium FROM last_premium) as premium_received,
    (SELECT total_claims FROM last_claims) + (SELECT total_unclaimed FROM last_unclaimed_pa) as medical_spending,
    ((SELECT total_claims FROM last_claims) + (SELECT total_unclaimed FROM last_unclaimed_pa)) / 
    NULLIF((SELECT total_premium FROM last_premium), 0) * 100 as mlr_percentage;
```

### Hospital Comparison

```sql
-- Hospitals used in each period
WITH current_hospitals AS (
    SELECT DISTINCT 
        p.providerid,
        p.providername,
        COUNT(DISTINCT pa.panumber) as case_count,
        SUM(pa.granted) as total_cost,
        AVG(pa.granted) as avg_cost_per_case
    FROM "AI DRIVEN DATA"."PA DATA" pa
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM current_contract)
      AND pa.requestdate <= (SELECT enddate FROM current_contract)
    GROUP BY p.providerid, p.providername
),
last_hospitals AS (
    SELECT DISTINCT 
        p.providerid,
        p.providername,
        COUNT(DISTINCT pa.panumber) as case_count,
        SUM(pa.granted) as total_cost,
        AVG(pa.granted) as avg_cost_per_case
    FROM "AI DRIVEN DATA"."PA DATA" pa
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM last_contract)
      AND pa.requestdate <= (SELECT enddate FROM last_contract)
    GROUP BY p.providerid, p.providername
)

-- New hospitals (in current but not in last)
SELECT 
    'NEW HOSPITAL' as change_type,
    ch.providername,
    ch.case_count,
    ch.total_cost,
    ch.avg_cost_per_case,
    NULL as last_period_cost,
    NULL as cost_change
FROM current_hospitals ch
LEFT JOIN last_hospitals lh ON ch.providerid = lh.providerid
WHERE lh.providerid IS NULL

UNION ALL

-- Dropped hospitals (in last but not in current)
SELECT 
    'DROPPED HOSPITAL' as change_type,
    lh.providername,
    NULL as case_count,
    NULL as total_cost,
    NULL as avg_cost_per_case,
    lh.total_cost as last_period_cost,
    NULL as cost_change
FROM last_hospitals lh
LEFT JOIN current_hospitals ch ON lh.providerid = ch.providerid
WHERE ch.providerid IS NULL

UNION ALL

-- Hospitals with cost changes
SELECT 
    'COST CHANGE' as change_type,
    ch.providername,
    ch.case_count,
    ch.total_cost,
    ch.avg_cost_per_case,
    lh.total_cost as last_period_cost,
    ch.total_cost - lh.total_cost as cost_change
FROM current_hospitals ch
INNER JOIN last_hospitals lh ON ch.providerid = lh.providerid
WHERE ABS(ch.total_cost - lh.total_cost) > 100000  -- Significant change threshold
ORDER BY cost_change DESC;
```

### Diagnosis Comparison

```sql
-- Diagnosis patterns between periods
WITH current_diagnoses AS (
    SELECT 
        td.code as diagnosiscode,
        d.diagnosisdesc,
        COUNT(*) as frequency,
        SUM(pa.granted) as total_cost
    FROM "AI DRIVEN DATA"."TBPADIAGNOSIS" td
    INNER JOIN "AI DRIVEN DATA"."PA DATA" pa ON CAST(td.panumber AS VARCHAR) = CAST(pa.panumber AS VARCHAR)
    LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON td.code = d.diagnosiscode
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM current_contract)
      AND pa.requestdate <= (SELECT enddate FROM current_contract)
    GROUP BY td.code, d.diagnosisdesc
),
last_diagnoses AS (
    SELECT 
        td.code as diagnosiscode,
        d.diagnosisdesc,
        COUNT(*) as frequency,
        SUM(pa.granted) as total_cost
    FROM "AI DRIVEN DATA"."TBPADIAGNOSIS" td
    INNER JOIN "AI DRIVEN DATA"."PA DATA" pa ON CAST(td.panumber AS VARCHAR) = CAST(pa.panumber AS VARCHAR)
    LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON td.code = d.diagnosiscode
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM last_contract)
      AND pa.requestdate <= (SELECT enddate FROM last_contract)
    GROUP BY td.code, d.diagnosisdesc
)

-- New diagnoses
SELECT 
    'NEW DIAGNOSIS' as change_type,
    cd.diagnosiscode,
    cd.diagnosisdesc,
    cd.frequency,
    cd.total_cost,
    NULL as last_frequency,
    NULL as frequency_change
FROM current_diagnoses cd
LEFT JOIN last_diagnoses ld ON cd.diagnosiscode = ld.diagnosiscode
WHERE ld.diagnosiscode IS NULL

UNION ALL

-- Diagnosis frequency changes
SELECT 
    'FREQUENCY CHANGE' as change_type,
    cd.diagnosiscode,
    cd.diagnosisdesc,
    cd.frequency,
    cd.total_cost,
    ld.frequency as last_frequency,
    cd.frequency - ld.frequency as frequency_change
FROM current_diagnoses cd
INNER JOIN last_diagnoses ld ON cd.diagnosiscode = ld.diagnosiscode
WHERE ABS(cd.frequency - ld.frequency) > 5  -- Significant change threshold
ORDER BY frequency_change DESC;
```

### Procedure Type Comparison

```sql
-- Surgical procedures
WITH current_surgeries AS (
    SELECT COUNT(*) as surgery_count, SUM(pa.granted) as surgery_cost
    FROM "AI DRIVEN DATA"."PA DATA" pa
    LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON pa.code = pd.procedurecode
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM current_contract)
      AND pa.requestdate <= (SELECT enddate FROM current_contract)
      AND (pd.proceduredesc ILIKE '%surgery%'
           OR pd.proceduredesc ILIKE '%surgical%'
           OR pd.proceduredesc ILIKE '%operation%')
),
last_surgeries AS (
    SELECT COUNT(*) as surgery_count, SUM(pa.granted) as surgery_cost
    FROM "AI DRIVEN DATA"."PA DATA" pa
    LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON pa.code = pd.procedurecode
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM last_contract)
      AND pa.requestdate <= (SELECT enddate FROM last_contract)
      AND (pd.proceduredesc ILIKE '%surgery%'
           OR pd.proceduredesc ILIKE '%surgical%'
           OR pd.proceduredesc ILIKE '%operation%')
)

SELECT 
    'SURGICAL PROCEDURES' as procedure_type,
    (SELECT surgery_count FROM current_surgeries) as current_count,
    (SELECT surgery_cost FROM current_surgeries) as current_cost,
    (SELECT surgery_count FROM last_surgeries) as last_count,
    (SELECT surgery_cost FROM last_surgeries) as last_cost,
    (SELECT surgery_count FROM current_surgeries) - (SELECT surgery_count FROM last_surgeries) as count_change,
    (SELECT surgery_cost FROM current_surgeries) - (SELECT surgery_cost FROM last_surgeries) as cost_change;

-- Similar queries for dental, specialty procedures, etc.
```

---

## Usage Notes

1. **Replace placeholders**: Replace `{enrollee_id}`, `{company_name}`, `{period_start}`, `{period_end}` with actual values
2. **Date calculations**: For "last 6 months", use `CURRENT_DATE - INTERVAL '6 months'` as period_start
3. **Error handling**: Always check if contracts exist before running MLR comparison queries
4. **Performance**: These queries can be large - consider adding LIMIT clauses for initial exploration
5. **Null handling**: Use COALESCE or NULLIF to handle division by zero in MLR calculations


