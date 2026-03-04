# Renewal Analysis Testing Guide

## Current Status

The Renewal Analysis module has been built but is returning incomplete data. Here's how to test and debug:

## 1. Test the Backend API Directly

### Test Company List
```bash
curl http://localhost:8000/api/v1/renewal/companies
```

### Test Analysis for AIR PEACE NIGERIA
```bash
curl -X POST http://localhost:8000/api/v1/renewal/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "company_name": "AIR PEACE NIGERIA",
    "use_motherduck": false
  }' | python3 -m json.tool
```

This will show you EXACTLY what data is being returned.

## 2. Check What's Missing

Based on the Streamlit version, we need these fields that might be missing:

### In `benefit_analysis`:
- `total_violations`
- `total_overage_amount`
- `violations_by_benefit`
- `unlimited_benefits`

### In `pa` data:
- `approval_rate`
- `conversion_rate`
- `denial_rate`

### In `plan_analysis`:
- `over_utilizing_plans` (enrollment % vs cost %)
- `under_utilizing_plans`
- `balanced_plans`

### In `conditions`:
- `one_off_pct` (maternity + surgery + trauma)
- Should already have `chronic_pct`, `preventable_pct`, `catastrophic_pct`

### In `provider_bands`:
- Distribution by band (A, B, C, D, Special, Unknown)
- Percentage in each band

## 3. Most Likely Issues

### Issue 1: CalculationEngine Methods Not Returning Complete Data
Some methods in `complete_calculation_engine.py` might return dictionaries with missing keys.

**Solution**: Add default values in the API or ensure CalculationEngine returns all expected fields.

### Issue 2: Field Name Mismatches
The API might be using different field names than expected.

**Solution**: Check the actual response and update frontend components to use correct field names.

### Issue 3: Methods Not Being Called
Some analysis methods might not be called in the `/analyze` endpoint.

**Solution**: Verify all these are being called in `/api/routes/renewal.py`:
- `analyze_company_benefit_limits()`
- `analyze_plan_distribution()`
- `analyze_chronic_disease_burden()`
- `decompose_claims_trend()`
- `calculate_monthly_pmpm_trend()`

## 4. Quick Fix for Frontend

To see what data IS available, temporarily add console logging:

```javascript
// In RenewalAnalysis.jsx, after receiving analysisData:
console.log('Full Analysis Data:', JSON.stringify(analysisData, null, 2));
```

Then check browser console to see the actual data structure.

## 5. To Fix Missing Data

Once you identify what's missing:

1. **If data exists but has different field names**: Update frontend components
2. **If calculation methods aren't returning the data**: Update `complete_calculation_engine.py`
3. **If methods aren't being called**: Update the `/analyze` endpoint in `api/routes/renewal.py`

## 6. Expected Complete Data Structure

```json
{
  "success": true,
  "company_name": "AIR PEACE NIGERIA",
  "contract": {
    "groupid": "...",
    "startdate": "2025-07-01",
    "enddate": "2026-06-30",
    "months_elapsed": 5,
    "months_to_end": 7
  },
  "financial": {
    "debit": 100000000,
    "cash": 55900000,
    "outstanding": 40800000,
    "payment_rate": 55.9,
    "commission": 10000000,
    "commission_rate": 0.10
  },
  "mlr": {
    "bv_mlr": 75.4,
    "cash_mlr": 134.9,
    "pmpm": 5215,
    "pmpm_variance": 73.8,
    "monthly_burn": 15000000,
    "target_mlr": 75
  },
  "claims": {
    "total_claims": 5000,
    "unique_claimants": 850,
    "total_cost": 75000000
  },
  "enrollment": {
    "member_count": 1985,
    "total": 1985
  },
  "concentration": {
    "pattern_type": "STRUCTURAL",
    "top5_pct": 5.5,
    "top10_pct": 12.3,
    "top10_members": [...]
  },
  "conditions": {
    "one_off_pct": 45.2,
    "chronic_pct": 8.3,
    "preventable_pct": 27.2,
    "catastrophic_pct": 2.1
  },
  "fraud": {
    "risk_level": "MEDIUM RISK",
    "unknown_pct": 0.0,
    "unknown_amount": 0,
    "same_day_count": 1334,
    "same_day_instances": [...]
  },
  "benefit_analysis": {
    "total_violations": 30,
    "total_overage_amount": 596000,
    "violations_by_benefit": [...],
    "unlimited_benefits": [...]
  },
  "plan_analysis": {
    "over_utilizing_plans": [],
    "under_utilizing_plans": [],
    "balanced_plans": [...]
  },
  "risk_score": {
    "total_score": 73,
    "mlr_risk": 22,
    "hcc_persistence": 18,
    "chronic_burden": 15,
    "collection_risk": 11,
    "claims_trend": 7
  }
}
```

## 7. Testing Workflow

1. Run the backend server: `python main.py`
2. Test API endpoint with curl (see above)
3. Save the JSON response
4. Compare with expected structure
5. Identify missing/incorrect fields
6. Fix the appropriate layer (engine/API/frontend)
7. Re-test

## 8. Frontend Console Debugging

Add to any tab component to see what data it's receiving:

```javascript
useEffect(() => {
  console.log('Component Data:', data);
  console.log('Specific Section:', data?.section_name);
}, [data]);
```
