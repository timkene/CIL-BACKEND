# AI DRIVEN DATA – Procedure & Diagnosis Tables

From your local DuckDB schema **`"AI DRIVEN DATA"`**, these are the two tables used for procedure and diagnosis lookups.

---

## 1. PROCEDURE DATA (procedure table)

| Property | Value |
|----------|--------|
| **Schema** | `"AI DRIVEN DATA"` |
| **Table name** | `"PROCEDURE DATA"` *(space, not underscore)* |
| **Full reference** | `"AI DRIVEN DATA"."PROCEDURE DATA"` |

### Columns

| Column            | Type    | Description                    |
|-------------------|---------|--------------------------------|
| `procedurecode`   | VARCHAR | Procedure code (e.g. DRG1106)  |
| `proceduredesc`   | VARCHAR | Procedure name/description    |

**Note:** There is no `code`, `procedurename`, `category`, or `description` column. Use `procedurecode` and `proceduredesc` only.

### Example query

```sql
SELECT procedurecode, proceduredesc
FROM "AI DRIVEN DATA"."PROCEDURE DATA"
WHERE LOWER(TRIM(procedurecode)) = LOWER(TRIM('DRG1106'))
LIMIT 1;
```

### Usage in codebase

- `api/routes/providers.py`: `LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON c.code = pd.procedurecode`
- `api/routes/paclaims.py`: same join; uses `pd.proceduredesc` as procedure name
- `healthinsight/setup_ai_database.py`: `CREATE TABLE ... "PROCEDURE DATA" (procedurecode VARCHAR, proceduredesc VARCHAR)`

---

## 2. DIAGNOSIS (diagnosis table)

| Property | Value |
|----------|--------|
| **Schema** | `"AI DRIVEN DATA"` |
| **Table name** | `"DIAGNOSIS"` |
| **Full reference** | `"AI DRIVEN DATA"."DIAGNOSIS"` |

### Columns

| Column          | Type    | Description                      |
|-----------------|---------|----------------------------------|
| `diagnosiscode` | VARCHAR | Diagnosis code (e.g. J069)       |
| `diagnosisdesc` | VARCHAR | Diagnosis description/name      |

Some loads may add a `category` column; if missing, use only the two columns above.

### Example query

```sql
SELECT diagnosiscode, diagnosisdesc
FROM "AI DRIVEN DATA"."DIAGNOSIS"
WHERE LOWER(TRIM(diagnosiscode)) = LOWER(TRIM('J069'))
LIMIT 1;
```

### Usage in codebase

- `api/routes/providers.py`: `LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON c.diagnosiscode = d.diagnosiscode`
- `api/routes/paclaims.py`: same; uses `d.diagnosisdesc` as diagnosis name
- `services/enrollee_service.py`, `claims_vet_pa_claims.py`, etc.: same pattern

---

## Summary

| Table            | Full name                      | Key columns                          |
|------------------|---------------------------------|--------------------------------------|
| Procedure table  | `"AI DRIVEN DATA"."PROCEDURE DATA"` | `procedurecode`, `proceduredesc`  |
| Diagnosis table  | `"AI DRIVEN DATA"."DIAGNOSIS"`       | `diagnosiscode`, `diagnosisdesc`  |

Run `python verify_table_names.py` in the project root (with DuckDB and `ai_driven_data.duckdb` in place) to confirm table and column names in your local database.
