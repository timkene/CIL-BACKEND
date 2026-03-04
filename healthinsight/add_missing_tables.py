import os
import sys
import duckdb
import pandas as pd

# Ensure project root is importable
DLT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if DLT_ROOT not in sys.path:
    sys.path.insert(0, DLT_ROOT)

from dlt_sources import tbpadiagnosis, tariff as tariff_src, providers_tariff as providers_tariff_src


def load_table(con, name: str, df: pd.DataFrame, create_sql: str):
    con.execute('CREATE SCHEMA IF NOT EXISTS "AI DRIVEN DATA"')
    con.execute(create_sql)
    con.execute(f'DELETE FROM "AI DRIVEN DATA"."{name}"')
    con.register("df_src", df)
    con.execute(f'INSERT INTO "AI DRIVEN DATA"."{name}" SELECT * FROM df_src')


def main() -> None:
    db_path = os.path.join(DLT_ROOT, "ai_driven_data.duckdb")
    con = duckdb.connect(db_path)

    # TBPADIAGNOSIS
    try:
        df = list(tbpadiagnosis())[0]
    except Exception as e:
        print(f"⚠️ tbpadiagnosis fetch failed: {e}")
        df = pd.DataFrame(columns=["uniqueid","code","desc","panumber"])  # minimal shape
    load_table(
        con,
        "TBPADIAGNOSIS",
        df,
        'CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."TBPADIAGNOSIS" AS SELECT * FROM (SELECT 1 WHERE 0)'
    )
    cnt = con.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."TBPADIAGNOSIS"').fetchone()[0]
    print(f"✅ TBPADIAGNOSIS rows: {cnt}")

    # TARIFF
    try:
        df = list(tariff_src())[0]
    except Exception as e:
        print(f"⚠️ tariff fetch failed: {e}")
        df = pd.DataFrame(columns=["tariffid","tariffname","tariffamount","procedurecode","comments","expirydate"])
    load_table(
        con,
        "TARIFF",
        df,
        'CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."TARIFF" (tariffid VARCHAR, tariffname VARCHAR, tariffamount DOUBLE, procedurecode VARCHAR, comments VARCHAR, expirydate TIMESTAMP)'
    )
    cnt = con.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."TARIFF"').fetchone()[0]
    print(f"✅ TARIFF rows: {cnt}")

    # PROVIDERS_TARIFF
    try:
        df = list(providers_tariff_src())[0]
    except Exception as e:
        print(f"⚠️ providers_tariff fetch failed: {e}")
        df = pd.DataFrame(columns=["tariffprovid","tariffid","providerid","tariffdescr","dateadded"])
    load_table(
        con,
        "PROVIDERS_TARIFF",
        df,
        'CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PROVIDERS_TARIFF" (tariffprovid VARCHAR, tariffid VARCHAR, providerid VARCHAR, tariffdescr VARCHAR, dateadded TIMESTAMP)'
    )
    cnt = con.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."PROVIDERS_TARIFF"').fetchone()[0]
    print(f"✅ PROVIDERS_TARIFF rows: {cnt}")

    con.close()


if __name__ == "__main__":
    main()


