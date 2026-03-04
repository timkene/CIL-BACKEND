import os
import duckdb
import pandas as pd

# Ensure we can import from project root
DLT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
if DLT_ROOT not in sys.path:
    sys.path.insert(0, DLT_ROOT)

from dlt_sources import tbpadiagnosis


def main() -> None:
    # Fetch MediCloud tbpadiagnosis into a DataFrame
    try:
        df = list(tbpadiagnosis())[0]
    except Exception as e:
        print(f"ERROR: failed to fetch tbpadiagnosis from MediCloud: {e}")
        df = pd.DataFrame(columns=["uniqueid", "code", "desc", "panumber"])

    db_path = os.path.join(DLT_ROOT, "ai_driven_data.duckdb")
    con = duckdb.connect(db_path)
    con.execute('CREATE SCHEMA IF NOT EXISTS "AI DRIVEN DATA"')

    # Replace table
    con.register("df_src", df)
    con.execute('DROP TABLE IF EXISTS "AI DRIVEN DATA"."TBPADIAGNOSIS"')
    con.execute('CREATE TABLE "AI DRIVEN DATA"."TBPADIAGNOSIS" AS SELECT * FROM df_src')

    rows = con.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."TBPADIAGNOSIS"').fetchone()[0]
    con.close()
    print(f"Loaded AI DRIVEN DATA.TBPADIAGNOSIS rows: {rows}")


if __name__ == "__main__":
    main()


