import duckdb
import pandas as pd
from dlt_sources import fin_gl

DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'

EXCEL_2023_2024 = '/Users/kenechukwuchukwuka/Downloads/DLT/GL 2023 and 2024.xlsx'


def load_excel_to_table(conn: duckdb.DuckDBPyConnection, path: str, table: str, sheet_name: str = None) -> int:
    if sheet_name:
        df = pd.read_excel(path, sheet_name=sheet_name)
    else:
        df = pd.read_excel(path)
    
    # Convert all column headers to lowercase
    df.columns = df.columns.str.lower()
    
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
    conn.execute(f'CREATE TABLE "{SCHEMA}"."{table}" AS SELECT * FROM df')
    return conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{table}"').fetchone()[0]


def load_fin_gl_2025(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    try:
        df25 = list(fin_gl())[0]
        if df25.empty:
            print(f"⚠️ Warning: fin_gl returned empty DataFrame, creating empty table structure")
            # Create empty table with expected schema
            conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
            conn.execute(f'CREATE TABLE "{SCHEMA}"."{table}" (GLCode VARCHAR, Description VARCHAR, Amount DOUBLE, Date DATE)')
            return 0
        else:
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" AS SELECT * FROM df25')
            conn.execute(f'DELETE FROM "{SCHEMA}"."{table}"')
            conn.execute(f'INSERT INTO "{SCHEMA}"."{table}" SELECT * FROM df25')
            return conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{table}"').fetchone()[0]
    except Exception as e:
        print(f"⚠️ Warning: Failed to load fin_gl: {e}")
        # Create empty table with expected schema
        conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
        conn.execute(f'CREATE TABLE "{SCHEMA}"."{table}" (GLCode VARCHAR, Description VARCHAR, Amount DOUBLE, Date DATE)')
        return 0


def main() -> None:
    conn = duckdb.connect(DB_PATH)
    print('🔄 Loading GL data into DuckDB...')

    # Load 2023 data from first sheet
    c23 = load_excel_to_table(conn, EXCEL_2023_2024, 'FIN_GL_2023_RAW', 'GL2023')
    print(f'✅ FIN_GL_2023_RAW rows: {c23:,}')

    # Load 2024 data from second sheet
    c24 = load_excel_to_table(conn, EXCEL_2023_2024, 'FIN_GL_2024_RAW', 'GL2024')
    print(f'✅ FIN_GL_2024_RAW rows: {c24:,}')

    # Load 2025 data
    c25 = load_fin_gl_2025(conn, 'FIN_GL_2025_RAW')
    print(f'✅ FIN_GL_2025_RAW rows: {c25:,}')

    conn.close()
    print('🎉 GL raw tables ready in schema AI DRIVEN DATA')


if __name__ == '__main__':
    main()


