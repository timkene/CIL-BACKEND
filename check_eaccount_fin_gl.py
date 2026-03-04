import sys

try:
    from dlt_sources import create_eacount_connection
except Exception as e:
    print("Error importing create_eacount_connection from dlt_sources:", e)
    sys.exit(1)


def main():
    import pyodbc

    print("=" * 70)
    print("Checking EACCOUNT for FIN_GL_* tables")
    print("=" * 70)

    try:
        conn = create_eacount_connection()
        cursor = conn.cursor()

        # List all FIN_GL-related tables
        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_NAME LIKE 'FIN_GL%'
        ORDER BY TABLE_NAME
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print("No FIN_GL* tables found in EACCOUNT")
        else:
            print("Found FIN_GL-related tables:")
            for schema, name in rows:
                print(f"  {schema}.{name}")

        print("\nSpecific checks:")
        for year in [2023, 2024, 2025]:
            table_name = f"FIN_GL_{year}_RAW"
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_NAME = ?
                """,
                (table_name,),
            )
            exists = cursor.fetchone()[0] > 0
            print(f"  {table_name}: {'FOUND' if exists else 'not found'}")

        conn.close()
    except Exception as e:
        print("Error connecting to EACCOUNT or querying tables:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()

