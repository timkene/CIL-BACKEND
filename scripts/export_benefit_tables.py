#!/usr/bin/env python3
"""
Export benefit-related tables to Excel with separate sheets:
- planbenefitcode_limit
- benefitcode
- benefitcode_procedure
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

def export_benefit_tables(output_file: str = None):
    """Export benefit tables to Excel"""
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"benefit_tables_export_{timestamp}.xlsx"
    
    print(f"📊 Exporting benefit tables to {output_file}...")
    
    # Use direct read-only connection to avoid lock conflicts with running API
    db_path = project_root / "ai_driven_data.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Get all three tables
        tables = {
            'planbenefitcode_limit': 'SELECT * FROM "AI DRIVEN DATA"."planbenefitcode_limit"',
            'benefitcode': 'SELECT * FROM "AI DRIVEN DATA"."benefitcode"',
            'benefitcode_procedure': 'SELECT * FROM "AI DRIVEN DATA"."benefitcode_procedure"'
        }
        
        dfs = {}
        for table_name, query in tables.items():
            try:
                print(f"  Loading {table_name}...")
                df = conn.execute(query).fetchdf()
                print(f"    ✅ {len(df)} rows")
                dfs[table_name] = df
            except Exception as e:
                print(f"    ❌ Error loading {table_name}: {e}")
                dfs[table_name] = pd.DataFrame()  # Empty DataFrame if table doesn't exist
        
        # Write to Excel with separate sheets
        print(f"\n📝 Writing to Excel...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheet_name, df in dfs.items():
                if not df.empty:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"  ✅ Sheet '{sheet_name}': {len(df)} rows")
                else:
                    # Create empty sheet with column headers if we know them
                    empty_df = pd.DataFrame()
                    empty_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"  ⚠️  Sheet '{sheet_name}': empty")
        
        print(f"\n✅ Export complete: {output_file}")
        print(f"   Total sheets: {len(dfs)}")
        return output_file
        
    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Export benefit tables to Excel")
    parser.add_argument("-o", "--output", help="Output Excel file path", default=None)
    args = parser.parse_args()
    
    export_benefit_tables(args.output)
