#!/usr/bin/env python3
"""
Export benefitcode and benefitcode_procedure tables from DuckDB to Excel.
"""
import sys
from pathlib import Path
import pandas as pd
import duckdb
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def export_benefitcode_tables():
    """Export benefitcode and benefitcode_procedure from DuckDB"""
    db_path = project_root / "ai_driven_data.duckdb"
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    print(f"📊 Connecting to DuckDB: {db_path}")
    
    try:
        # Use read-only connection
        conn = duckdb.connect(str(db_path), read_only=True)
        
        # First, list all tables to find the correct names
        print("\n🔍 Searching for benefit tables...")
        try:
            all_tables = conn.execute("SHOW TABLES").fetchdf()
            print(f"   Found {len(all_tables)} tables total")
            
            # Look for benefit-related tables
            benefit_tables = all_tables[all_tables['name'].str.contains('benefit', case=False)]
            if not benefit_tables.empty:
                print(f"   Benefit-related tables found:")
                for _, row in benefit_tables.iterrows():
                    print(f"     - {row['name']}")
        except Exception as e:
            print(f"   Could not list tables: {e}")
            all_tables = pd.DataFrame()
        
        # Try to find and export benefitcode
        print("\n📋 Exporting benefitcode...")
        benefitcode_df = None
        benefitcode_variations = ['benefitcode', 'BENEFITCODE', 'BenefitCode', 'BENEFIT_CODE']
        
        for var in benefitcode_variations:
            try:
                # Try with schema
                query = f'SELECT * FROM "AI DRIVEN DATA"."{var}"'
                df = conn.execute(query).fetchdf()
                if not df.empty:
                    benefitcode_df = df
                    print(f"   ✅ Found '{var}' in schema 'AI DRIVEN DATA': {len(df)} rows")
                    break
            except:
                try:
                    # Try without schema
                    query = f'SELECT * FROM "{var}"'
                    df = conn.execute(query).fetchdf()
                    if not df.empty:
                        benefitcode_df = df
                        print(f"   ✅ Found '{var}' (no schema): {len(df)} rows")
                        break
                except:
                    continue
        
        if benefitcode_df is None or benefitcode_df.empty:
            print("   ⚠️  benefitcode table not found or empty")
            benefitcode_df = pd.DataFrame()
        
        # Try to find and export benefitcode_procedure
        print("\n📋 Exporting benefitcode_procedure...")
        benefitcode_procedure_df = None
        procedure_variations = ['benefitcode_procedure', 'BENEFITCODE_PROCEDURE', 'BenefitCode_Procedure', 'BENEFIT_CODE_PROCEDURE']
        
        for var in procedure_variations:
            try:
                # Try with schema
                query = f'SELECT * FROM "AI DRIVEN DATA"."{var}"'
                df = conn.execute(query).fetchdf()
                if not df.empty:
                    benefitcode_procedure_df = df
                    print(f"   ✅ Found '{var}' in schema 'AI DRIVEN DATA': {len(df)} rows")
                    break
            except:
                try:
                    # Try without schema
                    query = f'SELECT * FROM "{var}"'
                    df = conn.execute(query).fetchdf()
                    if not df.empty:
                        benefitcode_procedure_df = df
                        print(f"   ✅ Found '{var}' (no schema): {len(df)} rows")
                        break
                except:
                    continue
        
        if benefitcode_procedure_df is None or benefitcode_procedure_df.empty:
            print("   ⚠️  benefitcode_procedure table not found or empty")
            benefitcode_procedure_df = pd.DataFrame()
        
        # Export to Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = project_root / f'benefitcode_export_{timestamp}.xlsx'
        
        print(f"\n📝 Writing to Excel: {output_file}")
        with pd.ExcelWriter(str(output_file), engine='openpyxl') as writer:
            if not benefitcode_df.empty:
                benefitcode_df.to_excel(writer, sheet_name='benefitcode', index=False)
                print(f"   ✅ Sheet 'benefitcode': {len(benefitcode_df)} rows, {len(benefitcode_df.columns)} columns")
            else:
                pd.DataFrame().to_excel(writer, sheet_name='benefitcode', index=False)
                print(f"   ⚠️  Sheet 'benefitcode': empty")
            
            if not benefitcode_procedure_df.empty:
                benefitcode_procedure_df.to_excel(writer, sheet_name='benefitcode_procedure', index=False)
                print(f"   ✅ Sheet 'benefitcode_procedure': {len(benefitcode_procedure_df)} rows, {len(benefitcode_procedure_df.columns)} columns")
            else:
                pd.DataFrame().to_excel(writer, sheet_name='benefitcode_procedure', index=False)
                print(f"   ⚠️  Sheet 'benefitcode_procedure': empty")
        
        print(f"\n✅ Export complete: {output_file}")
        conn.close()
        return str(output_file)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    export_benefitcode_tables()
