import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dlt_sources import claims  # claims is a generator resource, so needs to be called - see below

def create_sample_claims_data():
    """
    Create sample data that matches the structure of the claims SQL table.
    Structure derived from dlt_sources.py claims resource.
    """
    print("Creating sample data structure for claims...")

    n = 10  # sample row count
    now = datetime.now()
    sample_data = {
        'nhislegacynumber': [f'LEG{str(i).zfill(5)}' for i in range(1, n+1)],
        'nhisproviderid': [f'PROV{str(i).zfill(3)}' for i in range(1, n+1)],
        'nhisgroupid': [f'GRP{str(i).zfill(4)}' for i in range(1, n+1)],
        'panumber': [f'PA{str(i).zfill(6)}' for i in range(1, n+1)],
        'encounterdatefrom': [(now - timedelta(days=i*7)).date() for i in range(n)],
        'datesubmitted': [(now - timedelta(days=i*5)).date() for i in range(n)],
        'chargeamount': [np.random.uniform(5000, 220000) for _ in range(n)],
        'approvedamount': [np.random.uniform(2000, 150000) for _ in range(n)],
        'procedurecode': [f'PROC{str(i).zfill(3)}' for i in range(1, n+1)],
        'deniedamount': [np.random.uniform(0, 30000) for _ in range(n)]
    }

    df = pd.DataFrame(sample_data)
    return df

def display_claims_sample():
    """
    Fetch data from claims and display first 10 rows of each column
    """
    print("Fetching data from claims...")

    # Try to fetch real data first
    try:
        # Claims is a generator; get the DataFrame from the generator
        df = next(claims())
    except Exception as e:
        print(f"❌ Failed to fetch data from claims ({e})")
        df = None

    if df is None or df.empty:
        print("❌ Failed to fetch data from claims (database connection issue or empty result)")
        print("📝 Creating sample claims data to show structure...")
        df = create_sample_claims_data()

    print(f"✅ Successfully loaded {len(df)} rows")
    print(f"📊 Data shape: {df.shape}")
    print(f"📋 Columns: {list(df.columns)}")

    print("\n" + "="*80)
    print("FIRST 10 ROWS OF EACH COLUMN")
    print("="*80)

    # Display first 10 rows
    print("\n📄 First 10 rows of the dataset:")
    print(df.head(10).to_string())

    print("\n" + "="*80)
    print("COLUMN-WISE SAMPLE DATA")
    print("="*80)

    # Display sample data for each column
    for col in df.columns:
        print(f"\n🔹 Column: {col}")
        print(f"   Data type: {df[col].dtype}")
        print(f"   Non-null values: {df[col].notna().sum()}/{len(df)}")
        print(f"   Unique values: {df[col].nunique()}")

        # Show first 10 non-null values
        sample_values = df[col].dropna().head(10).tolist()
        print(f"   Sample values: {sample_values}")

        # Show value counts if it's a categorical column with few unique values
        if df[col].nunique() <= 20 and df[col].dtype == 'object':
            print(f"   Value counts:")
            value_counts = df[col].value_counts().head(5)
            for val, count in value_counts.items():
                print(f"     '{val}': {count}")

        print("-" * 60)

if __name__ == "__main__":
    display_claims_sample()
