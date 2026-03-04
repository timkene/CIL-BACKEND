
import pandas as pd
from datetime import datetime
from dlt_sources import group_contract, debit_note

# Get the dataframes from dlt_sources
group_contract_data = list(group_contract())[0]  # Get DataFrame from generator
debit_note_data = list(debit_note())[0]  # Get DataFrame from generator

group_contract_df = group_contract_data
debit_note_df = debit_note_data

# Convert date columns to datetime
group_contract_df['startdate'] = pd.to_datetime(group_contract_df['startdate'])
debit_note_df['From'] = pd.to_datetime(debit_note_df['From'])

# Filter contracts that start in July and August 2025
july_august_2025_contracts = group_contract_df[
    (group_contract_df['startdate'].dt.year == 2025) & 
    (group_contract_df['startdate'].dt.month.isin([1, 2, 3, 4, 5 ,6, 7, 8]))
]

# Filter debit notes with 'From' date in July and August 2025
july_august_2025_debit_notes = debit_note_df[
    (debit_note_df['From'].dt.year == 2025) & 
    (debit_note_df['From'].dt.month.isin([1, 2, 3, 4, 5 ,6, 7, 8]))
]

# Get unique company names from each dataset
contract_companies = set(july_august_2025_contracts['groupname'].unique())
debit_note_companies = set(july_august_2025_debit_notes['CompanyName'].unique())

# Find companies in contracts but not in debit notes
companies_without_debit_notes = contract_companies - debit_note_companies

print("=== ANALYSIS RESULTS ===")
print(f"Total companies with contracts starting July-August 2025: {len(contract_companies)}")
print(f"Total companies with debit notes in July-August 2025: {len(debit_note_companies)}")
print(f"Companies with contracts but NO debit notes: {len(companies_without_debit_notes)}")

print("\n=== COMPANIES WITH CONTRACTS BUT NO DEBIT NOTES ===")
if companies_without_debit_notes:
    for i, company in enumerate(sorted(companies_without_debit_notes), 1):
        print(f"{i}. {company}")
else:
    print("All companies with July-August 2025 contracts have corresponding debit notes.")

print("\n=== DETAILED BREAKDOWN ===")
print("Companies in July-August 2025 contracts:")
for company in sorted(contract_companies):
    print(f"  - {company}")

print("\nCompanies in July-August 2025 debit notes:")
for company in sorted(debit_note_companies):
    print(f"  - {company}")

# Optional: Show contract details for companies without debit notes
if companies_without_debit_notes:
    print("\n=== CONTRACT DETAILS FOR COMPANIES WITHOUT DEBIT NOTES ===")
    missing_companies_contracts = july_august_2025_contracts[
        july_august_2025_contracts['groupname'].isin(companies_without_debit_notes)
    ]
    
    for _, row in missing_companies_contracts.iterrows():
        print(f"Company: {row['groupname']}")
        print(f"  Start Date: {row['startdate'].strftime('%Y-%m-%d')}")
        print(f"  End Date: {row['enddate']}")
        print(f"  Group ID: {row['groupid']}")
        print("  ---")
