import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
import re
from collections import defaultdict

def clean_amount(amount):
    """Convert amount to float, handling various formats"""
    if pd.isna(amount):
        return None
    if isinstance(amount, (int, float)):
        return float(amount)
    # Remove commas and convert to float
    return float(str(amount).replace(',', ''))

def extract_entity_names(description):
    """
    Extract potential entity names from transaction descriptions.
    Looks for patterns like hospital names, company names, etc.
    """
    if pd.isna(description):
        return []
    
    desc = str(description).upper()
    entities = []
    
    # Pattern 1: Extract text between "TO" and common suffixes
    patterns = [
        r'TO\s+([A-Z\s\-\.&]+?)(?:\s+GAPS|\s+ZBN|\s+LTD|\s+HOSPITAL|\s+SPECIALIST|\s+CLINIC|$)',
        r'FROM\s+([A-Z\s\-\.&]+?)(?:\s+GAPS|\s+TO|\s+LTD|\s+HOSPITAL|\s+SPECIALIST|\s+CLINIC|$)',
        # Look for sequences of capitalized words (potential names)
        r'\b([A-Z][A-Z\s\-\.&]{10,50}?)(?:\s+GAPS|\s+LTD|\s+HOSPITAL|\s+SPECIALIST|\s+CLINIC|\s+\d)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, desc)
        entities.extend([m.strip() for m in matches if len(m.strip()) > 5])
    
    # Also add the original description (cleaned)
    # Remove common transaction prefixes and reference numbers
    cleaned = re.sub(r'NIBSS.*?(?=\s[A-Z]{3,})', '', desc)
    cleaned = re.sub(r'TRANSFER.*?(?=\s[A-Z]{3,})', '', cleaned)
    cleaned = re.sub(r'\d{10,}', '', cleaned)  # Remove long numbers
    cleaned = re.sub(r'638\d+\-\d+', '', cleaned)  # Remove specific ref patterns
    cleaned = re.sub(r'GAPS\d+S?', '', cleaned)
    cleaned = cleaned.strip()
    
    if len(cleaned) > 5:
        entities.append(cleaned)
    
    return list(set(entities))  # Remove duplicates

def find_best_match(source_desc, target_descriptions, threshold=60):
    """
    Find the best matching description using fuzzy matching.
    Returns (best_match_index, score)
    """
    source_desc = str(source_desc).upper()
    
    # Extract potential entity names from source
    source_entities = extract_entity_names(source_desc)
    
    best_score = 0
    best_idx = None
    
    for idx, target_desc in enumerate(target_descriptions):
        target_desc_str = str(target_desc).upper()
        target_entities = extract_entity_names(target_desc)
        
        # Score 1: Direct partial match (fastest check)
        if source_desc in target_desc_str or target_desc_str in source_desc:
            score = 100
        else:
            # Score 2: Check if source entities appear in target description
            score = 0
            for entity in source_entities:
                if entity in target_desc_str:
                    score = max(score, 95)
                else:
                    # Score 3: Fuzzy match entities
                    for target_entity in target_entities:
                        fuzzy_score = fuzz.partial_ratio(entity, target_entity)
                        score = max(score, fuzzy_score)
        
        if score > best_score:
            best_score = score
            best_idx = idx
    
    if best_score >= threshold:
        return best_idx, best_score
    return None, 0

def reconcile_sheets(file1_path, file2_path, sheet1_name=0, sheet2_name=0, 
                     amount_threshold=0.01, fuzzy_threshold=60):
    """
    Reconcile two Excel sheets based on amount and description matching.
    
    Parameters:
    - file1_path: Path to first Excel file
    - file2_path: Path to second Excel file
    - sheet1_name: Sheet name or index for first file (default: 0)
    - sheet2_name: Sheet name or index for second file (default: 0)
    - amount_threshold: Maximum difference in amounts to consider a match (default: 0.01)
    - fuzzy_threshold: Minimum fuzzy match score (0-100) for descriptions (default: 60)
    """
    
    print("Loading Excel files...")
    df1 = pd.read_excel(file1_path, sheet_name=sheet1_name)
    df2 = pd.read_excel(file2_path, sheet_name=sheet2_name)
    
    # Standardize column names (case-insensitive)
    df1.columns = df1.columns.str.lower().str.strip()
    df2.columns = df2.columns.str.lower().str.strip()
    
    # Clean amounts
    print("Cleaning amounts...")
    df1['amount_clean'] = df1['amount'].apply(clean_amount)
    df2['amount_clean'] = df2['amount'].apply(clean_amount)
    
    # Remove rows with null amounts
    df1 = df1.dropna(subset=['amount_clean'])
    df2 = df2.dropna(subset=['amount_clean'])
    
    print(f"Sheet 1: {len(df1)} records")
    print(f"Sheet 2: {len(df2)} records")
    
    # Group by amount for faster matching
    print("\nGrouping transactions by amount...")
    amount_groups_2 = defaultdict(list)
    for idx, row in df2.iterrows():
        amount_groups_2[row['amount_clean']].append(idx)
    
    # Track matches
    matches = []
    matched_indices_2 = set()
    
    print("\nMatching transactions...")
    for idx1, row1 in df1.iterrows():
        if idx1 % 100 == 0:
            print(f"Processing record {idx1}/{len(df1)}...")
        
        amount1 = row1['amount_clean']
        desc1 = row1['description']
        
        # Find potential amount matches in sheet 2
        candidates = []
        for amount2 in amount_groups_2.keys():
            if abs(amount1 - amount2) <= amount_threshold:
                candidates.extend(amount_groups_2[amount2])
        
        # Filter out already matched records
        candidates = [c for c in candidates if c not in matched_indices_2]
        
        if not candidates:
            continue
        
        # Get descriptions of candidates
        candidate_descs = df2.loc[candidates, 'description'].tolist()
        
        # Find best description match
        best_idx_in_candidates, score = find_best_match(desc1, candidate_descs, fuzzy_threshold)
        
        if best_idx_in_candidates is not None:
            idx2 = candidates[best_idx_in_candidates]
            row2 = df2.loc[idx2]
            
            matches.append({
                'Sheet1_Index': idx1,
                'Sheet2_Index': idx2,
                'Amount_Sheet1': row1['amount'],
                'Amount_Sheet2': row2['amount'],
                'Description_Sheet1': desc1,
                'Description_Sheet2': row2['description'],
                'Match_Score': score,
                'Amount_Difference': abs(amount1 - row2['amount_clean'])
            })
            
            matched_indices_2.add(idx2)
    
    print(f"\nMatching complete! Found {len(matches)} matches.")
    
    # Create results DataFrames
    matched_df = pd.DataFrame(matches)
    
    # Unmatched records
    unmatched_1 = df1[~df1.index.isin(matched_df['Sheet1_Index'])]
    unmatched_2 = df2[~df2.index.isin(matched_df['Sheet2_Index'])]
    
    print(f"Unmatched in Sheet 1: {len(unmatched_1)}")
    print(f"Unmatched in Sheet 2: {len(unmatched_2)}")
    
    # Save results
    print("\nSaving results...")
    with pd.ExcelWriter('reconciliation_results.xlsx', engine='openpyxl') as writer:
        matched_df.to_excel(writer, sheet_name='Matched', index=False)
        unmatched_1[['amount', 'description']].to_excel(writer, sheet_name='Unmatched_Sheet1', index=False)
        unmatched_2[['amount', 'description']].to_excel(writer, sheet_name='Unmatched_Sheet2', index=False)
        
        # Summary
        summary = pd.DataFrame({
            'Metric': ['Total Sheet 1', 'Total Sheet 2', 'Matched', 'Unmatched Sheet 1', 'Unmatched Sheet 2', 'Match Rate (%)'],
            'Value': [len(df1), len(df2), len(matches), len(unmatched_1), len(unmatched_2), 
                     round(len(matches) / len(df1) * 100, 2) if len(df1) > 0 else 0]
        })
        summary.to_excel(writer, sheet_name='Summary', index=False)
    
    print("Results saved to 'reconciliation_results.xlsx'")
    return matched_df, unmatched_1, unmatched_2

# Example usage:
if __name__ == "__main__":
    # Install required library first: pip install rapidfuzz openpyxl
    
    # Replace with your file paths
    file1 = "/Users/kenechukwuchukwuka/Downloads/DLT/zainab bs.xlsx"
    file2 = "/Users/kenechukwuchukwuka/Downloads/DLT/zainab cb.xlsx"
    
    matched, unmatched1, unmatched2 = reconcile_sheets(
        file1_path=file1,
        file2_path=file2,
        sheet1_name=0,  # First sheet (or use sheet name as string)
        sheet2_name=0,
        amount_threshold=0.01,  # Allow 1 cent difference
        fuzzy_threshold=60  # 60% similarity minimum
    )
    
    print("\n=== RECONCILIATION COMPLETE ===")
    print(f"Matched transactions: {len(matched)}")
    print(f"Check 'reconciliation_results.xlsx' for detailed results")