#!/usr/bin/env python3
"""
KLAIRE Smart Procedure Classification System - CLEARLINE INTERNATIONAL LIMITED
Focuses on the most frequently used procedure codes from the last 12 months
"""

import duckdb
import pandas as pd
import openai
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta
import time
import json

# Load OpenAI API key
load_dotenv()

def get_openai_client():
    """Get OpenAI client with error handling"""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("❌ OpenAI API key not found. Please set OPENAI_API_KEY environment variable.")
        return None
    return openai.OpenAI(api_key=api_key)

def get_frequent_procedure_codes():
    """Get the most frequently used procedure codes from the last 12 months"""
    print("📊 Analyzing procedure usage from the last 12 months...")
    
    try:
        conn = duckdb.connect('ai_driven_data.duckdb')
        
        # Get procedure usage from both PA DATA and CLAIMS DATA for the last 12 months
        query = """
        WITH recent_procedures AS (
            -- From PA DATA (last 12 months)
            SELECT 
                code as procedurecode,
                COUNT(*) as usage_count,
                'PA_DATA' as source
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE requestdate >= CURRENT_DATE - INTERVAL 12 MONTH
            GROUP BY code
            
            UNION ALL
            
            -- From CLAIMS DATA (last 12 months) 
            SELECT 
                code as procedurecode,
                COUNT(*) as usage_count,
                'CLAIMS_DATA' as source
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE datesubmitted >= CURRENT_DATE - INTERVAL 12 MONTH
            GROUP BY code
        ),
        combined_usage AS (
            SELECT 
                procedurecode,
                SUM(usage_count) as total_usage,
                COUNT(DISTINCT source) as source_count
            FROM recent_procedures
            GROUP BY procedurecode
        )
        SELECT 
            procedurecode,
            total_usage,
            source_count
        FROM combined_usage
        ORDER BY total_usage DESC
        """
        
        usage_df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Found {len(usage_df):,} unique procedure codes used in the last 12 months")
        
        # Calculate 90th percentile threshold
        total_usage = usage_df['total_usage'].sum()
        cumulative_usage = 0
        threshold_90_percent = total_usage * 0.9
        
        top_90_percent_codes = []
        for _, row in usage_df.iterrows():
            cumulative_usage += row['total_usage']
            top_90_percent_codes.append(row['procedurecode'])
            if cumulative_usage >= threshold_90_percent:
                break
        
        print(f"📈 Top 90% usage covers {len(top_90_percent_codes):,} codes (out of {len(usage_df):,} total)")
        print(f"💡 This represents {cumulative_usage:,} total procedures ({cumulative_usage/total_usage*100:.1f}% of all usage)")
        
        return top_90_percent_codes, usage_df
        
    except Exception as e:
        print(f"❌ Error analyzing procedure usage: {e}")
        return None, None

def load_procedure_descriptions(procedure_codes):
    """Load procedure descriptions for the selected codes"""
    print("📋 Loading procedure descriptions for selected codes...")
    
    try:
        conn = duckdb.connect('ai_driven_data.duckdb')
        
        # Create a list of codes for the SQL IN clause
        codes_list = "', '".join(procedure_codes)
        
        query = f"""
        SELECT procedurecode, proceduredesc
        FROM "AI DRIVEN DATA"."PROCEDURE DATA"
        WHERE procedurecode IN ('{codes_list}')
        """
        
        procedures_df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Loaded descriptions for {len(procedures_df):,} procedures")
        return procedures_df
        
    except Exception as e:
        print(f"❌ Error loading procedure descriptions: {e}")
        return None

def get_medical_categories():
    """Define medical classification categories with keywords"""
    return {
        'ANALGESICS': ['pain', 'analgesic', 'paracetamol', 'acetaminophen', 'ibuprofen', 'aspirin', 'morphine', 'tramadol', 'diclofenac', 'ketorolac', 'codeine', 'oxycodone'],
        'ANTIBIOTICS': ['antibiotic', 'antibacterial', 'cefuroxime', 'amoxicillin', 'azithromycin', 'ciprofloxacin', 'metronidazole', 'doxycycline', 'clindamycin', 'vancomycin', 'ceftriaxone', 'gentamicin'],
        'ANTIMALARIALS': ['antimalarial', 'artemether', 'lumefantrine', 'chloroquine', 'quinine', 'primaquine', 'mefloquine', 'artesunate', 'malaria', 'falciparum'],
        'ANTIHYPERTENSIVES': ['antihypertensive', 'blood pressure', 'amlodipine', 'lisinopril', 'losartan', 'metoprolol', 'hydrochlorothiazide', 'atenolol', 'captopril', 'enalapril'],
        'ANTIDIABETICS': ['antidiabetic', 'diabetes', 'insulin', 'metformin', 'glibenclamide', 'gliclazide', 'pioglitazone', 'glipizide', 'sitagliptin'],
        'CARDIOVASCULAR': ['cardiac', 'heart', 'cardiovascular', 'nitroglycerin', 'digoxin', 'warfarin', 'aspirin cardiac', 'statin', 'atorvastatin', 'simvastatin'],
        'RESPIRATORY': ['respiratory', 'asthma', 'bronchodilator', 'salbutamol', 'inhaler', 'ventolin', 'theophylline', 'beclomethasone', 'budesonide'],
        'GASTROINTESTINAL': ['gastro', 'stomach', 'antacid', 'omeprazole', 'ranitidine', 'loperamide', 'metoclopramide', 'domperidone', 'pantoprazole'],
        'DERMATOLOGICAL': ['skin', 'dermatology', 'topical', 'cream', 'ointment', 'hydrocortisone', 'antifungal', 'clotrimazole', 'miconazole'],
        'OPHTHALMOLOGICAL': ['eye', 'ophthalmic', 'ophthalmology', 'cataract', 'glaucoma', 'eye drop', 'vision', 'retinal', 'corneal'],
        'GYNECOLOGICAL': ['gynecology', 'obstetric', 'pregnancy', 'contraceptive', 'hormone', 'estrogen', 'progesterone', 'mifepristone', 'misoprostol'],
        'PEDIATRIC': ['pediatric', 'child', 'infant', 'neonatal', 'pediatric dose', 'syrup', 'drops', 'suspension'],
        'LABORATORY': ['lab', 'laboratory', 'test', 'blood test', 'urine test', 'culture', 'biochemistry', 'hematology', 'microbiology', 'serology'],
        'RADIOLOGY': ['x-ray', 'radiology', 'scan', 'ultrasound', 'ct', 'mri', 'imaging', 'radiograph', 'mammography', 'angiography'],
        'SURGICAL': ['surgery', 'surgical', 'operation', 'procedure', 'incision', 'suture', 'anesthesia', 'laparoscopy', 'endoscopy'],
        'EMERGENCY': ['emergency', 'trauma', 'acute', 'urgent', 'resuscitation', 'cpr', 'ambulance', 'trauma care'],
        'NUTRITIONAL': ['nutrition', 'vitamin', 'supplement', 'feeding', 'tpn', 'dietary', 'multivitamin', 'folic acid', 'iron'],
        'PSYCHIATRIC': ['psychiatric', 'mental health', 'antidepressant', 'anxiolytic', 'psychotropic', 'fluoxetine', 'sertraline', 'diazepam'],
        'ONCOLOGICAL': ['cancer', 'oncology', 'chemotherapy', 'tumor', 'malignancy', 'neoplasm', 'carcinoma', 'sarcoma'],
        'UROLOGICAL': ['urology', 'urinary', 'kidney', 'bladder', 'prostate', 'nephrology', 'dialysis', 'catheter']
    }

def classify_with_keywords(procedure_desc, procedure_code, categories):
    """Classify procedure using keyword matching"""
    desc_lower = procedure_desc.lower()
    code_lower = procedure_code.lower()
    
    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword in desc_lower or keyword in code_lower:
                return category, f'Keyword match: {keyword}'
    
    return None, None

def classify_with_ai(procedure_desc, procedure_code, client, categories):
    """Classify procedure using AI"""
    try:
        category_list = ', '.join(categories.keys())
        
        prompt = f'''Classify this medical procedure into ONE of these categories: {category_list}

Procedure Code: {procedure_code}
Description: {procedure_desc}

Return ONLY the category name. No explanations.'''
        
        response = client.chat.completions.create(
            model='gpt-3.5-turbo',
            messages=[
                {'role': 'system', 'content': 'You are a medical classification expert. Return only the category name.'},
                {'role': 'user', 'content': prompt}
            ],
            max_tokens=20,
            temperature=0.1
        )
        
        result = response.choices[0].message.content.strip().upper()
        
        # Validate the result is a valid category
        if result in categories:
            return result, 'AI classification'
        else:
            return 'UNCLASSIFIED', f'Invalid AI response: {result}'
            
    except Exception as e:
        return 'UNCLASSIFIED', f'AI error: {str(e)[:50]}'

def process_procedures_batch(procedures_df, start_idx, batch_size, categories, client, use_ai=True):
    """Process a batch of procedures"""
    end_idx = min(start_idx + batch_size, len(procedures_df))
    batch = procedures_df.iloc[start_idx:end_idx]
    
    results = []
    
    for idx, row in batch.iterrows():
        procedure_code = str(row['procedurecode'])
        procedure_desc = str(row['proceduredesc'])
        
        # Try keyword classification first
        category, reason = classify_with_keywords(procedure_desc, procedure_code, categories)
        
        # If no keyword match and AI is enabled, use AI
        if not category and use_ai:
            category, reason = classify_with_ai(procedure_desc, procedure_code, client, categories)
        
        # If still no classification, mark as unclassified
        if not category:
            category, reason = 'UNCLASSIFIED', 'No keyword match and AI disabled'
        
        results.append({
            'procedurecode': procedure_code,
            'proceduredesc': procedure_desc,
            'category': category,
            'classification_reason': reason,
            'original_index': idx
        })
    
    return results

def main():
    """Main classification function"""
    print("🏥 KLAIRE SMART PROCEDURE CLASSIFICATION SYSTEM - CLEARLINE INTERNATIONAL LIMITED")
    print("=" * 70)
    print("🎯 Focus: Top 90% most frequently used procedures (last 12 months)")
    print()
    
    # Step 1: Get frequently used procedure codes
    frequent_codes, usage_df = get_frequent_procedure_codes()
    if frequent_codes is None:
        return
    
    # Step 2: Load procedure descriptions for selected codes
    procedures_df = load_procedure_descriptions(frequent_codes)
    if procedures_df is None:
        return
    
    # Get categories
    categories = get_medical_categories()
    
    # Get OpenAI client
    client = get_openai_client()
    use_ai = client is not None
    
    if not use_ai:
        print("⚠️  OpenAI not available - using keyword matching only")
    else:
        print("🤖 OpenAI available - using AI + keyword classification")
    
    print()
    
    # Process in small batches
    batch_size = 25  # Slightly larger batches since we have fewer procedures
    total_procedures = len(procedures_df)
    all_results = []
    
    print(f"📊 Processing {total_procedures:,} high-usage procedures in batches of {batch_size}")
    print("⏳ Estimated time: 1-2 minutes")
    print()
    
    start_time = time.time()
    
    for i in range(0, total_procedures, batch_size):
        batch_num = (i // batch_size) + 1
        total_batches = (total_procedures - 1) // batch_size + 1
        
        print(f"📦 Processing batch {batch_num}/{total_batches} (procedures {i+1}-{min(i+batch_size, total_procedures)})")
        
        batch_results = process_procedures_batch(
            procedures_df, i, batch_size, categories, client, use_ai
        )
        
        all_results.extend(batch_results)
        
        # Progress update
        progress = (i + batch_size) / total_procedures * 100
        elapsed = time.time() - start_time
        eta = (elapsed / (i + batch_size)) * (total_procedures - i - batch_size)
        
        print(f"   ✅ Completed {len(all_results):,} procedures ({progress:.1f}%) - ETA: {eta/60:.1f} min")
        
        # Small delay to avoid overwhelming the API
        if use_ai:
            time.sleep(0.3)
    
    # Create results DataFrame
    results_df = pd.DataFrame(all_results)
    
    elapsed_total = time.time() - start_time
    print()
    print(f"✅ Classification completed in {elapsed_total/60:.1f} minutes")
    print()
    
    # Show summary
    print("📊 CLASSIFICATION RESULTS SUMMARY:")
    print("-" * 50)
    
    category_counts = results_df['category'].value_counts()
    for category, count in category_counts.items():
        percentage = (count / len(results_df)) * 100
        print(f"{category:<20} {count:>6} procedures ({percentage:>5.1f}%)")
    
    print()
    print(f"✅ Total classified: {len(results_df):,} procedures")
    print(f"📈 These represent the top 90% most used procedures from the last 12 months")
    
    # Save to Excel
    output_file = f"SMART_PROCEDURE_CLASSIFICATION_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Main classification sheet
        results_df.to_excel(writer, sheet_name='Classification', index=False)
        
        # Summary sheet
        summary_df = pd.DataFrame([
            {'Category': cat, 'Count': count, 'Percentage': f"{(count/len(results_df)*100):.1f}%"}
            for cat, count in category_counts.items()
        ])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Usage statistics
        usage_summary = usage_df.head(len(frequent_codes)).copy()
        usage_summary['included_in_classification'] = usage_summary['procedurecode'].isin(frequent_codes)
        usage_summary.to_excel(writer, sheet_name='Usage_Stats', index=False)
        
        # Sample procedures for each category
        for category in category_counts.index[:10]:  # Top 10 categories
            category_data = results_df[results_df['category'] == category].head(20)
            sheet_name = category[:30]  # Excel sheet name limit
            category_data.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"💾 Results saved to: {output_file}")
    print()
    print("🎯 SMART CLASSIFICATION COMPLETE!")
    print("📋 Excel file contains:")
    print("   - Classification: High-usage procedures with categories")
    print("   - Summary: Category counts and percentages")
    print("   - Usage_Stats: Procedure usage frequency data")
    print("   - Individual sheets: Sample procedures for each category")
    print()
    print("🤖 KLAIRE has intelligently classified the most important procedures!")

if __name__ == "__main__":
    main()
