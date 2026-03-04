# Hospital Band Analysis - Streamlit App

This Streamlit application analyzes hospital pricing bands based on claims data from MediCloud and uploaded tariff files.

## Features

- 🔄 **Real-time Data**: Fetches claims data directly from MediCloud for the past 12 months
- 📁 **File Upload**: Upload standard tariff and hospital tariff CSV files
- 📊 **Interactive Analysis**: View results directly in the browser
- 📥 **Download Reports**: Export results as Excel files
- 🏥 **Hospital Details**: Drill down into individual hospital pricing

## Setup

1. Install dependencies:
```bash
pip install -r requirements_streamlit.txt
```

2. Ensure your `secrets.toml` file is configured with MediCloud credentials

3. Run the Streamlit app:
```bash
streamlit run band_streamlit.py
```

## File Formats

### Standard Tariff File (standard_tariff.csv)
Required columns:
- `procedurecode`: Procedure code
- `band_a`: Band A price
- `band_b`: Band B price  
- `band_c`: Band C price
- `band_d`: Band D price
- `band_special`: Special band price

### Hospital Tariff File (hospital_tariff.csv)
Required columns:
- `procedurecode`: Procedure code
- `proceduredesc`: Procedure description
- `tariffamount`: Price for this hospital
- `tariffname`: Hospital name

## Usage

1. Open the Streamlit app in your browser
2. Click "Fetch Claims Data from MediCloud" to load recent claims
3. Upload your standard tariff CSV file
4. Upload your hospital tariff CSV file
5. Click "Run Analysis" to process the data
6. View results and download Excel reports as needed

## Analysis Process

1. **Claims Processing**: Calculates procedure frequency from MediCloud claims data
2. **Band Assignment**: Assigns each hospital procedure to appropriate pricing bands
3. **Weighted Analysis**: Calculates frequency-weighted average prices
4. **Ranking**: Ranks hospitals based on pricing quartiles
5. **Reporting**: Generates comprehensive analysis reports
