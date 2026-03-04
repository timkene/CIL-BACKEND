import requests
import pandas as pd
import ast

url = "https://invoice-backend.clearlinehmo.com/api/v1/invoice/public?perPage=all"

# Fetch data from the endpoint
response = requests.get(url)
response.raise_for_status()  # Raise an exception for HTTP errors

data = response.json()

# Check the data structure and extract the relevant part for DataFrame creation
if isinstance(data, dict):
    if "data" in data:
        records = data["data"]
    elif "results" in data:
        records = data["results"]
    elif "invoices" in data:
        # Some payloads wrap the list of invoice dicts under the key 'invoices'
        records = data["invoices"]
    else:
        records = data
else:
    records = data

def coerce_invoices_list(obj):
    """Return a list of invoice dicts from various possible API shapes."""
    # Case A: already a list of dicts
    if isinstance(obj, list):
        if all(isinstance(x, dict) for x in obj):
            return obj
        # List of rows that each contain an 'invoices' field
        extracted = []
        for x in obj:
            if isinstance(x, dict) and "invoices" in x and isinstance(x["invoices"], dict):
                extracted.append(x["invoices"])
        if extracted:
            return extracted
        return []

    # Case B: dict that directly holds a list
    if isinstance(obj, dict):
        # Common wrappers
        for key in ("invoices", "data", "results"):
            if key in obj:
                sub = obj[key]
                if isinstance(sub, list) and all(isinstance(x, dict) for x in sub):
                    return sub
                if isinstance(sub, dict):
                    # Sometimes invoices are nested one more level
                    for inner_key in ("invoices", "data", "results"):
                        inner = sub.get(inner_key)
                        if isinstance(inner, list) and all(isinstance(x, dict) for x in inner):
                            return inner
        # A single invoice dict
        if "_id" in obj or "invoiceNumber" in obj:
            return [obj]
    return []

# Normalize into a DataFrame of invoices
invoices = coerce_invoices_list(records)

# Fallback: when we get a DataFrame with an 'invoices' column where each row is a dict/string
if not invoices and isinstance(records, list):
    try:
        temp_df = pd.DataFrame(records)
        if "invoices" in temp_df.columns:
            parsed_invoices = temp_df["invoices"].apply(
                lambda x: (
                    x
                    if isinstance(x, dict)
                    else (ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("{") else {})
                )
            )
            invoices = list(parsed_invoices)
    except Exception:
        pass

df = pd.json_normalize(invoices, sep="_") if invoices else pd.DataFrame()

# Map source keys to the exact output column names requested
rename_map = {
    "_id": "id",
    "createdBy": "createdby",
    "name": "name",
    "email": "email",
    "userId": "userid",
    "status": "status",
    "invoiceType": "invoiceType",
    "invoiceNumber": "invoicenumber",
    "invoiceStartDate": "invoiceStartDate",
    "invoiceEndDate": "invoiceEndDate",
    "clientName": "clientName",
    "clientCompany": "clientCompany",
}

df = df.rename(columns=rename_map)

desired_order = [
    "id",
    "createdby",
    "name",
    "email",
    "userid",
    "status",
    "invoiceType",
    "invoicenumber",
    "invoiceStartDate",
    "invoiceEndDate",
    "clientName",
    "clientCompany",
    "totalLives",
    "totalAmount",
    "discountAmount",   
    "amount",
]

# Ensure all desired columns exist even if missing in some rows
for c in desired_order:
    if c not in df.columns:
        df[c] = pd.NA

# amount calculation removed per request

df = df[desired_order]

# Export to Excel file
excel_filename = "invoice_data.xlsx"
df.to_excel(excel_filename, index=False)
print(f"Data exported to {excel_filename}")
