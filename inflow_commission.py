import os
import re
import pandas as pd
import streamlit as st
from datetime import datetime
from rapidfuzz import fuzz

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

import requests
import ast
import pyodbc
import toml

# --- BEGIN: commision.py LOGIC (flattened here directly to get zyra = df) ---

# Fetch data from the endpoint
url = "https://invoice-backend.clearlinehmo.com/api/v1/invoice/public?perPage=all"
response = requests.get(url)
response.raise_for_status()  # Raise an exception for HTTP errors

data = response.json()

# Normalize and find the invoice records
def coerce_invoices_list(obj):
    """Return a list of invoice dicts from various possible API shapes."""
    if isinstance(obj, list):
        if all(isinstance(x, dict) for x in obj):
            return obj
        extracted = []
        for x in obj:
            if isinstance(x, dict) and "invoices" in x and isinstance(x["invoices"], dict):
                extracted.append(x["invoices"])
        if extracted:
            return extracted
        return []
    if isinstance(obj, dict):
        for key in ("invoices", "data", "results"):
            if key in obj:
                sub = obj[key]
                if isinstance(sub, list) and all(isinstance(x, dict) for x in sub):
                    return sub
                if isinstance(sub, dict):
                    for inner_key in ("invoices", "data", "results"):
                        inner = sub.get(inner_key)
                        if isinstance(inner, list) and all(isinstance(x, dict) for x in inner):
                            return inner
        if "_id" in obj or "invoiceNumber" in obj:
            return [obj]
    return []

records = None
if isinstance(data, dict):
    if "data" in data:
        records = data["data"]
    elif "results" in data:
        records = data["results"]
    elif "invoices" in data:
        records = data["invoices"]
    else:
        records = data
else:
    records = data

invoices = coerce_invoices_list(records)

# Fallback for certain weird payloads
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

df_comm = pd.json_normalize(invoices, sep="_") if invoices else pd.DataFrame()

# Rename columns to map to desired output
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
    "totalLives": "totalLives",
    "totalAmount": "totalAmount",
    "discountAmount": "discountAmount"
}
df_comm = df_comm.rename(columns=rename_map)

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
    "discountAmount"
]

for c in desired_order:
    if c not in df_comm.columns:
        df_comm[c] = pd.NA

zyra = df_comm[desired_order]

# --- END: commision.py LOGIC ---

# --- BEGIN: group_contract LOGIC ---

def load_secrets():
    secrets_path = os.path.join(os.path.dirname(__file__), "secrets.toml")
    if os.path.exists(secrets_path):
        return toml.load(secrets_path)
    raise FileNotFoundError(f"Secrets file not found: {secrets_path}")


def get_sql_driver() -> str:
    drivers = [x for x in pyodbc.drivers()]
    preferred = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server'
    ]
    for d in preferred:
        if d in drivers:
            return d
    raise RuntimeError("No compatible SQL Server driver found. Install ODBC Driver 17/18 for SQL Server.")


def create_medicloud_connection():
    """Create connection to Medicloud database using secrets.toml (aligned with dlt_sources.py)."""
    secrets = load_secrets()
    driver = get_sql_driver()
    server = secrets['credentials']['server']
    port = secrets['credentials'].get('port')
    database = secrets['credentials']['database']
    username = secrets['credentials']['username']
    password = secrets['credentials']['password']

    server_part = f"{server},{port}" if port else server
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server_part};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def fetch_group_contract():
    """Fetch group_contract data from Medicloud"""
    conn = create_medicloud_connection()
    query = """
        SELECT 
        gc.groupid,
        gc.startdate,
        gc.enddate,
        g.groupname
        FROM dbo.group_contract gc
        JOIN dbo.[group] g ON gc.groupid = g.groupid
        WHERE gc.iscurrent = 1
        AND CAST(gc.enddate AS DATETIME) >= CAST(GETDATE() AS DATETIME);
    """
    df_group_contract = pd.read_sql(query, conn)
    conn.close()
    return df_group_contract

# Fetch group_contract data
try:
    df_group_contract = fetch_group_contract()
    print(f"Loaded {len(df_group_contract)} group contracts")
except Exception as e:
    print(f"Error loading group_contract: {e}")
    df_group_contract = pd.DataFrame()

# --- END: group_contract LOGIC ---



def get_gsheet_client():
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]

    # Expect a service account file named CREDENTIALS.json in the workspace
    creds_path = os.path.join(os.path.dirname(__file__), "CREDENTIALS.json")
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_zyra() -> pd.DataFrame:
    """Fetch latest invoices and build the zyra DataFrame fresh (for manual lookups)."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        # Reuse the existing coerce_invoices_list logic
        if isinstance(payload, dict):
            recs = (
                payload.get("invoices")
                or payload.get("data")
                or payload.get("results")
                or payload
            )
        else:
            recs = payload

        invs = coerce_invoices_list(recs)
        df_tmp = pd.json_normalize(invs, sep="_") if invs else pd.DataFrame()
        df_tmp = df_tmp.rename(columns=rename_map)

        for c in desired_order:
            if c not in df_tmp.columns:
                df_tmp[c] = pd.NA

        return df_tmp[desired_order]
    except Exception:
        # Fallback to existing zyra if refresh fails
        return zyra.copy()


@st.cache_data(ttl=60, show_spinner=False)
def load_dump_df(spreadsheet_name: str, worksheet_name: str) -> pd.DataFrame:
    client = get_gsheet_client()
    sh = client.open(spreadsheet_name)
    ws = sh.worksheet(worksheet_name)
    rows = ws.get_all_records()
    df_dump = pd.DataFrame(rows)
    # Safely normalize column names
    df_dump.columns = [str(col).strip() for col in df_dump.columns]
    return df_dump, ws

@st.cache_data(ttl=300, show_spinner=False)
def load_commission_sheet(spreadsheet_name: str) -> pd.DataFrame:
    """Load COMMISION sheet data"""
    client = get_gsheet_client()
    sh = client.open(spreadsheet_name)
    try:
        ws = sh.worksheet("COMMISION")
        rows = ws.get_all_records()
        df_commission = pd.DataFrame(rows)
        # Safely strip column names
        df_commission.columns = [str(col).strip() for col in df_commission.columns]
        # Convert CLIENT column to string to avoid .str accessor errors
        if "CLIENT" in df_commission.columns:
            df_commission["CLIENT"] = df_commission["CLIENT"].astype(str)
        return df_commission, ws
    except Exception as e:
        st.error(f"Error loading COMMISION sheet: {e}")
        return pd.DataFrame(), None

@st.cache_data(ttl=300, show_spinner=False)
def load_contact_person_sheet(spreadsheet_name: str) -> pd.DataFrame:
    """Load CONTACT PERSON sheet data"""
    client = get_gsheet_client()
    sh = client.open(spreadsheet_name)
    try:
        ws = sh.worksheet("CONTACT PERSON")
        rows = ws.get_all_records()
        df_contact = pd.DataFrame(rows)
        # Safely strip column names
        df_contact.columns = [str(col).strip() for col in df_contact.columns]
        # Convert all columns to string to avoid .str accessor errors
        for col in df_contact.columns:
            df_contact[col] = df_contact[col].astype(str)
        return df_contact, ws
    except Exception as e:
        st.error(f"Error loading CONTACT PERSON sheet: {e}")
        return pd.DataFrame(), None

def process_commission_data(groupname: str, client_company: str, transaction_amount: float, invoice_type: str, 
                          df_commission: pd.DataFrame, contact_ws, spreadsheet_name: str):
    """Process commission data and add to CONTACT PERSON sheet"""
    
    # Try to find matching group in COMMISION sheet first by groupname
    if not df_commission.empty and "CLIENT" in df_commission.columns:
        matching_rows = df_commission[df_commission["CLIENT"].str.upper() == groupname.upper()]
        
        # If no match found, try clientCompany as fallback
        if matching_rows.empty and client_company:
            matching_rows = df_commission[df_commission["CLIENT"].str.upper() == client_company.upper()]
            if not matching_rows.empty:
                st.info(f"⚠️ Group '{groupname}' not found, using clientCompany '{client_company}' instead")
    else:
        matching_rows = pd.DataFrame()
    
    if matching_rows.empty:
        st.warning(f"⚠️ Neither group '{groupname}' nor clientCompany '{client_company}' found in COMMISION sheet. Skipping commission processing.")
        return True  # Return True to continue reconciliation without commission
    
    # Take first match
    commission_row = matching_rows.iloc[0]

    # Helper to fetch column values with flexible header names
    def get_row_value(row: pd.Series, candidates: list, required: bool = True):
        header_map = {str(col).strip().upper(): col for col in row.index}
        for cand in candidates:
            key = cand.strip().upper()
            if key in header_map:
                return row[header_map[key]]
        if required:
            raise KeyError(
                f"Required column not found. Tried: {candidates}. Available: {list(header_map.keys())}"
            )
        return None

    # Extract commission data with robust header handling
    contact_person = get_row_value(
        commission_row,
        ["CONTACT PERSON", "CONTACT_PERSON", "CONTACTPERSON"],
    )
    percentage_raw = get_row_value(
        commission_row,
        ["%CONTACT PEROSN", "%CONTACT PERSON", "PERCENTAGE", "% CONTACT PERSON", "PERCENT"],
    )
    contact_bank = get_row_value(
        commission_row,
        ["CONTACT BANK", "CONTACT_BANK", "BANK", "BANK NAME"],
    )
    account_no = get_row_value(
        commission_row,
        ["ACCOUNT NO.", "ACCOUNT NO", "ACCOUNT NUMBER", "ACCOUNT_NUMBER", "ACCOUNT"],
    )

    # Coerce percentage to float (strip possible % and commas)
    try:
        contact_percentage = float(str(percentage_raw).replace("%", "").replace(",", "").strip())
    except Exception:
        st.error(f"Could not parse percentage value: {percentage_raw}")
        return False
    
    # Calculate fields
    premium = transaction_amount
    comm = contact_percentage * premium / 100
    wht = comm * 0.02  # 2% of COMM
    net_paid = comm - wht
    
    # Initialize client services and sales
    client_services = ""
    sales = ""
    
    # Check invoice type for client services and sales
    inv_type_norm = str(invoice_type or "").strip().upper()
    if "INSTALL" in inv_type_norm:  # robust match for INSTALLMENT variants
        client_services = round(net_paid * 0.25, 2)  # 25% of NET PAID
        sales = round(net_paid * 0.75, 2)  # 75% of NET PAID
    
    # Use the actual matched name for COMPANY NAME
    if not df_commission.empty and "CLIENT" in df_commission.columns:
        matched_name = groupname if df_commission[df_commission["CLIENT"].str.upper() == groupname.upper()].shape[0] > 0 else client_company
    else:
        matched_name = groupname
    
    # Prepare new row data
    new_row = {
        "COMPANY NAME": matched_name,
        "CONTACT PERSON": contact_person,
        "PERCENTAGE": contact_percentage,
        "CONTACT BANK": contact_bank,
        "ACCOUNT NO": account_no,
        "PREMIUM": premium,
        "COMM": comm,
        "WHT": wht,
        "NET PAID": net_paid,
        "CLIENT SERVICES": client_services,
        "SALES": sales
    }
    
    # Add to CONTACT PERSON sheet
    try:
        # Get current data to find next row
        current_data = contact_ws.get_all_records()
        next_row = len(current_data) + 2  # +2 because of header row and 0-based indexing
        
        # Update each column
        for col_idx, (col_name, value) in enumerate(new_row.items(), start=1):
            contact_ws.update_cell(next_row, col_idx, value)
        
        st.success(f"✅ Commission data added to CONTACT PERSON sheet for {groupname}")
        return True
        
    except Exception as e:
        st.error(f"Error updating CONTACT PERSON sheet: {e}")
        return False


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")


def normalize_company_text(text: str) -> str:
    """Normalize names by uppercasing, removing punctuation, expanding common abbreviations,
    and stripping corporate suffixes so variants like 'NIG.' vs 'NIGERIA' still match.
    """
    if not isinstance(text, str):
        return ""
    t = text.upper()
    # Unify connectors
    t = t.replace("&", " AND ")
    # Remove punctuation and brackets
    t = re.sub(r"[\.,:()\-]", " ", t)
    # Expand common abbreviations
    replacements = {
        " NIG ": " NIGERIA ",
        " NIG ": " NIGERIA ",
        " NIGER ": " NIGERIA ",
        " ENGR ": " ENGINEERING ",
        " CONSTR ": " CONSTRUCTION ",
        " CO ": " COMPANY ",
        " LTD ": " LIMITED ",
        " PLC ": " PLC ",
    }
    # Pad with spaces for whole-word replacement
    t = f" {t} "
    for k, v in replacements.items():
        t = t.replace(k, f" {v.strip()} ")
    # Remove frequent corporate stopwords
    stopwords = {
        "LIMITED", "LTD", "PLC", "COMPANY", "COMP", "NIGERIA", "NIG",
        "ENGINEERING", "ENGR", "CONSTRUCTION", "CONSTR", "AND"
    }
    tokens = [tok for tok in t.split() if tok and tok not in stopwords]
    return " ".join(tokens)


def find_recommendations(
    dump_row: pd.Series,
    invoices: pd.DataFrame,
    amount_tolerance: float = 100.0,
    fuzzy_threshold: int = 60,
) -> pd.DataFrame:
    # Amount comparison: CREDIT vs totalAmount within tolerance
    credit = dump_row.get("CREDIT")
    credit_num = coerce_numeric(pd.Series([credit])).iloc[0]

    inv = invoices.copy()
    inv["totalAmount_num"] = coerce_numeric(inv.get("totalAmount", pd.Series(index=inv.index)))

    # Filter by status first (Expired or Pending, case insensitive)
    status_mask = inv["status"].astype(str).str.upper().isin(["EXPIRED", "PENDING"])
    inv = inv[status_mask].copy()
    
    if inv.empty:
        return pd.DataFrame()

    # Filter by amount window
    mask_amount = inv["totalAmount_num"].between(credit_num - amount_tolerance, credit_num + amount_tolerance)
    candidates = inv[mask_amount].copy()
    
    if candidates.empty:
        return pd.DataFrame()

    # Company name containment + fuzzy score with normalization and token-set ratio
    raw_details = str(dump_row.get("DETAILS", ""))
    details_upper = raw_details.upper()
    norm_details = normalize_company_text(raw_details)

    def compute_scores(name: str) -> tuple:
        if not isinstance(name, str) or not name.strip():
            return 0, 0, 0
        company_upper = name.upper().strip()
        norm_company = normalize_company_text(name)
        containment = 1 if company_upper in details_upper else 0
        # Partial ratio on raw strings
        pr = fuzz.partial_ratio(company_upper, details_upper)
        # Token-set ratio on normalized strings (handles word order + noise)
        ts = fuzz.token_set_ratio(norm_company, norm_details)
        # Use the stronger of pr and ts as the fuzzy score
        fuzzy_score = max(pr, ts)
        return containment, fuzzy_score, ts

    scores = candidates["clientCompany"].apply(compute_scores)
    candidates["company_match"], candidates["fuzzy_score"], candidates["token_set_score"] = zip(*scores)
    
    # Both amount AND name matching must pass
    candidates = candidates[candidates["fuzzy_score"] >= fuzzy_threshold]
    
    if candidates.empty:
        return pd.DataFrame()
    
    candidates = candidates.sort_values(["company_match", "fuzzy_score", "totalAmount_num"], ascending=[False, False, True])

    # Columns to return as recommendations
    cols = [
        "invoicenumber",
        "clientCompany",
        "totalAmount",
        "invoiceStartDate",
        "invoiceEndDate",
        "status",
    ]
    for c in cols:
        if c not in candidates.columns:
            candidates[c] = pd.NA
    return candidates[cols + ["totalAmount_num", "company_match", "fuzzy_score", "token_set_score"]]


def main():
    st.set_page_config(page_title="Bank Inflow Reconciliation", layout="wide")

    # Theming via custom CSS (blue/green corporate palette)
    st.markdown(
        """
        <style>
        :root {
          --primary: #0A64A4; /* blue */
          --accent: #1FA97A;  /* green */
        }
        .stApp { background: #f7fbff; }
        h1, .css-10trblm, .stMarkdown h1 { color: var(--primary) !important; }
        .recommend-card { border-left: 4px solid var(--primary); background: #ffffff; padding: 12px 16px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.06); }
        .success { color: var(--accent); }
        .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #e6f7f0; color: var(--accent); font-size: 12px; margin-left: 8px; }
        .expander-header { color: #0b3954; font-weight: 600; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<h1>Bank Inflow Reconciliation</h1>", unsafe_allow_html=True)

    st.sidebar.header("Data Source")
    sheet_name = st.sidebar.text_input("Spreadsheet name", value="INFLOW NEW")
    worksheet = st.sidebar.text_input("Worksheet name", value="DUMP")
    amount_tolerance = st.sidebar.number_input("Amount tolerance (+/-)", value=100.0, step=10.0)
    fuzzy_threshold = st.sidebar.slider("Fuzzy threshold", min_value=0, max_value=100, value=60, step=1)

    # Load data and store in session state to persist across reruns
    if "data_loaded" not in st.session_state:
        st.session_state.data_loaded = False
    
    if st.sidebar.button("Load data") or st.session_state.data_loaded:
        if not st.session_state.data_loaded:
            dump_df, ws = load_dump_df(sheet_name, worksheet)
            
            # Load commission and contact person sheets
            df_commission, commission_ws = load_commission_sheet(sheet_name)
            df_contact, contact_ws = load_contact_person_sheet(sheet_name)
            
            # Store in session state
            st.session_state.dump_df = dump_df
            st.session_state.ws = ws
            st.session_state.df_commission = df_commission
            st.session_state.commission_ws = commission_ws
            st.session_state.df_contact = df_contact
            st.session_state.contact_ws = contact_ws
            st.session_state.data_loaded = True
        else:
            # Use stored data
            dump_df = st.session_state.dump_df
            ws = st.session_state.ws
            df_commission = st.session_state.df_commission
            commission_ws = st.session_state.commission_ws
            df_contact = st.session_state.df_contact
            contact_ws = st.session_state.contact_ws

        # Expect columns: ENTRY DATE, DETAILS, VALUE DATE, DEBIT, CREDIT, BALANCE, INVOICE
        required_cols = ["ENTRY DATE", "DETAILS", "VALUE DATE", "DEBIT", "CREDIT", "BALANCE", "INVOICE"]
        missing = [c for c in required_cols if c not in dump_df.columns]
        if missing:
            st.error(f"Missing columns in DUMP: {missing}")
            return

        # Filter unreconciled rows (INVOICE empty)
        unreconciled = dump_df[dump_df["INVOICE"].astype(str).str.strip() == ""].copy()
        st.write(f"Unreconciled rows: {len(unreconciled)}")

        if unreconciled.empty:
            st.success("All rows are already reconciled.")
            return

        # Show a table with recommendations per row
        for idx, row in unreconciled.iterrows():
            credit_amount = f"{float(row['CREDIT']):,.2f}" if pd.notna(row['CREDIT']) else "N/A"
            with st.expander(f"Row {idx} | VALUE DATE: {row['VALUE DATE']} | CREDIT: ₦{credit_amount} | DETAILS: {row['DETAILS'][:80]}"):
                recs = find_recommendations(row, zyra, amount_tolerance, fuzzy_threshold)
                
                # Prepare selection variables
                invoice_number = None
                invoice_status = None
                invoice_amount = None
                selected = None

                # Option 1: Select from recommendations (if available)
                if not recs.empty:
                    st.subheader("Recommendations")
                    st.markdown("<div class='recommend-card'>Top matches based on amount and company name</div>", unsafe_allow_html=True)
                    st.dataframe(recs.reset_index(drop=True), use_container_width=True)

                    choice = st.selectbox(
                        "Select a recommendation (by row number)",
                        options=[None] + list(range(len(recs))),
                        format_func=lambda x: "— Select —" if x is None else str(x),
                        index=0,
                        key=f"choice_{idx}",
                    )

                    if choice is not None:
                        selected = recs.iloc[int(choice)]
                        st.write(
                            f"Selected invoice: {selected['invoicenumber']} | Company: {selected['clientCompany']} | Amount: {selected['totalAmount']} | Status: {selected['status']}"
                        )
                        invoice_number = str(selected["invoicenumber"]).strip()
                        invoice_status = str(selected["status"]).upper()
                        invoice_amount = selected["totalAmount_num"]
                else:
                    st.info("No recommendations found for this row.")

                # Option 2: Manual invoice input (always available)
                st.subheader("Manual Invoice Input")
                manual_invoice = st.text_input("Enter invoice number manually (optional)", key=f"manual_{idx}")

                if manual_invoice:
                    # Refresh zyra live to avoid stale statuses
                    zyra_live = fetch_latest_zyra()
                    matching_invoices = zyra_live[zyra_live["invoicenumber"].astype(str) == manual_invoice]
                    if not matching_invoices.empty:
                        selected_manual = matching_invoices.iloc[0]
                        manual_status = str(selected_manual["status"]).upper()
                        if manual_status not in ["PENDING", "EXPIRED"]:
                            st.warning(f"⚠️ Invoice status is '{manual_status}'. Only PENDING or EXPIRED invoices can be reconciled.")
                            # Do not override selection if manual invalid
                        else:
                            # Override selection with manual choice
                            selected = selected_manual
                            invoice_number = str(selected_manual["invoicenumber"]).strip()
                            invoice_status = manual_status
                            invoice_amount = coerce_numeric(pd.Series([selected_manual["totalAmount"]])).iloc[0]
                            st.write(f"Using manual invoice: {invoice_number} | Company: {selected_manual['clientCompany']} | Amount: {selected_manual['totalAmount']} | Status: {selected_manual['status']}")
                    else:
                        st.error("Invoice number not found in system.")
                        # keep previous selection if any
                
                if not invoice_number:
                    # Nothing selected yet; skip to next row
                    continue

                # Process the selected/manual invoice
                if invoice_number:
                    credit_num = coerce_numeric(pd.Series([row["CREDIT"]])).iloc[0]
                    equal_amount = pd.notna(invoice_amount) and float(invoice_amount) == float(credit_num)
                    
                    # Check status and amount rules
                    if invoice_status == "EXPIRED":
                        st.warning("⚠️ Selected invoice is EXPIRED. Please activate the invoice before reconciling.")
                    elif invoice_status == "PENDING" and not equal_amount:
                        st.warning("⚠️ Invoice status is PENDING but amount does not equal CREDIT. Please correct the invoice amount before reconciling.")
                    elif invoice_status == "PENDING" and equal_amount:
                        # Both criteria met - show group selection
                        st.success("✅ Invoice status is PENDING and amount matches. Please select group for reconciliation.")
                        
                        # Get unique group names from df_group_contract
                        if not df_group_contract.empty:
                            unique_groups = df_group_contract["groupname"].unique().tolist()
                            selected_group = st.selectbox(
                                "Select Group Name:",
                                options=unique_groups,
                                key=f"group_{idx}",
                            )
                            
                            if st.button("Complete Reconciliation", key=f"reconcile_{idx}"):
                                # Get transaction amount and invoice type
                                transaction_amount = coerce_numeric(pd.Series([row["CREDIT"]])).iloc[0]
                                invoice_type = str(selected["invoiceType"]) if "invoiceType" in selected else ""
                                
                                # Process commission data
                                commission_success = process_commission_data(
                                    selected_group, 
                                    str(selected["clientCompany"]),  # Pass clientCompany as fallback
                                    transaction_amount, 
                                    invoice_type, 
                                    df_commission, 
                                    contact_ws, 
                                    sheet_name
                                )
                                
                                if commission_success:
                                    # Write back to Google Sheet: set INVOICE cell with invoice number
                                    gsheet_row = idx + 2
                                    ws.update_acell(f"G{gsheet_row}", invoice_number)  # Column G is INVOICE
                                    # Update in-memory data so we don't force a full reload
                                    try:
                                        st.session_state.dump_df.loc[idx, "INVOICE"] = invoice_number
                                    except Exception:
                                        pass
                                    st.success(f"✅ Row {idx} reconciled with invoice {invoice_number} and group {selected_group}.")
                                    # Soft refresh: re-run without clearing loaded data
                                    st.rerun()
                                else:
                                    st.error("❌ Failed to process commission data. Reconciliation not completed.")
                        else:
                            st.error("No group contracts available. Cannot complete reconciliation.")
                    else:
                        st.info(f"Invoice status: {invoice_status}. Amount match: {equal_amount}")


if __name__ == "__main__":
    main()


