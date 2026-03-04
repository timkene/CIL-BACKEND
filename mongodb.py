from pymongo import MongoClient
import pandas as pd
import duckdb
from typing import Dict, Optional
from pathlib import Path
import tempfile
import os


def get_mongo_client():
    """Create and return MongoDB client connection"""
    return MongoClient(
    host='176.58.106.196',
    port=17018,
    username='kene',
    password='WY00zEH4CJoaUqiw9g',
    authSource='admin'
)


def fetch_collections_as_dataframes(
    collections: Optional[list] = None,
    database: str = 'admin',
    client: Optional[MongoClient] = None
) -> Dict[str, pd.DataFrame]:
    """
    Fetch specified MongoDB collections and convert them to pandas DataFrames.
    
    Args:
        collections: List of collection names to fetch. If None, fetches all specified collections.
        database: Database name (default: 'admin')
        client: MongoDB client instance. If None, creates a new connection.
    
    Returns:
        Dictionary with collection names as keys and pandas DataFrames as values.
    """
    # Default collections to fetch
    if collections is None:
        collections = ['nhisprocedures', 'nhiaenrollees', 'requests', 'medicaldiagnoses']
    
    # Use provided client or create new one
    close_client = False
    if client is None:
        client = get_mongo_client()
        close_client = True
    
    try:
        db = client[database]
        dataframes = {}
        
        for coll_name in collections:
            print(f"Fetching {coll_name}...")
        coll = db[coll_name]

            # Get all documents from collection
            cursor = coll.find({})
            
            # Convert to list and then to DataFrame
            documents = list(cursor)
            
            if documents:
                df = pd.DataFrame(documents)
                # Remove MongoDB _id column if present (optional)
                if '_id' in df.columns:
                    df = df.drop('_id', axis=1)
                dataframes[coll_name] = df
                print(f"  ✓ {coll_name}: {len(df):,} rows, {len(df.columns)} columns")
            else:
                # Create empty DataFrame with proper structure
                dataframes[coll_name] = pd.DataFrame()
                print(f"  ⚠ {coll_name}: Empty collection")
        
        return dataframes
    
    finally:
        if close_client:
            client.close()


def get_requests_with_populated_procedures(
    database: str = 'admin',
    client: Optional[MongoClient] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Fetch requests with populated procedures from nhisprocedures and PrivateProcedures collections.
    Also includes populated issuer (issuerInfo) and provider (providerInfo) information.
    This performs the aggregation pipeline equivalent to the JavaScript version.
    
    Args:
        database: Database name (default: 'admin')
        client: MongoDB client instance. If None, creates a new connection.
        limit: Limit the number of results (for testing/review). If None, returns all.
    
    Returns:
        pandas DataFrame with requests, populated procedure data, issuerInfo, and providerInfo.
        The DataFrame is flattened with extracted fields from enrollee, procedureList, and paCode,
        while preserving issuerInfo and providerInfo columns.
    """
    # Use provided client or create new one
    close_client = False
    if client is None:
        client = get_mongo_client()
        close_client = True
    
    try:
        db = client[database]
        
        # Build aggregation pipeline
        pipeline = [
            {"$unwind": {"path": "$procedureList", "preserveNullAndEmptyArrays": True}},
            
            # Lookup issuer information
            {
                "$lookup": {
                    "from": "users",
                    "localField": "issuer",
                    "foreignField": "_id",
                    "as": "issuerData",
                }
            },
            {
                "$addFields": {
                    "issuerInfo": {"$arrayElemAt": ["$issuerData", 0]}
                }
            },
            
            # Lookup provider information (hospital and pharmacy)
            {
                "$lookup": {
                    "from": "hospitalproviders",
                    "localField": "provider",
                    "foreignField": "_id",
                    "as": "hospitalProviderData",
                }
            },
            {
                "$lookup": {
                    "from": "pharmacyproviders",
                    "localField": "provider",
                    "foreignField": "_id",
                    "as": "pharmacyProviderData",
                }
            },
            {
                "$addFields": {
                    "providerInfo": {
                        "$ifNull": [
                            {
                                "$arrayElemAt": [
                                    {"$concatArrays": ["$hospitalProviderData", "$pharmacyProviderData"]},
                                    0,
                                ]
                            },
                            {},
                        ]
                    }
                }
            },
            
            # Lookup procedure information
            {
                "$lookup": {
                    "from": "nhisprocedures",
                    "localField": "procedureList.procedure",
                    "foreignField": "_id",
                    "as": "nhisData"
                }
            },
            {
                "$lookup": {
                    "from": "PrivateProcedures",
                    "localField": "procedureList.procedure",
                    "foreignField": "_id",
                    "as": "privateData"
                }
            },
            {
                "$addFields": {
                    "procedureList.populatedProcedure": {
                        "$ifNull": [
                            {
                                "$arrayElemAt": [
                                    {"$concatArrays": ["$nhisData", "$privateData"]},
                                    0,
                                ]
                            },
                            {},
                        ]
                    }
                }
            },
            {
                "$project": {
                    "nhisData": 0,
                    "privateData": 0,
                    "hospitalProviderData": 0,
                    "pharmacyProviderData": 0,
                    "issuerData": 0,
                    "issuerInfo.password": 0,
                    "issuerInfo.__v": 0
                }
            }
        ]
        
        # Add limit if specified
        if limit:
            pipeline.append({"$limit": limit})
        
        print(f"Running aggregation pipeline on 'requests' collection...")
        
        try:
            cursor = db["requests"].aggregate(pipeline)
            # Convert to list and then to DataFrame
            documents = list(cursor)
        except Exception as e:
            print(f"  ⚠ Error during aggregation: {e}")
            # If PrivateProcedures doesn't exist, try without it
            if "PrivateProcedures" in str(e) or "namespace not found" in str(e).lower():
                print("  Attempting aggregation without PrivateProcedures lookup...")
                # Simplified pipeline without PrivateProcedures
                simplified_pipeline = [
                    {"$unwind": {"path": "$procedureList", "preserveNullAndEmptyArrays": True}},
                    
                    # Lookup issuer information
                    {
                        "$lookup": {
                            "from": "users",
                            "localField": "issuer",
                            "foreignField": "_id",
                            "as": "issuerData",
                        }
                    },
                    {
                        "$addFields": {
                            "issuerInfo": {"$arrayElemAt": ["$issuerData", 0]}
                        }
                    },
                    
                    # Lookup provider information (hospital and pharmacy)
                    {
                        "$lookup": {
                            "from": "hospitalproviders",
                            "localField": "provider",
                            "foreignField": "_id",
                            "as": "hospitalProviderData",
                        }
                    },
                    {
                        "$lookup": {
                            "from": "pharmacyproviders",
                            "localField": "provider",
                            "foreignField": "_id",
                            "as": "pharmacyProviderData",
                        }
                    },
                    {
                        "$addFields": {
                            "providerInfo": {
                                "$ifNull": [
                                    {
                                        "$arrayElemAt": [
                                            {"$concatArrays": ["$hospitalProviderData", "$pharmacyProviderData"]},
                                            0,
                                        ]
                                    },
                                    {},
                                ]
                            }
                        }
                    },
                    
                    # Lookup procedure information
                    {
                        "$lookup": {
                            "from": "nhisprocedures",
                            "localField": "procedureList.procedure",
                            "foreignField": "_id",
                            "as": "nhisData"
                        }
                    },
                    {
                        "$addFields": {
                            "procedureList.populatedProcedure": {
                                "$ifNull": [
                                    {
                                        "$arrayElemAt": ["$nhisData", 0]
                                    },
                                    {},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "nhisData": 0,
                            "hospitalProviderData": 0,
                            "pharmacyProviderData": 0,
                            "issuerData": 0,
                            "issuerInfo.password": 0,
                            "issuerInfo.__v": 0
                        }
                    }
                ]
                if limit:
                    simplified_pipeline.append({"$limit": limit})
                cursor = db["requests"].aggregate(simplified_pipeline)
                documents = list(cursor)
            else:
                raise
        
        if documents:
            df = pd.DataFrame(documents)
            # Remove MongoDB _id column if present (optional)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            print(f"  ✓ Aggregation result: {len(df):,} rows, {len(df.columns)} columns")
            
            # Flatten nested structures
            df = flatten_requests_dataframe(df)
            
            return df
        else:
            print("  ⚠ No results from aggregation")
            return pd.DataFrame()
    
    finally:
        if close_client:
            client.close()


def flatten_requests_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten nested structures in the requests DataFrame.
    Extracts data from enrollee, procedureList, and paCode columns.
    
    Args:
        df: DataFrame with nested structures
    
    Returns:
        Flattened DataFrame with extracted columns
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # 1. Extract enrollee data
    if 'enrollee' in df.columns:
        # Extract enrolleeId
        df['enrolleeId'] = df['enrollee'].apply(
            lambda x: x.get('enrolleeId') if isinstance(x, dict) else None
        )
        
        # Extract enrolleename
        df['enrolleename'] = df['enrollee'].apply(
            lambda x: x.get('enrolleeName') if isinstance(x, dict) else None
        )
        
        # Extract isPrincipal
        df['isPrincipal'] = df['enrollee'].apply(
            lambda x: x.get('isPrincipal') if isinstance(x, dict) else None
        )
        
        # Drop original enrollee column
        df = df.drop('enrollee', axis=1)
    
    # 2. Extract procedureList data
    if 'procedureList' in df.columns:
        # Extract procedureName from populatedProcedure
        df['procedureName'] = df['procedureList'].apply(
            lambda x: (
                x.get('populatedProcedure', {}).get('procedureName')
                if isinstance(x, dict)
                else None
            )
        )
        
        # Extract procedureCode from populatedProcedure
        df['procedureCode'] = df['procedureList'].apply(
            lambda x: (
                x.get('populatedProcedure', {}).get('procedureCode')
                if isinstance(x, dict)
                else None
            )
        )
        
        # Extract rate (from procedureList directly)
        df['rate'] = df['procedureList'].apply(
            lambda x: x.get('rate') if isinstance(x, dict) else None
        )
        
        # Extract diagnosis (directly from procedureList)
        df['diagnosis'] = df['procedureList'].apply(
            lambda x: x.get('diagnosis') if isinstance(x, dict) else None
        )
        
        # Extract diagnosisCode (directly from procedureList)
        df['diagnosisCode'] = df['procedureList'].apply(
            lambda x: x.get('diagnosisCode') if isinstance(x, dict) else None
        )
        
        # Extract quantity
        df['quantity'] = df['procedureList'].apply(
            lambda x: x.get('quantity') if isinstance(x, dict) else None
        )
        
        # Drop original procedureList column
        df = df.drop('procedureList', axis=1)
    
    # 3. Extract paCode data
    if 'paCode' in df.columns:
        # Store original paCode column temporarily
        pa_code_col = df['paCode'].copy()
        
        # Extract code from paCode
        df['code'] = pa_code_col.apply(
            lambda x: x.get('code') if isinstance(x, dict) else None
        )
        
        # Extract status from paCode
        # Note: This will overwrite the original request 'status' if it exists
        # since user specifically requested status from paCode
        df['status'] = pa_code_col.apply(
            lambda x: x.get('status') if isinstance(x, dict) else None
        )
        
        # Drop original paCode column
        df = df.drop('paCode', axis=1)
    
    # 4. Extract providerInfo data
    if 'providerInfo' in df.columns:
        # Extract providername from providerInfo.name
        df['providername'] = df['providerInfo'].apply(
            lambda x: x.get('name') if isinstance(x, dict) else None
        )
        
        # Extract providerKey from providerInfo.providerKey
        df['providerKey'] = df['providerInfo'].apply(
            lambda x: x.get('providerKey') if isinstance(x, dict) else None
        )
        
        # Extract genericCode from providerInfo.genericCode
        df['genericCode'] = df['providerInfo'].apply(
            lambda x: x.get('genericCode') if isinstance(x, dict) else None
        )
        
        # Extract primaryCode from providerInfo.primaryCode
        df['primaryCode'] = df['providerInfo'].apply(
            lambda x: x.get('primaryCode') if isinstance(x, dict) else None
        )
        
        # Drop original providerInfo column
        df = df.drop('providerInfo', axis=1)
    
    # 5. Remove unwanted columns
    columns_to_remove = [
        'updatedBy', 'updatedby',  # Handle both cases
        'issuer',
        'provider',
        'endorsmentType', 'endorsementType',  # Handle both spellings
        'admission',
        'issuerInfo',
        'providerType',
        '__v', '_V',  # Handle both cases
        'comment'
    ]
    
    # Remove columns that exist in the DataFrame
    columns_to_drop = [col for col in columns_to_remove if col in df.columns]
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop, axis=1)
    
    # 6. Select only the required columns in the specified order
    required_columns = [
        'enrolleeId', 'enrolleename', 'isPrincipal',
        'procedureName', 'procedureCode', 'rate', 'diagnosis', 'diagnosisCode', 'quantity',
        'code', 'status',  # code and status from paCode
        'providername', 'providerKey', 'genericCode', 'primaryCode',  # provider fields
        'encounterDate', 'processDate', 'createdAt'
    ]
    
    # Only include columns that exist in the DataFrame
    available_columns = [col for col in required_columns if col in df.columns]
    
    # Create final column list with required columns first, then any others
    other_columns = [col for col in df.columns if col not in required_columns]
    final_columns = available_columns + other_columns
    
    # Reorder DataFrame to match final_columns order
    df = df[final_columns]
    
    return df


def get_duckdb_connection(db_path: str = 'ai_driven_data.duckdb'):
    """
    Get or create DuckDB connection.
    
    Args:
        db_path: Path to DuckDB database file
    
    Returns:
        DuckDB connection object
    """
    # Convert to absolute path if relative
    if not Path(db_path).is_absolute():
        db_path = str(Path(db_path).resolve())
    
    conn = duckdb.connect(db_path)
    return conn


def create_nhia_schema(conn):
    """
    Create NHIA schema in DuckDB if it doesn't exist.
    
    Args:
        conn: DuckDB connection object
    """
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS NHIA")
        print("✓ NHIA schema created/verified")
    except Exception as e:
        print(f"⚠ Error creating NHIA schema: {e}")
        raise


def push_dataframe_to_duckdb(df: pd.DataFrame, table_name: str, conn, schema: str = 'NHIA', replace: bool = True):
    """
    Push a pandas DataFrame to DuckDB table in specified schema.
    
    Args:
        df: pandas DataFrame to push
        table_name: Name of the table in DuckDB
        conn: DuckDB connection object
        schema: Schema name (default: 'NHIA')
        replace: If True, replace existing table; if False, append
    """
    if df.empty:
        print(f"  ⚠ {table_name}: DataFrame is empty, skipping...")
        return
    
    try:
        # Clean DataFrame: replace empty strings with None to avoid type casting issues
        df_clean = df.copy()
        
        # Convert datetime columns to strings first
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                # Convert to string, handling NaT values
                df_clean[col] = df_clean[col].apply(lambda x: str(x) if pd.notna(x) else None)
        
        # Then handle object columns
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                # Replace various forms of empty/NaN values
                df_clean[col] = df_clean[col].replace(['', 'nan', 'NaN', 'None', 'N/A', 'NaT'], None)
                # Also handle pandas NaT (Not a Time) values
                df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
        
        # Create fully qualified table name
        full_table_name = f'"{schema}"."{table_name}"'
        
        # Ensure all datetime columns are converted to strings before registration
        # Convert entire DataFrame to ensure types are properly changed
        df_final = pd.DataFrame()
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                # Convert datetime to string
                converted = pd.to_datetime(df_clean[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                df_final[col] = converted.where(pd.notna(converted), None)
            else:
                df_final[col] = df_clean[col]
        
        # Force all columns to be object type to avoid any type inference issues
        for col in df_final.columns:
            df_final[col] = df_final[col].astype('object')
        
        if replace:
            # Drop table if exists
            conn.execute(f'DROP TABLE IF EXISTS {full_table_name}')
            
            # Use DuckDB's direct DataFrame insertion which handles type inference better
            # Register DataFrame
            temp_view_name = f'temp_{table_name}'
            conn.register(temp_view_name, df_final)
            
            try:
                # Create table - DuckDB will infer types, but we'll handle errors
                conn.execute(f'CREATE TABLE {full_table_name} AS SELECT * FROM {temp_view_name}')
                print(f"  ✓ {table_name}: {len(df_final):,} rows pushed (replaced)")
            except Exception as type_error:
                # If type inference fails, use CSV as intermediate format
                print(f"  ⚠ Type inference issue, using CSV fallback...")
                conn.execute(f'DROP TABLE IF EXISTS {full_table_name}')
                
                # Write DataFrame to temporary CSV
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
                    csv_path = tmp_file.name
                    # Write CSV with all columns as strings
                    df_final.to_csv(csv_path, index=False, na_rep='')
                
                try:
                    # Read CSV into DuckDB (DuckDB will handle type inference from CSV)
                    conn.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
                    print(f"  ✓ {table_name}: {len(df_final):,} rows pushed (replaced, via CSV)")
                finally:
                    # Clean up temporary file
                    try:
                        os.unlink(csv_path)
                    except:
                        pass
            finally:
                try:
                    conn.unregister(temp_view_name)
                except:
                    pass
        else:
            # Append to existing table
            temp_view_name = f'temp_{table_name}'
            conn.register(temp_view_name, df_final)
            try:
                conn.execute(f'INSERT INTO {full_table_name} SELECT * FROM {temp_view_name}')
                print(f"  ✓ {table_name}: {len(df_final):,} rows appended")
            finally:
                try:
                    conn.unregister(temp_view_name)
                except:
                    pass
            
    except Exception as e:
        print(f"  ❌ Error pushing {table_name}: {e}")
        raise


def search_nhisprocedures(search_terms: list, database: str = 'admin', client: Optional[MongoClient] = None) -> pd.DataFrame:
    """
    Search nhisprocedures collection for procedures containing the search terms.
    
    Args:
        search_terms: List of search terms to look for in procedure names
        database: Database name (default: 'admin')
        client: MongoDB client instance. If None, creates a new connection.
    
    Returns:
        pandas DataFrame with matching procedures
    """
    close_client = False
    if client is None:
        client = get_mongo_client()
        close_client = True
    
    try:
        db = client[database]
        coll = db['nhisprocedures']
        
        # Build search query - case insensitive search
        search_queries = []
        for term in search_terms:
            search_queries.append({
                'procedureName': {'$regex': term, '$options': 'i'}
            })
        
        # Combine with OR
        query = {'$or': search_queries} if len(search_queries) > 1 else search_queries[0]
        
        # Fetch matching documents
        documents = list(coll.find(query))
        
        if documents:
            df = pd.DataFrame(documents)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            return df
        else:
            return pd.DataFrame()
    
    finally:
        if close_client:
client.close()


# Main execution - can be called directly or imported
if __name__ == "__main__":
    print("="*70)
    print("MONGODB TO DUCKDB SYNC - NHIA SCHEMA")
    print("="*70)
    
    # Step 1: Fetch collections from MongoDB (excluding requests)
    print("\n[Step 1] Fetching collections from MongoDB...")
    print("-"*70)
    collections_to_fetch = ['nhisprocedures', 'nhiaenrollees', 'medicaldiagnoses']
    dfs = fetch_collections_as_dataframes(collections=collections_to_fetch)
    
    # Step 2: Get flattened requests
    print("\n[Step 2] Fetching and flattening requests...")
    print("-"*70)
    requests_flattened = get_requests_with_populated_procedures(limit=None)  # Get all requests
    
    # Add flattened requests to the dictionary
    dfs['requests'] = requests_flattened
    
    # Step 3: Connect to DuckDB and create schema
    print("\n[Step 3] Connecting to DuckDB...")
    print("-"*70)
    db_path = 'ai_driven_data.duckdb'
    conn = get_duckdb_connection(db_path)
    print(f"✓ Connected to DuckDB: {db_path}")
    
    create_nhia_schema(conn)
    
    # Step 4: Push all tables to DuckDB
    print("\n[Step 4] Pushing tables to DuckDB NHIA schema...")
    print("-"*70)
    
    # Define table mapping (collection name -> table name)
    table_mapping = {
        'nhisprocedures': 'nhisprocedures',
        'nhiaenrollees': 'nhiaenrollees',
        'medicaldiagnoses': 'medicaldiagnoses',
        'requests': 'requests'  # This is the flattened requests table
    }
    
    for collection_name, table_name in table_mapping.items():
        if collection_name in dfs:
            df = dfs[collection_name]
            print(f"\nPushing {collection_name} -> {table_name}...")
            push_dataframe_to_duckdb(df, table_name, conn, schema='NHIA', replace=True)
        else:
            print(f"\n⚠ {collection_name} not found in fetched data")
    
    # Close connection
    conn.close()
    
    # Summary
    print("\n" + "="*70)
    print("SYNC COMPLETE - SUMMARY")
    print("="*70)
    for collection_name, df in dfs.items():
        table_name = table_mapping.get(collection_name, collection_name)
        print(f"{table_name}: {len(df):,} rows × {len(df.columns)} columns")
    
    print("\n✓ All tables have been pushed to DuckDB NHIA schema")