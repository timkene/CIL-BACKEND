import pandas as pd
import googlemaps
from datetime import datetime
import time
from difflib import SequenceMatcher
import os
import sys
import json
from pathlib import Path
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def similarity_score(a, b):
    """Calculate similarity between two strings (0-1)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def verify_customer_with_google(df, api_key, customer_col='customer_name', 
                               lat_col='latitude', lon_col='longitude', 
                               radius_m=100, similarity_threshold=0.6,
                               sales_rep_col=None):
    """
    Verify customer names against actual businesses at given coordinates using Google Places API
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing customer data
    api_key : str
        Your Google Maps API key
    customer_col : str
        Name of the column containing customer names
    lat_col : str
        Name of the column containing latitude
    lon_col : str
        Name of the column containing longitude
    radius_m : int
        Search radius in meters (default: 100)
    similarity_threshold : float
        Minimum similarity score (0-1) to consider a match
    
    Returns:
    --------
    pandas.DataFrame with verification results
    """
    
    # Initialize Google Maps client
    gmaps = googlemaps.Client(key=api_key)
    
    results = []

    # Prepare incremental persistence
    output_path = Path('customer_verification_google_results.csv')
    state_path = Path('geomapping_state.json')
    header_needed = not output_path.exists() or output_path.stat().st_size == 0
    
    KEYWORDS = [
        'hospital', 'pharmacy', 'medicine', 'distributor', 'store',
        'clinic', 'drug', 'chemist', 'medical'
    ]

    for idx, row in df.iterrows():
        customer_name = row[customer_col]
        lat = row[lat_col]
        lon = row[lon_col]
        sales_rep = None
        if sales_rep_col and sales_rep_col in df.columns:
            sales_rep = row[sales_rep_col]

        # Skip invalid coordinates: missing or (0.0, 0.0)
        try:
            if pd.isna(lat) or pd.isna(lon):
                continue
            if float(lat) == 0.0 and float(lon) == 0.0:
                continue
        except Exception:
            continue
        
        print(f"Processing {idx + 1}/{len(df)}: {customer_name} at ({lat}, {lon})")
        
        try:
            # Method 1: Reverse geocode to get the address
            reverse_result = gmaps.reverse_geocode((lat, lon))
            
            address = reverse_result[0]['formatted_address'] if reverse_result else 'Not found'
            
            # Method 2: Search for nearby places within radius
            nearby_places = gmaps.places_nearby(
                location=(lat, lon),
                radius=radius_m
            )
            
            places_found = []
            best_match = ""
            best_score = 0
            best_place_details = {}
            
            all_candidates = {}
            keyword_places = []

            if nearby_places['results']:
                for place in nearby_places['results']:
                    place_name = place.get('name', '')
                    place_type = ', '.join(place.get('types', []))
                    place_address = place.get('vicinity', '')
                    
                    # Calculate similarity
                    score = similarity_score(customer_name, place_name)
                    
                    places_found.append({
                        'name': place_name,
                        'type': place_type,
                        'address': place_address,
                        'similarity': round(score, 3)
                    })

                    # Track all candidates (by name) with best score/details
                    existing = all_candidates.get(place_name)
                    if not existing or score > existing['similarity']:
                        all_candidates[place_name] = {
                            'name': place_name,
                            'type': place_type,
                            'address': place_address,
                            'similarity': round(score, 3)
                        }
                    
                    if score > best_score:
                        best_score = score
                        best_match = place_name
                        best_place_details = {
                            'name': place_name,
                            'type': place_type,
                            'address': place_address,
                            'place_id': place.get('place_id', ''),
                            'rating': place.get('rating', 'N/A'),
                            'business_status': place.get('business_status', 'N/A')
                        }

                    # Collect keyword-based places
                    lower_name = place_name.lower()
                    lower_types = place.get('types', [])
                    if any(k in lower_name for k in KEYWORDS) or any(k in lower_types for k in ['hospital', 'pharmacy', 'drugstore', 'health', 'store']):
                        keyword_places.append({
                            'name': place_name,
                            'type': place_type,
                            'address': place_address,
                            'rating': place.get('rating', 'N/A')
                        })
            
            # Method 3: Try text search with customer name at location
            text_search = gmaps.places(
                query=customer_name,
                location=(lat, lon),
                radius=radius_m
            )
            
            exact_match_found = False
            exact_match_details = {}
            
            if text_search['results']:
                for result in text_search['results']:
                    search_name = result.get('name', '')
                    search_score = similarity_score(customer_name, search_name)
                    
                    if search_score > best_score:
                        best_score = search_score
                        best_match = search_name
                        exact_match_found = True
                        exact_match_details = {
                            'name': search_name,
                            'address': result.get('formatted_address', ''),
                            'place_id': result.get('place_id', ''),
                            'rating': result.get('rating', 'N/A'),
                            'business_status': result.get('business_status', 'N/A')
                        }

                    # Feed into candidates pool as well
                    existing = all_candidates.get(search_name)
                    if not existing or search_score > existing['similarity']:
                        all_candidates[search_name] = {
                            'name': search_name,
                            'type': ', '.join(result.get('types', [])),
                            'address': result.get('formatted_address', ''),
                            'similarity': round(search_score, 3)
                        }
            
            # Check if customer name appears in address
            name_in_address = customer_name.lower() in address.lower()
            
            # Determine verification status
            is_match = best_score >= similarity_threshold or name_in_address
            
            # Determine confidence level
            if best_score >= 0.9:
                confidence = 'VERY_HIGH'
            elif best_score >= 0.75:
                confidence = 'HIGH'
            elif best_score >= 0.6:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'
            
            # Build alternative matches (top 3-4 excluding best)
            alt_matches = sorted(
                [c for c in all_candidates.values() if c['name'] != best_match],
                key=lambda x: x['similarity'], reverse=True
            )[:4]

            # Pick top 4 keyword places (prioritize rating desc when available)
            def rating_key(p):
                try:
                    return float(p.get('rating', 0))
                except Exception:
                    return 0.0

            keyword_places_top = sorted(keyword_places, key=rating_key, reverse=True)[:4]

            result_row = {
                'customer_name': customer_name,
                'latitude': lat,
                'longitude': lon,
                'sales_rep': sales_rep,
                'address_at_location': address,
                'places_within_radius': len(places_found),
                'best_match_name': best_match if best_match else 'None',
                'best_match_score': round(best_score, 3),
                'best_match_type': best_place_details.get('type', exact_match_details.get('business_status', 'N/A')),
                'best_match_address': best_place_details.get('address', exact_match_details.get('address', 'N/A')),
                'best_match_rating': best_place_details.get('rating', exact_match_details.get('rating', 'N/A')),
                'exact_search_found': exact_match_found,
                'name_in_address': name_in_address,
                'verification_status': 'MATCH' if is_match else 'NO_MATCH',
                'confidence': confidence,
                'all_nearby_places': str(places_found[:5]),  # Top 5 for reference
                'alternative_matches': str(alt_matches),
                'nearby_keyword_places': str(keyword_places_top)
            }
            results.append(result_row)
            
        except googlemaps.exceptions.ApiError as e:
            print(f"API Error for {customer_name}: {e}")
            result_row = {
                'customer_name': customer_name,
                'latitude': lat,
                'longitude': lon,
                'sales_rep': sales_rep,
                'address_at_location': f'API Error: {str(e)}',
                'places_within_radius': 0,
                'best_match_name': None,
                'best_match_score': 0,
                'best_match_type': None,
                'best_match_address': None,
                'best_match_rating': None,
                'exact_search_found': False,
                'name_in_address': False,
                'verification_status': 'ERROR',
                'confidence': 'N/A',
                'all_nearby_places': None,
                'alternative_matches': None,
                'nearby_keyword_places': None
            }
            results.append(result_row)
        except Exception as e:
            print(f"Error processing {customer_name}: {e}")
            result_row = {
                'customer_name': customer_name,
                'latitude': lat,
                'longitude': lon,
                'sales_rep': sales_rep,
                'address_at_location': f'Error: {str(e)}',
                'places_within_radius': 0,
                'best_match_name': None,
                'best_match_score': 0,
                'best_match_type': None,
                'best_match_address': None,
                'best_match_rating': None,
                'exact_search_found': False,
                'name_in_address': False,
                'verification_status': 'ERROR',
                'confidence': 'N/A',
                'all_nearby_places': None,
                'alternative_matches': None,
                'nearby_keyword_places': None
            }
            results.append(result_row)
        
        # Incremental persistence: append current row and checkpoint
        try:
            pd.DataFrame([result_row]).to_csv(
                output_path,
                mode='a',
                header=header_needed,
                index=False
            )
            header_needed = False
            state_path.write_text(json.dumps({
                'last_index': int(idx),
                'timestamp': datetime.utcnow().isoformat()
            }))
        except Exception as persist_err:
            print(f"Warning: failed to persist row {idx}: {persist_err}")

        # Small delay to avoid hitting rate limits
        time.sleep(0.1)
    
    return pd.DataFrame(results)

def preflight_check(api_key: str) -> tuple:
    """Validate API key authorization for Geocoding and Places APIs before heavy processing.

    Returns (ok: bool, message: str)
    """
    try:
        client = googlemaps.Client(key=api_key)
    except Exception as e:
        return False, f"Failed to initialize Google Maps client: {e}"

    # Quick Geocoding check
    try:
        _ = client.geocode("Lagos, Nigeria")
    except googlemaps.exceptions.ApiError as e:
        return False, f"Geocoding API error: {e}"
    except Exception as e:
        return False, f"Geocoding call failed: {e}"

    # Quick Places Nearby check
    try:
        _ = client.places_nearby(location=(6.5244, 3.3792), radius=100)
    except googlemaps.exceptions.ApiError as e:
        return False, f"Places API error: {e}"
    except Exception as e:
        return False, f"Places Nearby call failed: {e}"

    return True, "OK"

""" Example usage adapted to new CSV format

Expected columns:
- 'sales reps', 'Customer', 'address', 'Teritory Description', 'Channel Description', 'Location'
Where 'Location' looks like "(9.6239363, 3.2902753)"
"""
if __name__ == "__main__":
    # Your Google Maps API Key (load from environment if available)
    GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY', 'REPLACE_WITH_YOUR_API_KEY')

    # Preflight authorization check to fail fast
    ok, msg = preflight_check(GOOGLE_API_KEY)
    if not ok:
        print(f"Preflight check failed: {msg}")
        print("Please ensure billing is enabled, and both Places API and Geocoding API are enabled for this key, and key restrictions allow server IP usage.")
        sys.exit(1)

    # Load your data (allow overriding via CLI arg)
    # Usage: python geomapping.py "/path/to/input.csv"
    if len(sys.argv) > 1 and sys.argv[1].strip():
        input_csv = sys.argv[1]
    else:
        input_csv = '/Users/kenechukwuchukwuka/Downloads/DLT/Greenlife Secondary Customers.csv'
    if not os.path.exists(input_csv):
        print(f"Input CSV not found: {input_csv}")
        sys.exit(1)
    df = pd.read_csv(input_csv, encoding='latin-1')

    # Normalize expected columns and parse Location -> latitude/longitude
    if 'Location' in df.columns:
        loc = df['Location'].astype(str).str.strip()
        loc = loc.str.replace('[()]', '', regex=True)
        df['latitude'] = pd.to_numeric(loc.str.split(',').str[0].str.strip(), errors='coerce')
        df['longitude'] = pd.to_numeric(loc.str.split(',').str[1].str.strip(), errors='coerce')
    elif 'Column1' in df.columns:
        loc = df['Column1'].astype(str).str.strip()
        loc = loc.str.replace('[()]', '', regex=True)
        df['latitude'] = pd.to_numeric(loc.str.split(',').str[0].str.strip(), errors='coerce')
        df['longitude'] = pd.to_numeric(loc.str.split(',').str[1].str.strip(), errors='coerce')
    else:
        # Fallback: already split columns
        if 'latitude' not in df.columns or 'longitude' not in df.columns:
            raise ValueError("CSV must contain 'Location' or 'latitude' and 'longitude' columns")

    # Drop invalid coords, including (0.0, 0.0)
    df = df[df['latitude'].notna() & df['longitude'].notna()].copy()
    df = df[~((df['latitude'] == 0.0) & (df['longitude'] == 0.0))].copy()

    # Auto-resume: skip rows already processed (based on geomapping_state.json)
    state_file = 'geomapping_state.json'
    start_index = 0
    if os.path.exists(state_file):
        try:
            import json
            with open(state_file, 'r') as f:
                state = json.load(f)
            last_index = int(state.get('last_index', -1))
            if last_index >= 0:
                start_index = last_index + 1
                print(f"Resuming from saved index {start_index}")
        except Exception as e:
            print(f"Warning: could not read resume state, starting from 0 ({e})")

    if start_index > 0 and start_index < len(df):
        df_to_process = df.iloc[start_index:].copy()
    elif start_index >= len(df):
        print("Nothing to process. All rows have already been handled.")
        sys.exit(0)
    else:
        df_to_process = df

    # Run verification (now resume-aware)
    results_df = verify_customer_with_google(
        df_to_process,
        api_key=GOOGLE_API_KEY,
        customer_col='Customer',
        lat_col='latitude',
        lon_col='longitude',
        radius_m=100,
        similarity_threshold=0.6,
        sales_rep_col='sales reps'
    )

    # Results are already appended incrementally to CSV during processing

    # Minimal console summary
    total = len(results_df)
    matches = (results_df['verification_status'] == 'MATCH').sum()
    errors = (results_df['verification_status'] == 'ERROR').sum()
    print(f"Processed: {total} | MATCH: {matches} | ERROR: {errors}")

    