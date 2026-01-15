import requests
import json
import os

# Load API keys from JSON file
def load_api_keys():
    """Load API keys from api_keys.json file"""
    try:
        with open('api_keys.json', 'r') as f:
            keys = json.load(f)
            return keys
    except FileNotFoundError:
        print("Error: api_keys.json file not found!")
        print("Please create an api_keys.json file with your API keys.")
        exit(1)
    except json.JSONDecodeError:
        print("Error: api_keys.json is not valid JSON!")
        exit(1)

# Load keys
api_keys = load_api_keys()
TMDB_API_KEY = api_keys.get('tmdb_key')
OMDB_API_KEY = api_keys.get('omdb_key')
PMDB_API_KEY = api_keys.get('pmdb_key')
TVDB_API_KEY = api_keys.get('tvdb_key')

TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/movie"
TMDB_TV_SEARCH_URL = "https://api.themoviedb.org/3/search/tv"
TMDB_MOVIE_URL = "https://api.themoviedb.org/3/movie"
TMDB_TV_URL = "https://api.themoviedb.org/3/tv"
OMDB_URL = "http://www.omdbapi.com/"
PMDB_RATINGS_URL = "https://publicmetadb.com/api/external/ratings"
PMDB_MAPPINGS_URL = "https://publicmetadb.com/api/external/mappings"
TVDB_BASE_URL = "https://api4.thetvdb.com/v4"
TVDB_LOGIN_URL = f"{TVDB_BASE_URL}/login"
TVDB_SEARCH_URL = f"{TVDB_BASE_URL}/search"

# Global variable to store TVDB token
tvdb_token = None

def get_tvdb_token():
    """Authenticate with TVDB and get bearer token"""
    global tvdb_token
    
    if not TVDB_API_KEY:
        print("Warning: TVDB API key not found. TVDB features will be disabled.")
        return None
    
    if tvdb_token:
        return tvdb_token
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        "apikey": TVDB_API_KEY
    }
    
    try:
        response = requests.post(TVDB_LOGIN_URL, headers=headers, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            tvdb_token = data.get('data', {}).get('token')
            if tvdb_token:
                print("✓ TVDB authentication successful")
                return tvdb_token
            else:
                print("Warning: TVDB token not found in response")
                return None
        else:
            print(f"Warning: TVDB authentication failed ({response.status_code})")
            return None
    except Exception as e:
        print(f"Warning: Error authenticating with TVDB: {e}")
        return None

def search_tvdb_by_imdb(imdb_id):
    """Search TVDB using IMDb ID"""
    token = get_tvdb_token()
    
    if not token:
        return None
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "query": imdb_id,
        "type": "series"
    }
    
    try:
        response = requests.get(TVDB_SEARCH_URL, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('data', [])
            
            # Look for exact IMDb match in remote_ids
            for result in results:
                remote_ids = result.get('remote_ids', [])
                for remote_id in remote_ids:
                    if remote_id.get('sourceName') == 'IMDB' and remote_id.get('id') == imdb_id:
                        return result
            
            # If no exact match, return first result
            if results:
                return results[0]
        else:
            print(f"  TVDB search returned status {response.status_code}")
        
        return None
    except Exception as e:
        print(f"  Error searching TVDB: {e}")
        return None





def search_tmdb(title, media_type="movie"):
    """Search for a movie or TV show on TMDB"""
    if media_type == "tv":
        url = TMDB_TV_SEARCH_URL
    else:
        url = TMDB_SEARCH_URL
    
    params = {
        "api_key": TMDB_API_KEY,
        "query": title
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        return data.get('results', [])
    else:
        print(f"Error searching TMDB: {response.status_code}")
        return []

def get_tmdb_details(tmdb_id, media_type="movie"):
    """Get detailed info including IMDb ID and rating from TMDB"""
    # Select the correct base URL
    if media_type == "tv":
        base_url = TMDB_TV_URL
    else:
        base_url = TMDB_MOVIE_URL
    
    # Get external IDs
    external_url = f"{base_url}/{tmdb_id}/external_ids"
    external_params = {"api_key": TMDB_API_KEY}
    
    external_response = requests.get(external_url, params=external_params)
    
    # Get details (includes rating)
    details_url = f"{base_url}/{tmdb_id}"
    details_params = {"api_key": TMDB_API_KEY}
    
    details_response = requests.get(details_url, params=details_params)
    
    result = {}
    
    if external_response.status_code == 200:
        result['external_ids'] = external_response.json()
    else:
        print(f"Error getting TMDB external IDs: {external_response.status_code}")
        result['external_ids'] = None
    
    if details_response.status_code == 200:
        result['details'] = details_response.json()
    else:
        print(f"Error getting TMDB details: {details_response.status_code}")
        result['details'] = None
    
    return result

def get_omdb_ratings(imdb_id, media_type="movie"):
    """Get ratings from OMDb (includes IMDb, Rotten Tomatoes, Metacritic)"""
    params = {
        "apikey": OMDB_API_KEY,
        "i": imdb_id
    }
    
    # Add type parameter for OMDb (they use 'series' instead of 'tv')
    if media_type == "tv":
        params["type"] = "series"
    
    response = requests.get(OMDB_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error getting OMDb data: {response.status_code}")
        return None

def parse_omdb_ratings(omdb_data):
    """Parse OMDb ratings into a structured format using PMDB abbreviations"""
    ratings = {}
    
    # IMDb rating (out of 10, convert to 100) - Use IM abbreviation
    if omdb_data.get('imdbRating') and omdb_data['imdbRating'] != 'N/A':
        imdb_score = float(omdb_data['imdbRating']) * 10
        ratings['IM'] = round(imdb_score, 1)
    
    # Parse the Ratings array for RT and Metacritic
    for rating in omdb_data.get('Ratings', []):
        source = rating.get('Source')
        value = rating.get('Value')
        
        if source == 'Rotten Tomatoes' and value != 'N/A':
            # RT = Rotten Tomatoes Critics/Tomatometer
            score = float(value.replace('%', ''))
            ratings['RT'] = score
        
        elif source == 'Metacritic' and value != 'N/A':
            # MC = Metacritic
            score = float(value.split('/')[0])
            ratings['MC'] = score
    
    return ratings

def parse_tmdb_rating(tmdb_details):
    """Parse TMDB rating"""
    if tmdb_details and 'vote_average' in tmdb_details:
        vote_avg = tmdb_details['vote_average']
        
        if vote_avg and vote_avg > 0:
            # TMDB rating is out of 10, convert to 100
            return round(float(vote_avg) * 10, 1)
    return None

def get_existing_mappings(tmdb_id, media_type="movie"):
    """Check what ID mappings already exist in PMDB for this movie"""
    headers = {
        "Authorization": f"Bearer {PMDB_API_KEY}"
    }
    
    params = {
        "tmdb_id": tmdb_id,
        "media_type": media_type
    }
    
    try:
        response = requests.get(PMDB_MAPPINGS_URL, headers=headers, params=params)
        
        if response.status_code == 200:
            try:
                data = response.json()
                
                # Extract existing mapping types and values
                existing_mappings = {}
                
                if isinstance(data, dict) and 'mappings' in data:
                    for id_type, mappings_list in data['mappings'].items():
                        # Store the values for each id_type
                        existing_mappings[id_type] = [m['value'] for m in mappings_list if 'value' in m]
                
                return existing_mappings
            except json.JSONDecodeError as e:
                print(f"  Error parsing mappings JSON: {e}")
                return {}
        elif response.status_code == 404:
            return {}
        else:
            print(f"  Note: Could not check existing mappings ({response.status_code})")
            return {}
    except Exception as e:
        print(f"  Error checking existing mappings: {e}")
        return {}

def get_existing_ratings(tmdb_id, media_type="movie"):
    """Check what ratings already exist in PMDB for this movie"""
    headers = {
        "Authorization": f"Bearer {PMDB_API_KEY}"
    }
    
    params = {
        "tmdb_id": tmdb_id,
        "media_type": media_type
    }
    
    try:
        response = requests.get(PMDB_RATINGS_URL, headers=headers, params=params)
        
        if response.status_code == 200:
            try:
                data = response.json()
                
                # Extract existing labels into a set for quick lookup
                # Convert to uppercase for case-insensitive comparison
                existing_labels = set()
                
                # PMDB returns data with an 'items' key
                if isinstance(data, dict) and 'items' in data:
                    for rating in data['items']:
                        if 'label' in rating:
                            # Store as uppercase for consistent comparison
                            existing_labels.add(rating['label'].upper())
                elif isinstance(data, list):
                    # Fallback if they return a list directly
                    for rating in data:
                        if 'label' in rating:
                            existing_labels.add(rating['label'].upper())
                
                return existing_labels
            except json.JSONDecodeError as e:
                print(f"  Error parsing JSON: {e}")
                return set()
        elif response.status_code == 404:
            print(f"  No ratings found (404 - this is normal for new entries)")
            return set()
        else:
            print(f"  Note: Could not check existing ratings ({response.status_code})")
            return set()
    except Exception as e:
        print(f"  Error checking existing ratings: {e}")
        return set()

def display_movie_info(item, imdb_id, tvdb_id, new_ratings, existing_ratings, media_type="movie"):
    """Display all collected information for verification"""
    print("\n" + "="*70)
    print(f"{media_type.upper()} INFORMATION")
    print("="*70)
    
    # Handle both movie and TV show title fields
    if media_type == "tv":
        title = item.get('name', 'Unknown')
        year = item.get('first_air_date', 'Unknown')[:4]
    else:
        title = item.get('title', 'Unknown')
        year = item.get('release_date', 'Unknown')[:4]
    
    print(f"Title: {title}")
    print(f"Year: {year}")
    print(f"TMDB ID: {item['id']}")
    print(f"IMDb ID: {imdb_id}")
    if tvdb_id and media_type == "tv":
        print(f"TVDB ID: {tvdb_id}")
    
    if existing_ratings:
        print("\n" + "-"*70)
        print("RATINGS ALREADY IN PMDB (will skip):")
        print("-"*70)
        for source, score in existing_ratings.items():
            print(f"  {source}: {score}/100 [EXISTS]")
    
    print("\n" + "-"*70)
    print("NEW RATINGS TO SUBMIT:")
    print("-"*70)
    
    if new_ratings:
        for source, score in new_ratings.items():
            print(f"  {source}: {score}/100 [NEW]")
    else:
        print("  No new ratings to submit (all already exist)")
    
    print("="*70 + "\n")

def submit_mapping(tmdb_id, id_value, id_type, media_type="movie"):
    """Submit ID mapping to PMDB"""
    headers = {
        "Authorization": f"Bearer {PMDB_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "id_type": id_type,
        "id_value": id_value
    }
    
    response = requests.post(PMDB_MAPPINGS_URL, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"✓ Mapping submitted: TMDB {tmdb_id} → {id_type.upper()} {id_value}")
        return True
    else:
        print(f"✗ Error submitting {id_type} mapping: {response.status_code}")
        print(f"Response: {response.text}")
        return False

def submit_rating(tmdb_id, score, label, media_type="movie"):
    """Submit a single rating to PMDB"""
    headers = {
        "Authorization": f"Bearer {PMDB_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "score": score,
        "label": label
    }
    
    response = requests.post(PMDB_RATINGS_URL, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"✓ Rating submitted: {label} = {score}")
        return True
    else:
        print(f"✗ Error submitting {label} rating: {response.status_code}")
        print(f"Response: {response.text}")
        return False

def process_item():
    """Process a single movie or TV show"""
    # Step 1: Ask for media type
    media_type_choice = input("Search for (1) Movie or (2) TV Show? Enter 1 or 2: ").strip()
    
    if media_type_choice == "2":
        media_type = "tv"
        media_label = "TV show"
    else:
        media_type = "movie"
        media_label = "movie"
    
    # Step 2: Search TMDB
    title = input(f"\nEnter {media_label} title to search: ")
    results = search_tmdb(title, media_type)
    
    if not results:
        print(f"No {media_label}s found.")
        return
    
    # Display results
    print(f"\nSearch Results:")
    print("-" * 70)
    for idx, item in enumerate(results[:10], 1):
        if media_type == "tv":
            item_title = item.get('name', 'Unknown')
            year = item.get('first_air_date', 'Unknown')[:4]
        else:
            item_title = item.get('title', 'Unknown')
            year = item.get('release_date', 'Unknown')[:4]
        print(f"{idx}. {item_title} ({year}) - TMDB ID: {item['id']}")
    print("-" * 70)
    
    # Step 3: Select item
    try:
        choice = int(input(f"\nSelect {media_label} number (or 0 to cancel): "))
        if choice == 0:
            print("Cancelled.")
            return
        
        if not (1 <= choice <= len(results[:10])):
            print("Invalid selection.")
            return
        
        selected_item = results[choice - 1]
        tmdb_id = selected_item['id']
        
    except ValueError:
        print("Invalid input.")
        return
    
    # Step 4: Get IMDb ID and rating from TMDB
    print(f"\nFetching external IDs and rating from TMDB...")
    tmdb_data = get_tmdb_details(tmdb_id, media_type)
    
    if not tmdb_data or not tmdb_data.get('external_ids') or not tmdb_data['external_ids'].get('imdb_id'):
        print(f"Could not find IMDb ID for this {media_label}.")
        return
    
    imdb_id = tmdb_data['external_ids']['imdb_id']
    
    # Step 5: Get ratings from OMDb
    print(f"Fetching ratings from OMDb...")
    omdb_data = get_omdb_ratings(imdb_id, media_type)
    
    if not omdb_data or omdb_data.get('Response') == 'False':
        print("Could not fetch OMDb data.")
        return
    
    ratings = parse_omdb_ratings(omdb_data)
    
    # Add TMDB rating if available
    tmdb_rating = parse_tmdb_rating(tmdb_data.get('details'))
    if tmdb_rating:
        ratings['TM'] = tmdb_rating
    
    # Step 6: Get TVDB ID (TV shows only)
    tvdb_id = None
    if media_type == "tv":
        print(f"Fetching TVDB ID...")
        tvdb_data = search_tvdb_by_imdb(imdb_id)
        
        if tvdb_data:
            tvdb_id = tvdb_data.get('tvdb_id')
            
            if tvdb_id:
                print(f"✓ Found TVDB ID: {tvdb_id}")
            else:
                print("  TVDB ID not found in search results")
        else:
            print("  Could not find show on TVDB")
    
    # Step 7: Check existing mappings in PMDB
    print(f"Checking existing ID mappings in PMDB...")
    existing_mappings = get_existing_mappings(tmdb_id, media_type)
    
    # Check if IMDb mapping already exists
    imdb_mapping_exists = 'imdb' in existing_mappings and imdb_id in existing_mappings['imdb']
    if imdb_mapping_exists:
        print(f"Found existing IMDb mapping: {imdb_id}")
    else:
        print("No existing IMDb mapping found.")
    
    # Check if TVDB mapping already exists
    tvdb_mapping_exists = False
    if tvdb_id:
        tvdb_mapping_exists = 'tvdb' in existing_mappings and str(tvdb_id) in existing_mappings['tvdb']
        if tvdb_mapping_exists:
            print(f"Found existing TVDB mapping: {tvdb_id}")
        else:
            print("No existing TVDB mapping found.")
    
    # Step 8: Check existing ratings in PMDB
    print(f"Checking existing ratings in PMDB...")
    existing_labels = get_existing_ratings(tmdb_id, media_type)
    
    if existing_labels:
        print(f"Found existing ratings: {', '.join(sorted(existing_labels))}")
    else:
        print("No existing ratings found.")
    
    # Filter out ratings that already exist (case-insensitive comparison)
    new_ratings = {label: score for label, score in ratings.items() 
                   if label.upper() not in existing_labels}
    existing_ratings = {label: score for label, score in ratings.items() 
                        if label.upper() in existing_labels}
    
    # Step 9: Display everything for verification
    display_movie_info(selected_item, imdb_id, tvdb_id, new_ratings, existing_ratings, media_type)
    
    # Step 10: Ask about mappings
    mappings_to_submit = []
    
    if not imdb_mapping_exists:
        mappings_to_submit.append(('imdb', imdb_id))
    
    if tvdb_id and not tvdb_mapping_exists:
        mappings_to_submit.append(('tvdb', str(tvdb_id)))
    
    if mappings_to_submit:
        print("=" * 70)
        print("ID MAPPINGS")
        print("=" * 70)
        for id_type, id_value in mappings_to_submit:
            print(f"TMDB ID {tmdb_id} → {id_type.upper()} {id_value}")
        print("=" * 70 + "\n")
        
        confirm_mapping = input("Submit ID mapping(s) to PMDB? (y/n or press Enter for yes): ").lower().strip()
        
        if confirm_mapping in ['y', 'yes', '']:
            print("\nSubmitting mappings...")
            print("-" * 70)
            for id_type, id_value in mappings_to_submit:
                submit_mapping(tmdb_id, id_value, id_type, media_type)
            print("-" * 70 + "\n")
        else:
            print("Mapping submission skipped.\n")
    else:
        print("=" * 70)
        print("ID MAPPINGS")
        print("=" * 70)
        print(f"All ID mappings already exist in PMDB")
        print("=" * 70 + "\n")
    
    # Step 11: Ask about ratings (only if there's something to submit)
    if not new_ratings:
        print("No new ratings to submit - all ratings already exist in PMDB!")
        return
    
    confirm_ratings = input("Submit new ratings to PMDB? (y/n or press Enter for yes): ").lower().strip()
    
    if confirm_ratings not in ['y', 'yes', '']:
        print("Ratings submission cancelled.")
        return
    
    # Step 12: Submit ratings to PMDB
    print("\nSubmitting ratings...")
    print("-" * 70)
    
    # Submit only new ratings
    for label, score in new_ratings.items():
        submit_rating(tmdb_id, score, label, media_type)
    
    print("-" * 70)
    print(f"\n✓ Submitted {len(new_ratings)} new rating(s)!")
    if existing_ratings:
        print(f"  Skipped {len(existing_ratings)} existing rating(s).")

def main():
    print("=== Movie/TV Data Collector & Submitter ===\n")
    
    while True:
        process_item()
        
        print("\n" + "=" * 70)
        another = input("\nProcess another movie/TV show? (y/n or press Enter for yes): ").lower().strip()
        
        # Accept 'y', 'yes', or empty string (Enter) as yes
        if another not in ['y', 'yes', '']:
            print("\nExiting. Goodbye!")
            break
        
        print("\n" + "=" * 70 + "\n")

if __name__ == "__main__":
    main()
