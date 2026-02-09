import requests
import json
import os
import time
from typing import Dict, List, Optional, Set, Tuple

# Constants
REQUEST_TIMEOUT = 10  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

class APIError(Exception):
    """Custom exception for API-related errors"""
    pass

class MovieTVCollector:
    """Main class for collecting and submitting movie/TV data"""
    
    def __init__(self, api_keys_file: str = 'api_keys.json'):
        """Initialize with API keys from file"""
        self.api_keys = self._load_api_keys(api_keys_file)
        self.tmdb_key = self.api_keys.get('tmdb_key')
        self.pmdb_key = self.api_keys.get('pmdb_key')
        self.mdblist_key = self.api_keys.get('mdblist_key')
        
        # API endpoints
        self.tmdb_search_url = "https://api.themoviedb.org/3/search/movie"
        self.tmdb_tv_search_url = "https://api.themoviedb.org/3/search/tv"
        self.tmdb_movie_url = "https://api.themoviedb.org/3/movie"
        self.tmdb_tv_url = "https://api.themoviedb.org/3/tv"
        self.mdblist_url = "https://mdblist.com/api/"
        self.pmdb_ratings_url = "https://publicmetadb.com/api/external/ratings"
        self.pmdb_mappings_url = "https://publicmetadb.com/api/external/mappings"
        
        # Validate essential keys
        self._validate_keys()
    
    def _load_api_keys(self, filename: str) -> Dict:
        """Load API keys from JSON file with error handling"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"[ERROR] {filename} file not found!")
            print(f"Please create a {filename} file with your API keys.")
            raise
        except json.JSONDecodeError as e:
            print(f"[ERROR] {filename} is not valid JSON: {e}")
            raise
    
    def _validate_keys(self):
        """Validate that essential API keys are present"""
        if not self.tmdb_key:
            raise ValueError("TMDB API key is required")
        if not self.pmdb_key:
            raise ValueError("PMDB API key is required")
        if not self.mdblist_key:
            raise ValueError("MDblist API key is required for ratings")
    
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with retry logic and timeout"""
        kwargs.setdefault('timeout', REQUEST_TIMEOUT)
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                if attempt == MAX_RETRIES - 1:
                    raise APIError(f"Request timed out after {MAX_RETRIES} attempts")
                time.sleep(RETRY_DELAY)
            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    raise APIError(f"Request failed: {e}")
                time.sleep(RETRY_DELAY)
    
    def search_tmdb(self, title: str, media_type: str = "movie") -> List[Dict]:
        """Search for a movie or TV show on TMDB"""
        url = self.tmdb_tv_search_url if media_type == "tv" else self.tmdb_search_url
        params = {"api_key": self.tmdb_key, "query": title}
        
        try:
            response = self._make_request('GET', url, params=params)
            data = response.json()
            return data.get('results', [])
        except Exception as e:
            print(f"[ERROR] Error searching TMDB: {e}")
            return []
    
    def get_tmdb_details(self, tmdb_id: int, media_type: str = "movie") -> Dict:
        """Get detailed info including IMDb ID from TMDB"""
        base_url = self.tmdb_tv_url if media_type == "tv" else self.tmdb_movie_url
        
        # Get external IDs and details
        external_url = f"{base_url}/{tmdb_id}/external_ids"
        details_url = f"{base_url}/{tmdb_id}"
        params = {"api_key": self.tmdb_key}
        
        result = {'external_ids': None, 'details': None}
        
        try:
            external_response = self._make_request('GET', external_url, params=params)
            result['external_ids'] = external_response.json()
        except Exception as e:
            print(f"[ERROR] Error getting TMDB external IDs: {e}")
        
        try:
            details_response = self._make_request('GET', details_url, params=params)
            result['details'] = details_response.json()
        except Exception as e:
            print(f"[ERROR] Error getting TMDB details: {e}")
        
        return result

    def get_mdblist_data(self, imdb_id: str) -> Optional[Dict]:
        """Fetch all ratings from MDblist"""
        if not self.mdblist_key:
            return None
            
        params = {"apikey": self.mdblist_key, "i": imdb_id}
        
        try:
            response = self._make_request('GET', self.mdblist_url, params=params)
            return response.json()
        except Exception as e:
            print(f"[ERROR] Error getting MDblist data: {e}")
            return None

    def parse_mdblist_ratings(self, data: Dict) -> Dict[str, float]:
        """Parse MDblist response into structured ratings (0-100 scale), using robust logic"""
        ratings = {}
        if not data:
            return ratings

        # --- PRIORITY 1: Top Level Fields (Most Reliable) ---
        
        # Metacritic (Metascore) - Top Level
        # This avoids picking up the User Score from the ratings list
        if 'Metascore' in data and data['Metascore'] != 'N/A':
            try:
                mc = float(data['Metascore'])
                if mc > 0: ratings['MC'] = mc
            except ValueError: pass
            
        # IMDb Rating - Top Level
        if 'imdbRating' in data and data['imdbRating'] != 'N/A':
            try:
                im = float(data['imdbRating'])
                if im > 0: ratings['IM'] = round(im * 10, 1) if im <= 10 else im
            except ValueError: pass

        # --- PRIORITY 2: Parse Ratings List (Backup/Extras) ---
        raw_ratings = data.get('ratings', [])
        
        for r in raw_ratings:
            source = r.get('source', '').lower()
            val = r.get('value')
            
            if val is None:
                continue

            # Robust value parsing (handle "53/100", "85%", etc.)
            try:
                clean_val = str(val).replace('%', '').strip()
                if '/' in clean_val:
                    score = float(clean_val.split('/')[0])
                else:
                    score = float(clean_val)
            except ValueError:
                continue 

            # Logic
            try:
                # IMDb (IM)
                if 'internet movie database' in source:
                    # Skip if we already got it from top level
                    if 'IM' not in ratings:
                        final = score * 10 if score <= 10 else score
                        if final > 0: ratings['IM'] = round(final, 1)

                # Rotten Tomatoes Critics (RT)
                elif source == 'rotten tomatoes':
                    if 'audience' not in source:
                        if score > 0: ratings['RT'] = score

                # Rotten Tomatoes Audience / Popcornmeter (PC)
                elif 'tomatoes' in source and 'audience' in source:
                    if score > 0: ratings['PC'] = score

                # Metacritic (MC)
                elif source == 'metacritic':
                    if 'user' not in source and 'MC' not in ratings:
                        # STRICT FILTER: Critic scores are rarely <= 10. User scores are usually <= 10.
                        # This prevents picking up "5.3" (User) vs "53" (Critic)
                        if score > 10: 
                            ratings['MC'] = score

                # Letterboxd (LB)
                elif 'letterboxd' in source:
                    # Convert /5 to /100
                    final = score * 20 if score <= 5 else (score * 10 if score <= 10 else score)
                    if final > 0: ratings['LB'] = round(final, 1)

                # Trakt (TR)
                elif 'trakt' in source:
                    final = score * 10 if score <= 10 else score
                    if final > 0: ratings['TR'] = round(final, 1)

            except (ValueError, IndexError):
                continue
                
        # Fallback: Check top level score for Trakt if not in list
        if 'score' in data and data['score'] and 'TR' not in ratings:
            try:
                tr_score = float(data['score'])
                if tr_score > 0:
                    ratings['TR'] = round(tr_score, 1)
            except ValueError:
                pass

        return ratings
    
    @staticmethod
    def parse_tmdb_rating(tmdb_details: Optional[Dict]) -> Optional[float]:
        """Parse TMDB rating"""
        if not tmdb_details:
            return None
        
        vote_avg = tmdb_details.get('vote_average')
        if vote_avg and vote_avg > 0:
            return round(float(vote_avg) * 10, 1)
        return None
    
    def get_existing_mappings(self, tmdb_id: int, media_type: str = "movie") -> Dict[str, List[str]]:
        """Check existing ID mappings in PMDB"""
        headers = {"Authorization": f"Bearer {self.pmdb_key}"}
        params = {"tmdb_id": tmdb_id, "media_type": media_type}
        
        try:
            response = self._make_request(
                'GET',
                self.pmdb_mappings_url,
                headers=headers,
                params=params
            )
            
            data = response.json()
            existing_mappings = {}
            
            if isinstance(data, dict) and 'mappings' in data:
                for id_type, mappings_list in data['mappings'].items():
                    existing_mappings[id_type] = [
                        m['value'] for m in mappings_list if 'value' in m
                    ]
            
            return existing_mappings
        except Exception as e:
            if "404" not in str(e):
                print(f"  [WARNING] Could not check existing mappings: {e}")
            return {}
    
    def get_existing_ratings(self, tmdb_id: int, media_type: str = "movie") -> Set[str]:
        """Check existing ratings in PMDB"""
        headers = {"Authorization": f"Bearer {self.pmdb_key}"}
        params = {"tmdb_id": tmdb_id, "media_type": media_type}
        
        try:
            response = self._make_request(
                'GET',
                self.pmdb_ratings_url,
                headers=headers,
                params=params
            )
            
            data = response.json()
            existing_labels = set()
            
            items = data.get('items', []) if isinstance(data, dict) else data
            for rating in items:
                if 'label' in rating:
                    existing_labels.add(rating['label'].upper())
            
            return existing_labels
        except Exception as e:
            if "404" not in str(e):
                print(f"  [WARNING] Could not check existing ratings: {e}")
            return set()
    
    @staticmethod
    def get_safe_year(date_str: str, default: str = "Unknown") -> str:
        """Safely extract year from date string"""
        if not date_str or date_str == "Unknown":
            return default
        try:
            return date_str[:4] if len(date_str) >= 4 else default
        except (TypeError, IndexError):
            return default
    
    def display_item_info(
        self,
        item: Dict,
        imdb_id: str,
        tvdb_id: Optional[str],
        new_ratings: Dict[str, float],
        existing_ratings: Dict[str, float],
        media_type: str = "movie"
    ):
        """Display collected information for verification"""
        print("\n" + "=" * 70)
        print(f"{media_type.upper()} INFORMATION")
        print("=" * 70)
        
        # Get title and year based on media type
        if media_type == "tv":
            title = item.get('name', 'Unknown')
            year = self.get_safe_year(item.get('first_air_date', ''))
        else:
            title = item.get('title', 'Unknown')
            year = self.get_safe_year(item.get('release_date', ''))
        
        print(f"Title: {title}")
        print(f"Year: {year}")
        print(f"TMDB ID: {item['id']}")
        print(f"IMDb ID: {imdb_id}")
        if tvdb_id and media_type == "tv":
            print(f"TVDB ID: {tvdb_id}")
        
        if existing_ratings:
            print("\n" + "-" * 70)
            print("RATINGS ALREADY IN PMDB (will skip):")
            print("-" * 70)
            for source, score in sorted(existing_ratings.items()):
                print(f"  {source}: {score}/100 [EXISTS]")
        
        print("\n" + "-" * 70)
        print("NEW RATINGS TO SUBMIT:")
        print("-" * 70)
        
        if new_ratings:
            for source, score in sorted(new_ratings.items()):
                print(f"  {source}: {score}/100 [NEW]")
        else:
            print("  No new ratings to submit (all already exist)")
        
        print("=" * 70 + "\n")
    
    def submit_mapping(
        self,
        tmdb_id: int,
        id_value: str,
        id_type: str,
        media_type: str = "movie"
    ) -> bool:
        """Submit ID mapping to PMDB"""
        headers = {
            "Authorization": f"Bearer {self.pmdb_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "tmdb_id": tmdb_id,
            "media_type": media_type,
            "id_type": id_type,
            "id_value": id_value
        }
        
        try:
            response = self._make_request(
                'POST',
                self.pmdb_mappings_url,
                headers=headers,
                json=payload
            )
            print(f"[OK] Mapping submitted: TMDB {tmdb_id} -> {id_type.upper()} {id_value}")
            return True
        except Exception as e:
            print(f"[ERROR] Error submitting {id_type} mapping: {e}")
            return False
    
    def submit_rating(
        self,
        tmdb_id: int,
        score: float,
        label: str,
        media_type: str = "movie"
    ) -> bool:
        """Submit a single rating to PMDB"""
        headers = {
            "Authorization": f"Bearer {self.pmdb_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "tmdb_id": tmdb_id,
            "media_type": media_type,
            "score": score,
            "label": label
        }
        
        try:
            response = self._make_request(
                'POST',
                self.pmdb_ratings_url,
                headers=headers,
                json=payload
            )
            print(f"[OK] Rating submitted: {label} = {score}")
            return True
        except Exception as e:
            print(f"[ERROR] Error submitting {label} rating: {e}")
            return False
    
    def process_item(self):
        """Process a single movie or TV show"""
        # Step 1: Choose media type
        media_type_choice = input("Search for (1) Movie or (2) TV Show? Enter 1 or 2: ").strip()
        media_type = "tv" if media_type_choice == "2" else "movie"
        media_label = "TV show" if media_type == "tv" else "movie"
        
        # Step 2: Search TMDB
        title = input(f"\nEnter {media_label} title to search: ").strip()
        if not title:
            print("[ERROR] Title cannot be empty.")
            return
        
        results = self.search_tmdb(title, media_type)
        
        if not results:
            print(f"No {media_label}s found.")
            return
        
        # Display results
        print(f"\nSearch Results:")
        print("-" * 70)
        for idx, item in enumerate(results[:10], 1):
            if media_type == "tv":
                item_title = item.get('name', 'Unknown')
                year = self.get_safe_year(item.get('first_air_date', ''))
            else:
                item_title = item.get('title', 'Unknown')
                year = self.get_safe_year(item.get('release_date', ''))
            print(f"{idx}. {item_title} ({year}) - TMDB ID: {item['id']}")
        print("-" * 70)
        
        # Step 3: Select item
        try:
            choice = int(input(f"\nSelect {media_label} number (or 0 to cancel): "))
            if choice == 0:
                print("Cancelled.")
                return
            
            if not (1 <= choice <= len(results[:10])):
                print("[ERROR] Invalid selection.")
                return
            
            selected_item = results[choice - 1]
            tmdb_id = selected_item['id']
            
        except ValueError:
            print("[ERROR] Invalid input. Please enter a number.")
            return
        
        # Step 4: Get IMDb ID and TMDB rating
        print(f"\nFetching data from TMDB...")
        tmdb_data = self.get_tmdb_details(tmdb_id, media_type)
        
        external_ids = tmdb_data.get('external_ids', {})
        imdb_id = external_ids.get('imdb_id') if external_ids else None
        
        if not imdb_id:
            print(f"[ERROR] Could not find IMDb ID for this {media_label}.")
            return
        
        # Step 5: Get ALL ratings from MDblist (plus IDs)
        print(f"Fetching ratings and IDs from MDblist...")
        mdblist_data = self.get_mdblist_data(imdb_id)
        
        ratings = {}
        tvdb_id = None
        
        if mdblist_data:
            ratings = self.parse_mdblist_ratings(mdblist_data)
            
            # Extract TVDB ID from MDblist
            if media_type == "tv":
                # MDblist usually provides this as 'tvdbid'
                tvdb_id = mdblist_data.get('tvdbid')
                if tvdb_id:
                    print(f"[OK] Found TVDB ID via MDblist: {tvdb_id}")
        else:
            print("[WARNING] Could not fetch MDblist data.")
        
        # Add TMDB rating (from TMDB direct source)
        tmdb_rating = self.parse_tmdb_rating(tmdb_data.get('details'))
        if tmdb_rating:
            ratings['TM'] = tmdb_rating
        
        # Step 6: Check existing mappings
        print(f"Checking existing ID mappings in PMDB...")
        existing_mappings = self.get_existing_mappings(tmdb_id, media_type)
        
        imdb_exists = 'imdb' in existing_mappings and imdb_id in existing_mappings['imdb']
        tvdb_exists = (tvdb_id and 'tvdb' in existing_mappings and 
                       str(tvdb_id) in existing_mappings['tvdb'])
        
        # Step 7: Check existing ratings
        print(f"Checking existing ratings in PMDB...")
        existing_labels = self.get_existing_ratings(tmdb_id, media_type)
        
        # Separate new and existing ratings
        new_ratings = {
            label: score for label, score in ratings.items() 
            if label.upper() not in existing_labels
        }
        existing_ratings = {
            label: score for label, score in ratings.items() 
            if label.upper() in existing_labels
        }
        
        # Step 8: Display info
        self.display_item_info(
            selected_item, imdb_id, tvdb_id,
            new_ratings, existing_ratings, media_type
        )
        
        # Step 9: Handle mappings
        mappings_to_submit = []
        if not imdb_exists:
            mappings_to_submit.append(('imdb', imdb_id))
        
        # Add TVDB mapping if found and not existing
        if tvdb_id and not tvdb_exists:
            mappings_to_submit.append(('tvdb', str(tvdb_id)))
        
        if mappings_to_submit:
            print("=" * 70)
            print("ID MAPPINGS TO SUBMIT")
            print("=" * 70)
            for id_type, id_value in mappings_to_submit:
                print(f"TMDB ID {tmdb_id} -> {id_type.upper()} {id_value}")
            print("=" * 70 + "\n")
            
            confirm = input("Submit ID mapping(s) to PMDB? (y/n, Enter=yes): ").lower().strip()
            
            if confirm in ['y', 'yes', '']:
                print("\nSubmitting mappings...")
                print("-" * 70)
                for id_type, id_value in mappings_to_submit:
                    self.submit_mapping(tmdb_id, id_value, id_type, media_type)
                print("-" * 70 + "\n")
            else:
                print("Mapping submission skipped.\n")
        else:
            print("[INFO] All ID mappings already exist in PMDB\n")
        
        # Step 10: Handle ratings
        if not new_ratings:
            print("[INFO] No new ratings to submit - all ratings already exist!")
            return
        
        confirm = input("Submit new ratings to PMDB? (y/n, Enter=yes): ").lower().strip()
        
        if confirm not in ['y', 'yes', '']:
            print("Ratings submission cancelled.")
            return
        
        # Step 11: Submit ratings
        print("\nSubmitting ratings...")
        print("-" * 70)
        
        success_count = 0
        for label, score in new_ratings.items():
            if self.submit_rating(tmdb_id, score, label, media_type):
                success_count += 1
        
        print("-" * 70)
        print(f"\n[OK] Successfully submitted {success_count}/{len(new_ratings)} rating(s)!")
        if existing_ratings:
            print(f"  Skipped {len(existing_ratings)} existing rating(s).")
    
    def run(self):
        """Main program loop"""
        print("=" * 70)
        print("Movie/TV Data Collector (Powered by MDblist)")
        print("=" * 70 + "\n")
        
        while True:
            try:
                self.process_item()
            except KeyboardInterrupt:
                print("\n\n[INFO] Operation cancelled by user.")
                break
            except Exception as e:
                print(f"\n[ERROR] Unexpected error: {e}")
                import traceback
                traceback.print_exc()
            
            print("\n" + "=" * 70)
            another = input("\nProcess another item? (y/n, Enter=yes): ").lower().strip()
            
            if another not in ['y', 'yes', '']:
                print("\nExiting. Goodbye!")
                break
            
            print("\n" + "=" * 70 + "\n")


def main():
    """Entry point"""
    try:
        collector = MovieTVCollector()
        collector.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
