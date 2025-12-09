import json
import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any
import requests

"""
OMDb API client with local caching and rate limiting.

Handles:
- API calls with retry logic
- Local JSON cache to avoid redundant calls
- Graceful handling of missing/unmatched movies
"""

logger = logging.getLogger(__name__)

@dataclass
class MovieMetadata:
    """Enriched from OMDB"""
    title: str
    year: str | None
    rated: str | None
    released: str | None
    runtime: str | None
    genre: str | None
    director: str | None
    actors: str | None
    plot: str | None
    language: str | None
    country: str | None
    awards: str | None
    poster_url: str | None
    metascore: int | None
    imdb_rating: float | None
    imdb_votes: int | None
    imdb_id: str | None
    box_office: str | None
    
    # Metadata
    enriched_at: str | None = None
    api_response_type: str | None = None  # "match", "not_found", "error"

class OMDbClientError(Exception):
    """Raised when API call fails after retries."""
    pass

class OMDbClient:
    """
    Client for OMDb API with caching and retry logic.
    
    Usage:
        client = OMDbClient(api_key="xxx", cache_path=Path("cache/omdb.json"))
        movie = client.get_movie("The Dark Knight", year=2008)
    """

    BASE_URL = "http://www.omdbapi.com/"

    def __init__(
        self,
        api_key: str,
        cache_path: Path,
        requests_per_day: int = 1000,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
    ):
        self.api_key = api_key
        self.cache_path = cache_path
        self.requests_per_day = requests_per_day
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

        self._cache: dict[str, dict] = {}
        self._api_calls_made = 0
        self._load_cache()

    def _load_cache(self) -> None:
        """Load existing cache from disk."""
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
                logger.info(f"Loaded {len(self._cache)} cached movies from {self.cache_path}")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load cache: {e}. Starting fresh.")
                self._cache = {}
        else:
            logger.info(f"No existing cache at {self.cache_path}")
            self._cache = {}

    def _save_cache(self) -> None:
        """Persist cache to disk"""
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, 'w', encoding='utf-8') as f:
            json.dump(self._cache, f, indent=2, ensure_ascii=False)

    def _make_cache_key(self, title: str, year: int | None = None) -> str:
        """Generate cache key from title and optional year"""
        normalized_title = title.lower().strip()
        if year:
            return f"{normalized_title}|{year}"
        return normalized_title

    def get_movie(self, title: str, year: int | None = None) -> MovieMetadata | None:
        """
        Get movie metadata, using cache if available.
        
        Args:
            title: Movie title to search for
            year: Optional release year to improve matching
            
        Returns:
            MovieMetadata if found, None if not found or error
        """
        cache_key = self._make_cache_key(title, year)

        # check cache first
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            logger.debug(f"Cache hit: {title}")
            return self._dict_to_metadata(cached) if cached.get('api_response_type') == 'match' else None

        # call api
        response_data = self._call_api(title, year)

        if response_data is None:
            # api error, cache as error and don't retry
            self._cache[cache_key] = {
                'title': title,
                'api_response_type': 'error',
                'enriched_at': datetime.utcnow().isoformat(),
            }
            self._save_cache()
            return None
        
        if response_data.get('Response') == 'False':
            # movie not found
            logger.info(f"Not found OMDb title: {title}")
            self._cache[cache_key] = {
                'title': title,
                'api_response_type': 'not_found',
                'enriched_at': datetime.utcnow().isoformat(),
            }
            self._save_cache()
            return None
        
        # success - parse and cache
        metadata = self._parse_response(response_data)
        self._cache[cache_key] = asdict(metadata)
        self._save_cache()

        return metadata

    def _call_api(self, title: str, year: int | None = None) -> dict | None:
        """Make API calls with retry logic"""

        if self._api_calls_made >= self.requests_per_day:
            logger.warning(f"Daily API limit reached ({self.requests_per_day})")
            return None

        params = {
            'apikey': self.api_key,
            't': title,
            'type': 'movie',
            'plot': 'short',
        }
        if year:
            params['y'] = year
        
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = requests.get(
                    self.BASE_URL,
                    params=params,
                    timeout=10,
                )
                self._api_calls_made += 1

                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt}/{self.retry_attempts}: {title}")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error on attempt {attempt}/{self.retry_attempts}: {e}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt}/{self.retry_attempts}: {e}")

            if attempt < self.retry_attempts:
                time.sleep(self.retry_delay * attempt) # exponential backoff

        logger.error(f"All {self.retry_attempts} attempts failed for: {title}")
        return None

    def _parse_response(self, data: dict) -> MovieMetadata:
        """Parse OMDb API response into MovieMetadata."""
        return MovieMetadata(
            title=data.get('Title', ''),
            year=data.get('Year'),
            rated=data.get('Rated') if data.get('Rated') != 'N/A' else None,
            released=data.get('Released') if data.get('Released') != 'N/A' else None,
            runtime=data.get('Runtime') if data.get('Runtime') != 'N/A' else None,
            genre=data.get('Genre') if data.get('Genre') != 'N/A' else None,
            director=data.get('Director') if data.get('Director') != 'N/A' else None,
            actors=data.get('Actors') if data.get('Actors') != 'N/A' else None,
            plot=data.get('Plot') if data.get('Plot') != 'N/A' else None,
            language=data.get('Language') if data.get('Language') != 'N/A' else None,
            country=data.get('Country') if data.get('Country') != 'N/A' else None,
            awards=data.get('Awards') if data.get('Awards') != 'N/A' else None,
            poster_url=data.get('Poster') if data.get('Poster') != 'N/A' else None,
            metascore=self._parse_int(data.get('Metascore')),
            imdb_rating=self._parse_float(data.get('imdbRating')),
            imdb_votes=self._parse_int(data.get('imdbVotes', '').replace(',', '')),
            imdb_id=data.get('imdbID'),
            box_office=data.get('BoxOffice') if data.get('BoxOffice') != 'N/A' else None,
            enriched_at=datetime.utcnow().isoformat(),
            api_response_type='match',
        )

    def _dict_to_metadata(self, data: dict) -> MovieMetadata:
        """Convert cached dict back to MovieMetadata."""
        return MovieMetadata(**data)

    @staticmethod
    def _parse_int(value: str | None) -> int | None:
        """Safely parse string to int."""
        if not value or value == 'N/A':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _parse_float(value: str | None) -> float | None:
        """Safely parse string to float."""
        if not value or value == 'N/A':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def get_stats(self) -> dict:
        """Return cache and API usage statistics."""
        cached_matches = sum(1 for v in self._cache.values() if v.get('api_response_type') == 'match')
        cached_not_found = sum(1 for v in self._cache.values() if v.get('api_response_type') == 'not_found')
        cached_errors = sum(1 for v in self._cache.values() if v.get('api_response_type') == 'error')
        
        return {
            'total_cached': len(self._cache),
            'cached_matches': cached_matches,
            'cached_not_found': cached_not_found,
            'cached_errors': cached_errors,
            'api_calls_this_session': self._api_calls_made,
            'api_calls_remaining': self.requests_per_day - self._api_calls_made,
        }

def enrich_movies(
    client: OMDbClient,
    movies: list[dict],
    progress_interval: int = 50,
) -> list[MovieMetadata]:
    """
    Enrich a list of movies using the OMDb client.
    
    Args:
        client: OMDbClient instance
        movies: List of dicts with at least 'title' key (from csv_parser.get_unique_movies_by_revenue)
        progress_interval: Log progress every N movies
        
    Returns:
        List of MovieMetadata for successfully enriched movies
    """
    enriched = []
    
    for i, movie in enumerate(movies, start=1):
        title = movie['title']
        
        # Try to extract year from title if present (e.g., "The Polar Express2017 IMAX Release")
        year = _extract_year_from_title(title)
        
        metadata = client.get_movie(title, year=year)
        
        if metadata:
            enriched.append(metadata)
        
        if i % progress_interval == 0:
            stats = client.get_stats()
            logger.info(
                f"Progress: {i}/{len(movies)} | "
                f"Enriched: {len(enriched)} | "
                f"API calls remaining: {stats['api_calls_remaining']}"
            )
    
    logger.info(f"Enrichment complete: {len(enriched)}/{len(movies)} movies matched")
    return enriched


def _extract_year_from_title(title: str) -> int | None:
    """
    Attempt to extract a 4-digit year from title.
    
    Handles cases like "The Polar Express2017 IMAX Release"
    """
    import re
    # Look for 4-digit year between 1900-2030
    match = re.search(r'(19\d{2}|20[0-2]\d)', title)
    if match:
        return int(match.group(1))
    return None