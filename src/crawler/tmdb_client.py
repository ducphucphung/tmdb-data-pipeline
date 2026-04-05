import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Set, Optional


BASE_URL = "https://api.themoviedb.org/3"

REQUEST_SLEEP_SECONDS = 0.2

class TMDBClient:
    def __init__(self, read_token: str):
        if not read_token:
            raise ValueError("Please enter TMDB Token")

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {read_token}",
            "Accept": "application/json",
        })

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        url = f"{BASE_URL}{endpoint}"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        time.sleep(REQUEST_SLEEP_SECONDS)
        return response.json()

    def discover_movies(
        self,
        release_date_gte: str,
        release_date_lte: str,
        page: int = 1,
    ) -> Dict:
        params = {
            "language": "en-US",
            "sort_by": "popularity.desc",
            "include_adult": "false",
            "include_video": "false",
            "primary_release_date.gte": release_date_gte,
            "primary_release_date.lte": release_date_lte,
            "sort_by": "primary_release_date.asc",
            "page": page,
        }
        return self.get("/discover/movie", params=params)

    def get_movie_details(self, movie_id: int) -> Dict:
        params = {
            "append_to_response": "credits,keywords,release_dates,videos"
        }
        return self.get(f"/movie/{movie_id}", params=params)