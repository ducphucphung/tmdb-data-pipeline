import os
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Dict, List, Set, Optional

from dotenv import load_dotenv

import requests

from tmdb_client import TMDBClient

load_dotenv()

TMDB_READ_TOKEN = os.getenv("TMDB_READ_TOKEN")
BASE_URL = "https://api.themoviedb.org/3"

# Raw output root
RAW_ROOT = Path("data/raw")

# Persistent file storing all tracked movie IDs
TRACKED_IDS_FILE = RAW_ROOT / "state" / "tracked_movie_ids.json"

# How many recent days to scan for newly released movies
DISCOVERY_LOOKBACK_DAYS = 1

# Small pause to be polite with the API
REQUEST_SLEEP_SECONDS = 0.2


def ensure_dirs() -> None:
    (RAW_ROOT / "discover_movies").mkdir(parents=True, exist_ok=True)
    (RAW_ROOT / "movie_details").mkdir(parents=True, exist_ok=True)
    (RAW_ROOT / "state").mkdir(parents=True, exist_ok=True)


def load_tracked_movie_ids() -> Set[int]:
    if not TRACKED_IDS_FILE.exists():
        return set()

    with open(TRACKED_IDS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    return {int(x) for x in data}


def save_tracked_movie_ids(movie_ids: Set[int]) -> None:
    TRACKED_IDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TRACKED_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(movie_ids), f, indent=2)


def write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def daterange_for_daily_discovery(lookback_days: int) -> tuple[str, str]:
    today = date.today()
    start_date = today - timedelta(days=lookback_days)
    return start_date.isoformat(), today.isoformat()


def discover_new_movie_ids(
    client: TMDBClient,
    release_date_gte: str,
    release_date_lte: str,
    snapshot_date: str,
) -> Set[int]:
    discovered_ids: Set[int] = set()

    first_page = client.discover_movies(
        release_date_gte=release_date_gte,
        release_date_lte=release_date_lte,
        page=1,
    )

    total_pages = first_page.get("total_pages", 1)

    first_page_path = (
        RAW_ROOT
        / "discover_movies"
        / f"snapshot_date={snapshot_date}"
        / f"window={release_date_gte}_{release_date_lte}"
        / "page=1.json"
    )
    write_json(first_page_path, first_page)

    for movie in first_page.get("results", []):
        movie_id = movie.get("id")
        discovered_ids.add(int(movie_id))

    for page in range(2, total_pages + 1):
        payload = client.discover_movies(
            release_date_gte=release_date_gte,
            release_date_lte=release_date_lte,
            page=page,
        )

        page_path = (
            RAW_ROOT
            / "discover_movies"
            / f"snapshot_date={snapshot_date}"
            / f"window={release_date_gte}_{release_date_lte}"
            / f"page={page}.json"
        )
        write_json(page_path, payload)

        for movie in payload.get("results", []):
            movie_id = movie.get("id")
            discovered_ids.add(int(movie_id))

    return discovered_ids


def fetch_and_store_movie_snapshot(
    client: TMDBClient,
    movie_id: int,
    snapshot_date: str,
) -> None:
    details = client.get_movie_details(movie_id)

    output_path = (
        RAW_ROOT
        / "movie_details"
        / f"snapshot_date={snapshot_date}"
        / f"movie_id={movie_id}.json"
    )
    write_json(output_path, details)


def main() -> None:
    ensure_dirs()

    client = TMDBClient(TMDB_READ_TOKEN)
    snapshot_date = date.today().isoformat()

    print(f"Running daily TMDB incremental crawler for snapshot_date={snapshot_date}")

    # Load the current tracked universe
    tracked_movie_ids = load_tracked_movie_ids()
    print(f"Loaded {len(tracked_movie_ids)} existing movie IDs")

    # 1) Discover new movies from a recent release-date window
    release_date_gte, release_date_lte = daterange_for_daily_discovery(DISCOVERY_LOOKBACK_DAYS)
    print(f"Discovering new movies between {release_date_gte} and {release_date_lte}")

    new_movie_ids = discover_new_movie_ids(
        client=client,
        release_date_gte=release_date_gte,
        release_date_lte=release_date_lte,
        snapshot_date=snapshot_date,
    )

    truly_new_ids = new_movie_ids - tracked_movie_ids
    print(f"Discovered {len(new_movie_ids)} movies in window")
    print(f"Newly added movies: {len(truly_new_ids)}")

    # Add newly discovered IDs into tracked list
    tracked_movie_ids.update(new_movie_ids)

    # 2) Refresh snapshots for all tracked movies
    # This gives you daily performance tracking over time
    print(f"Fetching daily snapshots for {len(tracked_movie_ids)} tracked movies")

    success_count = 0
    fail_count = 0

    for movie_id in sorted(tracked_movie_ids):
        try:
            fetch_and_store_movie_snapshot(
                client=client,
                movie_id=movie_id,
                snapshot_date=snapshot_date,
            )
            success_count += 1
        except requests.HTTPError as e:
            fail_count += 1
            print(f"[ERROR] movie_id={movie_id} failed: {e}")

    # Save updated state
    save_tracked_movie_ids(tracked_movie_ids)

    print("Done.")
    print(f"Successful snapshots: {success_count}")
    print(f"Failed snapshots: {fail_count}")
    print(f"Tracked movie universe size: {len(tracked_movie_ids)}")


if __name__ == "__main__":
    main()