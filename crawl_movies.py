#!/usr/bin/env python3

import os
import time
import json
import logging
import requests
from urllib.parse import quote
from datetime import datetime
from typing import Dict, Any, Union
import unicodedata
from upstash_redis import Redis

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO, # Cấp độ log mặc định cho các log của bạn
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Tắt hoặc giới hạn log của thư viện requests và urllib3
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Environment Variables ---
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

# --- API Configuration ---
API_BASE_URL = "https://phimapi.com"
NEW_MOVIES_API_ENDPOINT = f"{API_BASE_URL}/danh-sach/phim-moi-cap-nhat"
DETAIL_MOVIE_API_ENDPOINT = f"{API_BASE_URL}/phim" # Will append /{slug}
LIMIT_PER_PAGE = 3 # Number of movies to fetch per page in the crawl (for API calls)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36"

# --- Redis Cache Configuration ---
# Match the cache key prefix from api/movie.js
MOVIE_DETAIL_CACHE_PREFIX = "movie:" 
# CACHE_TTL_SECONDS is no longer needed as cache is permanent

# --- Data Constraints ---
MAX_CONTENT_LENGTH = 1000 # Max length for movie content in cache before truncation
REQUEST_TIMEOUT = 10 # Seconds for API requests

# --- Validate Environment Variables ---
if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
    logger.error("UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN must be set.")
    exit(1)

# --- Redis Initialization ---
redis_client = Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)

# --- Test Redis Connection ---
try:
    test_value = {"test": "value"}
    redis_client.set("test_key", test_value, ex=60) 
    
    retrieved_raw_value = redis_client.get("test_key")
    retrieved_value = json.loads(retrieved_raw_value) if isinstance(retrieved_raw_value, str) else retrieved_raw_value

    if retrieved_value != test_value:
        logger.error(f"Redis test failed: Expected {test_value}, got {retrieved_value}")
        exit(1)
    logger.info("Redis connection test successful.")
    redis_client.delete("test_key") # Clean up test key
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    exit(1)

# --- Helper Functions ---
def validate_movie_data(movie: Dict[str, Any]) -> bool:
    """Validates essential fields for a movie object."""
    return bool(
        movie.get("_id") and movie.get("name") and movie.get("slug") and movie.get("content") and
        isinstance(movie.get("category", []), list) and
        isinstance(movie.get("country", []), list) and
        (movie.get("poster_url") or movie.get("thumb_url"))
    )

def compare_objects(new_obj: Dict, old_obj: Dict) -> bool:
    """Compares two Python dictionaries for equality, after converting to JSON and back."""
    try:
        new_normalized = json.dumps(new_obj, sort_keys=True, ensure_ascii=False)
        old_normalized = json.dumps(old_obj, sort_keys=True, ensure_ascii=False)
        return new_normalized == old_normalized
    except (TypeError, json.JSONDecodeError) as e:
        logger.error(f"Error comparing objects: {e}. New: {new_obj}, Old: {old_obj}")
        return False

def sanitize_string(s: Any) -> str:
    """Sanitizes strings to be printable and consistent."""
    if not isinstance(s, str):
        return str(s) if s is not None else ""
    s = unicodedata.normalize('NFC', s)
    s = s.replace('“', '"').replace('”', '"').replace('‘', "'").replace('’', "'")
    return "".join(c for c in s if c.isprintable())

def cache_movie(movie_item: Dict[str, Any]) -> str:
    """
    Fetches movie detail and caches it in Redis.
    Returns "skipped" if unchanged, "cached" if updated/new, "failed" otherwise.
    """
    slug = movie_item.get("slug", "")

    if not slug:
        logger.warning(f"Invalid movie item (missing slug): {movie_item}")
        return "failed"

    cache_key = f"{MOVIE_DETAIL_CACHE_PREFIX}{slug}"

    try:
        response = requests.get(
            f"{DETAIL_MOVIE_API_ENDPOINT}/{quote(slug)}",
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status() 
        api_data = response.json()
        time.sleep(0.5) 
    except requests.RequestException as e:
        logger.error(f"Failed to fetch detail for {slug}: {e}")
        return "failed"

    if not api_data.get("status", False):
        logger.warning(f"API returned status false for {slug}: {api_data.get('msg')}")
        return "failed"

    movie_data = api_data.get("movie", {})
    episodes_data = api_data.get("episodes", [])

    if not validate_movie_data(movie_data):
        logger.warning(f"Invalid movie data from API for {slug}")
        return "failed"

    for key in ["content", "name", "origin_name", "trailer_url"]:
        if key in movie_data:
            movie_data[key] = sanitize_string(movie_data[key])
    if "content" in movie_data and len(movie_data["content"]) > MAX_CONTENT_LENGTH:
        movie_data["content"] = movie_data["content"][:MAX_CONTENT_LENGTH] + "..."

    for server in episodes_data:
        for episode in server.get("server_data", []):
            if "filename" in episode:
                episode["filename"] = sanitize_string(episode["filename"])
            if "name" in episode:
                episode["name"] = sanitize_string(episode["name"])

    full_data_to_cache = {
        "status": api_data.get("status"),
        "msg": api_data.get("msg", ""),
        "movie": movie_data,
        "episodes": episodes_data
    }

    try:
        existing_raw_data = redis_client.get(cache_key)
        existing_data = None
        if isinstance(existing_raw_data, str):
            try:
                existing_data = json.loads(existing_raw_data)
            except json.JSONDecodeError:
                logger.warning(f"Corrupted JSON in cache for {slug}. Fetching new data.")
                existing_data = None

        if existing_data and compare_objects(full_data_to_cache, existing_data):
            logger.info(f"Skipped unchanged movie: {slug}")
            return "skipped"

        redis_client.set(cache_key, full_data_to_cache)
        logger.info(f"Cached movie: {slug} (updated/new) - Permanent.")

    except Exception as e:
        logger.error(f"Error caching movie {slug}: {e}")
        return "failed"

    return "cached"

def crawl_movies():
    """Crawls new movies API and updates cache."""
    page = 1
    logger.info(f"Starting movie cache update at {datetime.utcnow().isoformat()}Z")

    while True:
        try:
            logger.info(f"Fetching page {page} of new movies (limit: {LIMIT_PER_PAGE})...")
            response = requests.get(
                f"{NEW_MOVIES_API_ENDPOINT}?page={page}&limit={LIMIT_PER_PAGE}",
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            api_data = response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch new movies page {page}: {e}")
            break # Exit loop on request error

        if not api_data.get("status", False):
            logger.warning(f"API returned status false for page {page}: {api_data.get('msg')}")
            break # Exit loop if API reports error

        items = api_data.get("items", [])
        total_pages = api_data.get("pagination", {}).get("totalPages", 0)

        if not items:
            logger.info(f"No more items on page {page}. Stopping.")
            break # Exit loop if no items found

        # --- ĐIỀU CHỈNH QUAN TRỌNG TẠI ĐÂY ---
        for item in items:
            result = cache_movie(item)
            if result == "skipped":
                logger.info(f"Encountered a skipped movie ({item.get('slug')}). Stopping script early.")
                return # Dừng script ngay lập tức
            
            # Một độ trễ nhỏ giữa xử lý mỗi bộ phim để giảm tải cho API
            time.sleep(0.1)

        # Logic này không cần thiết nữa vì chúng ta đã dừng ngay lập tức
        # if page >= total_pages:
        #     logger.info(f"Reached last page ({total_pages}). Stopping.")
        #     break

        page += 1
        time.sleep(1.0) # Độ trễ trước khi tìm nạp trang tiếp theo (nếu script chưa dừng)

    logger.info(f"Movie cache update completed at {datetime.utcnow().isoformat()}Z")

if __name__ == "__main__":
    crawl_movies()
