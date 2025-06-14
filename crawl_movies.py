#!/usr/bin/env python3

import os
import time
import json
import logging
import requests
from urllib.parse import quote
from datetime import datetime
from typing import Dict, Any
import unicodedata
from upstash_redis import Redis

# Only show INFO logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Env config
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
API_URL = "https://phimapi.com/danh-sach/phim-moi-cap-nhat"
LIMIT = 3
PRECACHE_KEY_SET = "movieapp:precached_keys"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36"
MAX_EPISODES = 20

# Validate env
if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
    logger.error("UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN must be set")
    exit(1)

# Redis init
redis_client = Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)

# Test Redis
try:
    redis_client.set("test_key", json.dumps({"test": "value"}), ex=60)
    value = redis_client.get("test_key")
    if value != json.dumps({"test": "value"}):
        logger.error("Redis test failed")
        exit(1)
    logger.info("Redis connection test successful")
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    exit(1)

# Local cache to store pre-cached data
local_cache = {}

def load_precache_to_local():
    """Tải tất cả dữ liệu pre-cached từ Redis vào local_cache một lần."""
    try:
        # Lấy tất cả các khóa trong PRECACHE_KEY_SET
        cache_keys = redis_client.smembers(PRECACHE_KEY_SET)
        if not cache_keys:
            logger.info("No pre-cached keys found in Redis")
            return

        # Tải dữ liệu cho từng khóa
        for key in cache_keys:
            try:
                data = redis_client.get(key)
                if data:
                    local_cache[key] = data
            except Exception as e:
                logger.error(f"Failed to load key {key}: {e}")
        logger.info(f"Loaded {len(local_cache)} pre-cached items into local memory")
    except Exception as e:
        logger.error(f"Failed to load pre-cache: {e}")

def validate_movie_data(movie: Dict[str, Any]) -> bool:
    return bool(
        movie.get("_id") and movie.get("name") and movie.get("slug") and movie.get("content") and
        isinstance(movie.get("category", []), list) and
        isinstance(movie.get("country", []), list) and
        (movie.get("poster_url") or movie.get("thumb_url"))
    )

def compare_json(new_json: str, old_json: str) -> bool:
    try:
        new_normalized = json.dumps(json.loads(new_json), sort_keys=True)
        old_normalized = json.dumps(json.loads(old_json), sort_keys=True)
        return new_normalized == old_normalized
    except json.JSONDecodeError:
        return False

def sanitize_string(s: Any) -> str:
    if not isinstance(s, str):
        return str(s) if s is not None else ""
    s = unicodedata.normalize('NFC', s)
    s = s.replace('“', '"').replace('”', '"').replace('‘', "'").replace('’', "'")
    return "".join(c for c in s if c.isprintable())

def cache_movie(movie: Dict[str, Any]) -> bool:
    slug = movie.get("slug", "")
    movie_id = movie.get("_id", "")
    if not slug or not movie_id:
        return False

    cache_key = f"movieapp:movie_{slug}"
    id_to_slug_key = f"movieapp:id_to_slug_{movie_id}"

    try:
        response = requests.get(f"https://phimapi.com/phim/{quote(slug)}", headers={"User-Agent": USER_AGENT}, timeout=10)
        response.raise_for_status()
        data = response.json()
        time.sleep(2.0)
    except requests.RequestException:
        return False

    if not data.get("status", False):
        return False

    movie_data = data.get("movie", {})
    episodes_data = data.get("episodes", [])
    if not validate_movie_data(movie_data):
        return False

    for key in ["content", "name", "origin_name", "trailer_url", "filename"]:
        if key in movie_data:
            movie_data[key] = sanitize_string(movie_data[key])
        for server in episodes_data:
            for episode in server.get("server_data", []):
                if key in episode:
                    episode[key] = sanitize_string(episode[key])

    if "content" in movie_data and len(movie_data["content"]) > 1000:
        movie_data["content"] = movie_data["content"][:1000] + "..."

    full_data = {
        "status": data.get("status"),
        "msg": data.get("msg", ""),
        "movie": movie_data,
        "episodes": episodes_data
    }
    full_data_json = json.dumps(full_data, ensure_ascii=False)

    # So sánh với dữ liệu trong local_cache thay vì truy vấn Redis
    existing_data = local_cache.get(cache_key, "{}")
    if existing_data and existing_data != "{}":
        if compare_json(full_data_json, existing_data):
            logger.info(f"Skipped unchanged: {slug}")
            return True

    # Cập nhật Redis và local_cache nếu dữ liệu mới
    try:
        redis_client.set(cache_key, full_data_json)
        redis_client.sadd(PRECACHE_KEY_SET, cache_key)
        local_cache[cache_key] = full_data_json  # Cập nhật local_cache
        logger.info(f"Cached movie: {slug}")
    except Exception:
        return False

    try:
        redis_client.set(id_to_slug_key, slug)
        local_cache[id_to_slug_key] = slug  # Cập nhật local_cache
    except Exception:
        return False

    for server_index, server in enumerate(episodes_data[-MAX_EPISODES:]):
        for episode_index, episode in enumerate(server.get("server_data", [])):
            stream_id = f"{movie_id}_{server_index}_{episode_index}"
            stream_key = f"movieapp:stream_detail_{stream_id}"
            stream_data = {
                "stream_links": [{
                    "id": f"default_{stream_id}",
                    "name": episode.get("name", f"Episode {episode_index + 1}"),
                    "type": "hls",
                    "default": False,
                    "url": episode.get("link_m3u8", "")
                }]
            }
            stream_data_json = json.dumps(stream_data, ensure_ascii=False)

            # So sánh với dữ liệu trong local_cache
            existing_stream = local_cache.get(stream_key, "{}")
            if existing_stream and existing_stream != "{}":
                if compare_json(stream_data_json, existing_stream):
                    continue

            # Cập nhật Redis và local_cache
            try:
                redis_client.set(stream_key, stream_data_json)
                redis_client.sadd(PRECACHE_KEY_SET, stream_key)
                local_cache[stream_key] = stream_data_json  # Cập nhật local_cache
            except Exception:
                continue

    logger.info(f"Updated episodes for: {slug}")
    return True

def crawl_movies():
    page = 1
    logger.info(f"Start crawl at {datetime.utcnow().isoformat()}Z")

    # Tải pre-cache một lần trước khi bắt đầu crawl
    load_precache_to_local()

    while True:
        try:
            response = requests.get(f"{API_URL}?page={page}&limit={LIMIT}", headers={"User-Agent": USER_AGENT}, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException:
            break

        if not data.get("status", False):
            break

        items = data.get("items", [])
        total_pages = data.get("pagination", {}).get("totalPages", 0)

        if not items:
            break

        for item in items:
            cache_movie(item)

        if page >= total_pages:
            break

        page += 1

    logger.info(f"Crawl done at {datetime.utcnow().isoformat()}Z")

if __name__ == "__main__":
    crawl_movies()
