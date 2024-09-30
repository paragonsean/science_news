import asyncio
import re
import time
import urllib.parse
import json
from contextlib import contextmanager
from os.path import getsize
from aiohttp_client_cache import CachedSession, SQLiteBackend
import aiofiles

CACHE_NAME = 'precache'
JSON_FILE = 'urldict.json'  # JSON file containing the sciencealert URLs


async def load_json_data():
    """Load JSON data from urldictnewsurls.json asynchronously using aiofiles."""
    try:
        async with aiofiles.open(JSON_FILE, mode='r') as file:
            file_content = await file.read()
            data = json.loads(file_content)
            return data
    except FileNotFoundError:
        print(f"Error: {JSON_FILE} does not exist.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {JSON_FILE}.")
        return {}


async def get_sciencealert_urls(data):
    """Extract all 'sciencealert' URLs from the JSON data."""
    sciencealert_urls = []
    for key, entry in data.items():
        if isinstance(entry, dict) and 'sciencealert' in entry:
            sciencealert_url = entry['sciencealert']
            sciencealert_urls.append(sciencealert_url)
    return sciencealert_urls


async def precache_page_links(urls):
    """Fetch and cache the content of the ScienceAlert URLs."""
    async with CachedSession(cache=SQLiteBackend(CACHE_NAME)) as session:
        # Create tasks to cache the URLs in parallel
        tasks = [asyncio.create_task(cache_url(session, url)) for url in urls]
        responses = await asyncio.gather(*tasks)
    return responses


async def cache_url(session, url):
    """Cache the URL content using aiohttp's CachedSession."""
    try:
        print(f'Fetching and caching URL: {url}')
        response = await session.get(url)
        return response
    except Exception as e:
        print(f'Error fetching {url}: {e}')
        return None


def get_cache_bytes():
    """Get the current size of the cache, in bytes."""
    try:
        return getsize(f'{CACHE_NAME}.sqlite')
    except Exception:
        return 0


@contextmanager
def measure_cache():
    """Measure time elapsed and size of added cache content."""
    start_time = time.perf_counter()
    start_bytes = get_cache_bytes()
    yield
    elapsed_time = time.perf_counter() - start_time
    cached_bytes = (get_cache_bytes() - start_bytes) / 1024 / 1024
    print(f'Completed run in {elapsed_time:0.3f} seconds and cached {cached_bytes:0.3f} MB')


if __name__ == '__main__':
    with measure_cache():
        # Load JSON data and extract URLs
        json_data = asyncio.run(load_json_data())
        sciencealert_urls = asyncio.run(get_sciencealert_urls(json_data))

        if sciencealert_urls:
            print(f"Found {len(sciencealert_urls)} ScienceAlert URLs.")
            asyncio.run(precache_page_links(sciencealert_urls))
        else:
            print("No ScienceAlert URLs found.")
