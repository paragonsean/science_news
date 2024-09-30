import json
import aiohttp
import aiofiles
import asyncio
import logging
import os

# Configure logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR)

# Base directory for JSON files
BASE_JSON_DIR = 'JSON/'
CACHE_FILE = os.path.join(BASE_JSON_DIR, 'processed_papers.json')
DOI_TRACKING_FILE = os.path.join(BASE_JSON_DIR, 'doi_tracking.json')

doi_tracking = {}
cache_data = []

async def follow_redirects(session, url):
    """Follow redirects to get the final URL."""
    try:
        async with session.get(url, allow_redirects=True) as response:
            final_url = str(response.url)  # Get the final URL after redirects
            return final_url
    except Exception as e:
        logging.error(f"Error following URL {url}: {e}")
        return url  # Return the original URL if there's an error


async def get_paper_metadata(session, doi=None, url=None):
    """Fetch metadata of a paper from OpenAlex API using DOI or URL asynchronously."""
    if not any([doi, url]):
        raise ValueError("At least one of 'doi' or 'url' must be provided.")

    if doi:
        # Strip the "https://doi.org/" from the DOI if it exists
        doi = doi.replace("https://doi.org/", "")
        api_url = f"https://api.openalex.org/works/doi:{doi}"
    elif url:
        # Follow the URL to get the final URL after redirects
        final_url = await follow_redirects(session, url)
        api_url = f"https://api.openalex.org/works/{final_url}"

    try:
        async with session.get(api_url) as response:
            if response.status == 404:
                logging.error(f"Error: Received status code 404 for {doi or url}. Resource not found.")
                return default_paper_metadata(doi, url)
            if response.status != 200:
                logging.error(f"Error: Received status code {response.status} for {doi or url}.")
                return default_paper_metadata(doi, url)
            metadata = await response.json()

        doi = metadata.get("doi", None)
        if doi:
            doi = doi[len("https://doi.org/"):]  # Strip the DOI URL prefix

        title = metadata.get("display_name", "No Title Available")
        return {
            "title": title,
            "first_author": metadata["authorships"][0]["author"]["display_name"] if metadata.get(
                "authorships") else "N/A",
            "authors": ", ".join(
                [author["author"]["display_name"] for author in metadata.get("authorships", [])]) or "N/A",
            "year": metadata.get("publication_year", "N/A"),
            "doi": doi if doi else "N/A",
            "doi_url": metadata.get("doi", "N/A"),
            "journal_or_institution": metadata.get("host_venue", {}).get("display_name", "N/A"),
            "pages": f"{metadata['biblio'].get('first_page', 'N/A')}-{metadata['biblio'].get('last_page', 'N/A')}",
            "volume": metadata["biblio"].get("volume", "N/A"),
            "number": metadata["biblio"].get("issue", "N/A")
        }
    except Exception as e:
        logging.error(f"Error fetching metadata for {doi or url}: {e}")
        return default_paper_metadata(doi, url)


def default_paper_metadata(doi=None, url=None):
    """Return default metadata in case of errors or 404."""
    return {
        "title": "N/A",
        "first_author": "N/A",
        "authors": "N/A",
        "year": "N/A",
        "doi": doi or "N/A",
        "doi_url": url or "N/A",
        "journal_or_institution": "N/A",
        "pages": "N/A",
        "volume": "N/A",
        "number": "N/A"
    }


async def process_article(article, session):
    """Process each article and fetch metadata for each paperlink and DOI URL."""
    processed_papers = []

    for paperlink_info in article.get('paperlinks', []):
        try:
            doi = paperlink_info.get('doi', None)
            url = paperlink_info.get('paperlink', None)

            # Check cache before making API requests
            cached_data = check_cache(doi, url)
            if cached_data:
                processed_papers.append(cached_data)
            else:
                paper_data = await get_paper_metadata(session, doi=doi, url=url)
                paper_data['source_article_title'] = article['title']

                # Update cache
                update_cache(paper_data)
                processed_papers.append(paper_data)

            # Track DOIs across articles
            track_doi(doi, url, article['index'])

        except Exception as e:
            logging.error(f"Error processing {url or doi}: {e}")
            processed_papers.append(default_paper_metadata(doi, url))

    return processed_papers


def check_cache(doi=None, url=None):
    """Check if metadata for a given DOI or URL is already in the cache."""
    for paper in cache_data:
        if paper['doi'] == doi or paper['doi_url'] == url:
            return paper
    return None


def update_cache(new_data):
    """Update the cache with new metadata."""
    cache_data.append(new_data)

    with open(CACHE_FILE, 'w') as f:
        json.dump(cache_data, f, indent=4)


def track_doi(doi, url, article_index):
    """Track all the URLs and indices that point to the same DOI."""
    if doi not in doi_tracking:
        doi_tracking[doi] = {
            'urls': [],
            'indices': []
        }

    if url not in doi_tracking[doi]['urls']:
        doi_tracking[doi]['urls'].append(url)

    if article_index not in doi_tracking[doi]['indices']:
        doi_tracking[doi]['indices'].append(article_index)


async def process_all_articles(articleinfos_path, output_path):
    """Open articleinfos.json, process each article, and save results to a new JSON file."""
    print(f"Opening file: {articleinfos_path}")
    async with aiofiles.open(articleinfos_path, 'r') as f:
        articles = json.loads(await f.read())

    # Load the cache data from processed_papers.json
    load_cache()

    print("Setting up session...")
    async with aiohttp.ClientSession() as session:
        tasks = [process_article(article, session) for article in articles]
        print("Processing articles...")
        all_papers = await asyncio.gather(*tasks)

    print(f"Saving processed papers to: {output_path}")
    async with aiofiles.open(output_path, 'w') as f:
        await f.write(json.dumps(all_papers, indent=4))

    print("Processing complete.")

    # After processing, save DOI tracking info
    save_doi_tracking()


def load_cache():
    """Load cache from the processed_papers.json file."""
    global cache_data
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            try:
                cache_data = json.load(f)
            except json.JSONDecodeError:
                cache_data = []


def save_doi_tracking():
    """Save the DOI tracking information to a file."""
    os.makedirs(os.path.dirname(DOI_TRACKING_FILE), exist_ok=True)

    with open(DOI_TRACKING_FILE, 'w') as f:
        json.dump(doi_tracking, f, indent=4)

    print(f"DOI tracking saved to {DOI_TRACKING_FILE}")


# Example usage
if __name__ == '__main__':
    # File paths within the JSON directory
    articleinfos_path = os.path.join(BASE_JSON_DIR, 'urldictcleandoistwo.json')
    output_path = os.path.join(BASE_JSON_DIR, 'processed_papers_two.json')

    # Run the processing
    asyncio.run(process_all_articles(articleinfos_path, output_path))
