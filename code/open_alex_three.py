import json
import aiohttp
import aiofiles
import asyncio
from urllib.parse import urlparse, urlunparse
import logging
from tqdm.asyncio import tqdm
from aiohttp_client_cache import CachedSession, SQLiteBackend
from datetime import timedelta

# Configure logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR)

# Caching configuration
default_expire_after = 60 * 60  # Default cache expiration time: 1 hour
urls_expire_after = {
    'https://api.openalex.org/works/*': timedelta(days=7),  # Requests for this URL will expire in a week
 # Requests for this pattern will never expire
}

async def follow_redirects(session, url):
    """Follow redirects to get the final URL."""
    try:
        async with session.get(url, allow_redirects=True) as response:
            final_url = str(response.url)  # Get the final URL after redirects
            return final_url
    except Exception as e:
        logging.error(f"Error following URL {url}: {e}")
        return url  # Return the original URL if there's an error

def clean_url(url):
    """Clean the URL by removing query parameters, fragments, and specific cases with urllib and custom logic."""
    # Handle specific case for psycnet
    if url.startswith("https://psycnet.apa.org/doiLanding?doi="):
        return url.split('=')[-1]  # Extract DOI part

    # Remove "/full" or "/abstract" from the URL path
    url = url.split('/full')[0]
    url = url.split('/abstract')[0]

    # Parse the URL into components
    parsed_url = urlparse(url)

    # Rebuild the URL without the query and fragment parts
    cleaned_url = urlunparse((
        parsed_url.scheme,  # Keep scheme (e.g., https)
        parsed_url.netloc,  # Keep domain (e.g., www.example.com)
        parsed_url.path,  # Keep the path (but without /full or /abstract)
        parsed_url.params,  # Keep URL parameters (if any)
        '',  # Remove query parameters (anything after '?')
        ''  # Remove fragments (anything after '#')
    ))

    return cleaned_url

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
        final_url = clean_url(final_url)  # Clean the final URL
        api_url = f"https://api.openalex.org/works/{final_url}"

    try:
        async with session.get(api_url) as response:
            if response.status == 404:
                logging.error(f"Error: Received status code 404 for {doi or url}. Resource not found.")
                return {
                    "title": "N/A",
                    "first_author": "N/A",
                    "authors": "N/A",
                    "year": "N/A",
                    "doi": "N/A",
                    "doi_url": "N/A",
                    "journal_or_institution": "N/A",
                    "pages": "N/A",
                    "volume": "N/A",
                    "number": "N/A"
                }
            if response.status != 200:
                logging.error(f"Error: Received status code {response.status} for {doi or url}.")
                return {
                    "title": "N/A",
                    "first_author": "N/A",
                    "authors": "N/A",
                    "year": "N/A",
                    "doi": "N/A",
                    "doi_url": "N/A",
                    "journal_or_institution": "N/A",
                    "pages": "N/A",
                    "volume": "N/A",
                    "number": "N/A"
                }
            metadata = await response.json()

        # Extract metadata for DOI and title
        doi = metadata.get("doi", None)
        if doi:
            doi = doi[len("https://doi.org/"):]  # Strip the DOI URL prefix
        title = metadata.get("display_name", "No Title Available")

        return {
            "title": title,
            "first_author": metadata["authorships"][0]["author"]["display_name"] if metadata.get(
                "authorships") else "N/A",
            "authors": ", ".join([author["author"]["display_name"] for author in metadata.get("authorships", [])]) or "N/A",
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
        return {
            "title": "N/A",
            "first_author": "N/A",
            "authors": "N/A",
            "year": "N/A",
            "doi": "N/A",
            "doi_url": "N/A",
            "journal_or_institution": "N/A",
            "pages": "N/A",
            "volume": "N/A",
            "number": "N/A"
        }

async def process_article(article, session):
    """Process each article and fetch metadata for each paperlink and DOI URL."""
    processed_papers = []

    # Process paper links
    for paperlink_info in article.get('paperlinks', []):
        try:
            doi = paperlink_info.get('doi', None)
            url = paperlink_info.get('paperlink', None)

            paper_data = await get_paper_metadata(session, doi=doi, url=url)
            paper_data['source_article_title'] = article['title']

            processed_papers.append(paper_data)

        except Exception as e:
            logging.error(f"Error processing {url or doi}: {e}")
            processed_papers.append({
                "url": url or "N/A",
                "doi": doi or "N/A",
                "title": "N/A",
                "first_author": "N/A",
                "authors": "N/A",
                "year": "N/A",
                "doi_url": "N/A",
                "journal_or_institution": "N/A",
                "pages": "N/A",
                "volume": "N/A",
                "number": "N/A",
                "source_article_title": article.get('title', "N/A"),
                "message": f"Could not find metadata for {doi or url}"
            })

    # Process DOI URLs
    for doi_url in article.get('doi_urls', []):
        try:
            final_url = await follow_redirects(session, doi_url)
            doi = final_url.split('/')[-1]  # Extract DOI from URL
            paper_data = await get_paper_metadata(session, doi=doi)
            paper_data['source_article_title'] = article['title']
            paper_data['doi_url'] = doi_url

            processed_papers.append(paper_data)

        except Exception as e:
            logging.error(f"Error processing DOI URL {doi_url}: {e}")
            processed_papers.append({
                "doi_url": doi_url,
                "title": "N/A",
                "first_author": "N/A",
                "authors": "N/A",
                "year": "N/A",
                "doi": "N/A",
                "doi_url": doi_url,
                "journal_or_institution": "N/A",
                "pages": "N/A",
                "volume": "N/A",
                "number": "N/A",
                "source_article_title": article.get('title', "N/A"),
                "message": f"Could not find metadata for DOI URL {doi_url}"
            })

    return processed_papers

async def clean_urls_in_article(articles):
    """Clean the URLs for all articles in the given list."""
    for article in articles:
        cleaned_paperlinks = [clean_url(link['paperlink']) for link in article.get('paperlinks', [])]
        for idx, link in enumerate(article['paperlinks']):
            link['paperlink'] = cleaned_paperlinks[idx]

async def process_all_articles(articleinfos_path, output_path):
    """Open articleinfos.json, clean URLs, process each article, and save results to a new JSON file."""
    print(f"Opening file: {articleinfos_path}")
    async with aiofiles.open(articleinfos_path, 'r') as f:
        articles = json.loads(await f.read())

    # Clean URLs in articles
    print("Cleaning URLs in articles...")
    await clean_urls_in_article(articles)

    # Setup caching and session
    print("Setting up cache and session...")
    cache_backend = SQLiteBackend('cache.db', expire_after=default_expire_after)
    async with CachedSession(cache=cache_backend, expire_after=default_expire_after) as session:
        tasks = [process_article(article, session) for article in articles]
        print("Processing articles...")
        all_papers = await asyncio.gather(*tasks)

    # Save processed papers to output file
    print(f"Saving processed papers to: {output_path}")
    async with aiofiles.open(output_path, 'w') as f:
        await f.write(json.dumps(all_papers, indent=4))

    print("Processing complete.")


# Example usage
if __name__ == '__main__':
    # File paths
    articleinfos_path = 'urldictcleandoistwo.json'
    output_path = 'processed_papers_two.json'

    # Run the processing
    asyncio.run(process_all_articles(articleinfos_path, output_path))
