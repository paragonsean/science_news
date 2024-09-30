import json
import aiohttp
import aiofiles
import asyncio
from urllib.parse import urlparse, urlunparse
import logging

# Configure logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR)


async def get_paper_metadata(doi=None, url=None):
    """Fetch metadata of a paper from OpenAlex API using DOI or URL asynchronously."""
    if not any([doi, url]):
        raise ValueError("At least one of 'doi' or 'url' must be provided.")

    async with aiohttp.ClientSession() as session:
        if doi:
            # Strip the "https://doi.org/" from the DOI if it exists
            doi = doi.replace("https://doi.org/", "")
            api_url = f"https://api.openalex.org/works/doi:{doi}"
        elif url:
            url = clean_url(url)  # Clean the URL by removing query parameters and fragments
            api_url = f"https://api.openalex.org/works/{url}"

        async with session.get(api_url) as response:
            if response.status == 404:
                logging.error(f"Error: Received status code 404 for {url}. Resource not found.")
                raise Exception(f"Error: Received status code 404 for {doi or url}.")
            if response.status != 200:
                logging.error(f"Error: Received status code {response.status} for {url}.")
                raise Exception(f"Error: Received status code {response.status} for {doi or url}.")
            metadata = await response.json()

    # Extract metadata for DOI and title
    doi = metadata.get("doi", None)
    if doi:
        doi = doi[len("https://doi.org/"):]  # Strip the DOI URL prefix
    title = metadata.get("display_name", "No Title Available")

    print(f"Found paper: {title}")
    return {
        "title": metadata.get("display_name", "No Title Available"),
        "first_author": metadata["authorships"][0]["author"]["display_name"] if metadata.get(
            "authorships") else "No Author",
        "authors": ", ".join([author["author"]["display_name"] for author in metadata.get("authorships", [])]),
        "year": metadata.get("publication_year", "Unknown Year"),
        "doi": doi,
        "doi_url": metadata.get("doi", "No DOI available"),
        "journal_or_institution": metadata.get("host_venue", {}).get("display_name", "Unknown Institution"),
        "pages": f"{metadata['biblio'].get('first_page', '')}-{metadata['biblio'].get('last_page', '')}",
        "volume": metadata["biblio"].get("volume", ""),
        "number": metadata["biblio"].get("issue", "")
    }


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


async def process_article(article):
    """Process each article and fetch metadata for each paperlink."""
    processed_papers = []
    for paperlink in article['paperlinks']:
        try:
            # Extract DOI from the paperlink and fetch metadata
            paper_data = await get_paper_metadata(url=paperlink)
            paper_data['source_article_title'] = article['title']
            processed_papers.append(paper_data)
        except Exception as e:
            print(f"Error processing {paperlink}: {e}")
            processed_papers.append({
                "url": paperlink,
                "error": str(e)
            })
    return processed_papers


async def clean_urls_in_article(articles):
    """Clean the URLs for all articles in the given list."""
    for article in articles:
        cleaned_paperlinks = [clean_url(link) for link in article.get('paperlinks', [])]
        article['paperlinks'] = cleaned_paperlinks  # Replace with cleaned URLs


async def process_all_articles(articleinfos_path, output_path):
    """Open articleinfos.json, clean URLs, process each article, and save results to a new JSON file."""
    # Open and load articleinfos.json using aiofiles
    async with aiofiles.open(articleinfos_path, 'r') as f:
        content = await f.read()
        articles = json.loads(content)

    # Clean the URLs in the articles before processing
    await clean_urls_in_article(articles)

    all_processed_papers = []

    # Process each article
    for article in articles:
        processed_papers = await process_article(article)
        all_processed_papers.extend(processed_papers)

    # Save the result to a new JSON file using aiofiles
    async with aiofiles.open(output_path, 'w') as f:
        await f.write(json.dumps(all_processed_papers, indent=4))

    print(f"Paper metadata saved to {output_path}")


# Run the main function with asyncio
if __name__ == "__main__":
    asyncio.run(process_all_articles('cleaned_articleinfos.json', 'processed_papers.json'))
