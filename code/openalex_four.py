import json
import aiohttp
import aiofiles
import asyncio
from urllib.parse import urlparse, urlunparse
import logging
from tqdm.asyncio import tqdm

# Configure logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR)

async def follow_redirects(url):
    """Follow redirects to get the final URL."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, allow_redirects=True) as response:
                final_url = str(response.url)  # Get the final URL after redirects
                return final_url
        except Exception as e:
            logging.error(f"Error following URL {url}: {e}")
            return url  # Return the original URL if there's an error

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
            # Follow the URL to get the final URL after redirects
            final_url = await follow_redirects(url)
            final_url = clean_url(final_url)  # Clean the final URL
            api_url = f"https://api.openalex.org/works/{final_url}"

        async with session.get(api_url) as response:
            if response.status == 404:
                logging.error(f"Error: Received status code 404 for {doi or url}. Resource not found.")
                return {"error": f"Could not find metadata for {doi or url}"}
            if response.status != 200:
                logging.error(f"Error: Received status code {response.status} for {doi or url}.")
                return {"error": f"Error: Received status code {response.status} for {doi or url}"}
            metadata = await response.json()

    # Extract metadata for DOI and title
    doi = metadata.get("doi", None)
    if doi:
        doi = doi[len("https://doi.org/"):]  # Strip the DOI URL prefix
    title = metadata.get("display_name", "No Title Available")

    return {
        "title": title,
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



async def process_article(article):
    """Process each article and fetch metadata for each paperlink."""
    processed_papers = []
    for paperlink_info in article['paperlinks']:
        try:
            doi = paperlink_info.get('doi', None)
            url = paperlink_info.get('paperlink', None)

            paper_data = await get_paper_metadata(doi=doi, url=url)

            paper_data['source_article_title'] = article['title']

            if "error" in paper_data:
                paper_data['message'] = f"Could not find metadata for {doi or url}"
            processed_papers.append(paper_data)

        except Exception as e:
            logging.error(f"Error processing {url or doi}: {e}")
            processed_papers.append({
                "url": url or "No URL",
                "doi": doi or "No DOI",
                "error": str(e),
                "message": f"Could not find metadata for {doi or url}"
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
    async with aiofiles.open(articleinfos_path, 'r') as f:
        content = await f.read()
        articles = json.loads(content)

    await clean_urls_in_article(articles)

    all_processed_papers = []

    total_articles = len(articles)
    with tqdm(total=total_articles, desc="Processing articles") as pbar:
        for article in articles:
            processed_papers = await process_article(article)
            all_processed_papers.extend(processed_papers)
            pbar.update(1)

    async with aiofiles.open(output_path, 'w') as f:
        await f.write(json.dumps(all_processed_papers, indent=4))

    print(f"Paper metadata saved to {output_path}")

if __name__ == "__main__":
    asyncio.run(process_all_articles('urldictcleandoistwo.json', 'processed_papers_two.json'))
