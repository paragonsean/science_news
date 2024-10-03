import json
import aiohttp
import asyncio
import pathlib
import os
import sys
import itertools
from utils import get_working_proxies


async def get_paper_metadata(session, doi=None, url=None, proxy=None):
    """Fetch metadata of a paper from OpenAlex API using DOI or URL asynchronously."""
    if not any([doi, url]):
        raise ValueError("At least one of 'doi' or 'url' must be provided.")

    # Build the API URL
    if doi:
        doi = doi.replace("https://doi.org/", "")
        api_url = f"https://api.openalex.org/works/doi:{doi}"
    elif url:
        api_url = f"https://api.openalex.org/works/{url}"

    try:
        # Pass the proxy URL directly to the get request
        async with session.get(api_url, proxy=f"http://{proxy}" if proxy else None) as response:
            if response.status != 200:
                raise Exception(f"Error: Received status code {response.status} for {doi or url}.")
            metadata = await response.json()

        doi = metadata.get("doi", None)
        if doi:
            doi = doi[len("https://doi.org/"):]
        title = metadata.get("display_name", "No Title Available")

        return {
            "title": metadata.get("display_name", "No Title Available"),
            "first_author": metadata["authorships"][0]["author"]["display_name"] if metadata.get("authorships") else "No Author",
            "authors": ", ".join([author["author"]["display_name"] for author in metadata.get("authorships", [])]),
            "year": metadata.get("publication_year", "Unknown Year"),
            "doi": doi,
            "doi_url": metadata.get("doi", "No DOI available"),
            "journal_or_institution": metadata.get("host_venue", {}).get("display_name", "Unknown Institution"),
            "pages": f"{metadata['biblio'].get('first_page', '')}-{metadata['biblio'].get('last_page', '')}",
            "volume": metadata["biblio"].get("volume", ""),
            "number": metadata["biblio"].get("issue", "")
        }
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return {
            "title": "Error",
            "error": str(e)
        }


async def process_article_batch(session, article_batch, proxy, proxy_limit=50):
    """Process a batch of articles and fetch metadata for each paperlink."""
    processed_papers = []
    success_count = 0  # Count the number of successful scrapes

    for article in article_batch:
        for paperlink in article['paperlinks']:
            if success_count >= proxy_limit:
                # Stop the batch when the limit is reached
                return processed_papers, success_count

            try:
                paper_data = await get_paper_metadata(session, doi=paperlink.get('doi'), url=paperlink.get('paperlink'), proxy=proxy)
                paper_data['source_article_title'] = article['title']
                processed_papers.append(paper_data)
                success_count += 1  # Increment success count

                # Print successful request details
                print(f"Success: {paper_data['title']} (DOI: {paper_data.get('doi', 'N/A')}, URL: {paperlink.get('paperlink')})")
            except Exception as e:
                print(f"Error processing {paperlink['paperlink']}: {e}")
                return processed_papers, success_count  # Return on error to switch proxy

    return processed_papers, success_count


async def process_all_articles(articleinfos_path, output_path, proxies, proxy_limit=50):
    """Open articleinfos.json, process each article with 50 per proxy, and save results to a new JSON file."""
    with open(articleinfos_path, 'r') as f:
        articles = json.load(f)

    all_processed_papers = []

    # Use itertools.cycle to cycle through proxies
    proxy_cycle = itertools.cycle(proxies)

    # Batch articles
    batch_size = 50  # Process 50 articles per proxy
    async with aiohttp.ClientSession() as session:
        i = 0
        while i < len(articles):
            proxy = next(proxy_cycle)  # Use the next proxy in the cycle
            article_batch = articles[i:i + batch_size]
            print(f"Processing batch {i + 1} to {i + batch_size} using proxy: {proxy}")

            # Process the batch
            try:
                processed_papers, success_count = await process_article_batch(session, article_batch, proxy, proxy_limit=proxy_limit)
                all_processed_papers.extend(processed_papers)
                i += success_count  # Only move forward by the successful count
            except Exception as e:
                print(f"Error with proxy {proxy}: {e}")
                proxy = next(proxy_cycle)  # Switch proxy if there's an error

    # Save the final processed papers to the output file
    with open(output_path, 'w') as f:
        json.dump(all_processed_papers, f, indent=4)

    print(f"Paper metadata saved to {output_path}")


# Main function
if __name__ == "__main__":
    proxies = get_working_proxies(refresh=True)
    asyncio.run(process_all_articles('articleinfos.json', 'processed_papers.json', proxies))
