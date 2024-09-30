import json
import httpx
import asyncio
import itertools
from utils import get_working_proxies


async def get_paper_metadata(client, doi=None, url=None):
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
        response = await client.get(api_url)
        if response.status_code != 200:
            raise Exception(f"Error: Received status code {response.status_code} for {doi or url}.")
        metadata = response.json()

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
        return None


async def process_article_batch(client, article_batch, processed_papers, no_match_articles, proxy_limit=50):
    """Process a batch of articles and fetch metadata for each paperlink."""
    success_count = 0  # Count the number of successful scrapes

    for article in article_batch:
        index = article['index']
        for paperlink in article['paperlinks']:
            if success_count >= proxy_limit:
                # Stop the batch when the limit is reached
                return success_count

            doi = paperlink.get('doi')
            paperlink_url = paperlink.get('paperlink')

            key = doi if doi else paperlink_url  # Use DOI as key if available; else use URL

            if key in processed_papers:
                # DOI or URL already processed, append index and URL
                processed_papers[key]['source_article_indices'].append(index)
                processed_papers[key]['urls'].append(paperlink_url)
                continue  # Skip querying the API again
            else:
                # Try querying with DOI first
                paper_data = None
                if doi:
                    paper_data = await get_paper_metadata(client, doi=doi)

                # If DOI didn't work or wasn't available, try with URL
                if not paper_data and paperlink_url:
                    paper_data = await get_paper_metadata(client, url=paperlink_url)

                if paper_data:
                    # Successful retrieval
                    paper_data['source_article_indices'] = [index]  # Initialize list with current index
                    paper_data['urls'] = [paperlink_url]  # Initialize list with current URL
                    processed_papers[key] = paper_data  # Store paper_data with key
                    success_count += 1  # Increment success count

                    # Print successful request details
                    print(f"Success: {paper_data['title']} (DOI: {paper_data.get('doi', 'N/A')}, URL: {paperlink_url})")
                else:
                    # Neither DOI nor URL resulted in a match; record the failure
                    no_match_articles.append({
                        'index': index,
                        'doi': doi,
                        'url': paperlink_url
                    })
                    print(f"No match for article at index {index} (DOI: {doi}, URL: {paperlink_url})")

    return success_count


async def process_all_articles(articleinfos_path, output_path, proxy_limit=50):
    """Open articleinfos.json, process each article with 50 per batch, and save results to a new JSON file."""
    with open(articleinfos_path, 'r') as f:
        articles = json.load(f)

    processed_papers = {}  # Initialize dict to store processed papers
    no_match_articles = []  # List to store articles with no match

    # Batch articles
    batch_size = 50  # Process 50 articles per batch
    async with httpx.AsyncClient() as client:
        i = 0
        while i < len(articles):
            article_batch = articles[i:i + batch_size]
            print(f"Processing batch {i + 1} to {i + batch_size}")

            # Process the batch
            try:
                success_count = await process_article_batch(
                    client, article_batch, processed_papers, no_match_articles, proxy_limit=proxy_limit
                )
                i += len(article_batch)  # Move forward by the number of articles processed
            except Exception as e:
                print(f"Error: {e}")

    # Save the final processed papers to the output file
    # Since 'processed_papers' is a dict, we can convert it to a list
    all_processed_papers = list(processed_papers.values())
    with open(output_path, 'w') as f:
        json.dump(all_processed_papers, f, indent=4)

    # Save the no match articles to a separate file
    with open('no_match_articles.json', 'w') as f:
        json.dump(no_match_articles, f, indent=4)

    print(f"Paper metadata saved to {output_path}")
    print(f"No match articles saved to no_match_articles.json")


# Main function
if __name__ == "__main__":
    asyncio.run(process_all_articles('articleinfos.json', 'processed_papers_three.json'))
