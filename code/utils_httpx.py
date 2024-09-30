import json
import httpx
import asyncio
import pathlib
import os
import sys
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from tqdm import tqdm
import requests

def get_working_proxies(refresh: bool = False):
    if pathlib.Path("proxies.txt").exists() and not refresh:
        with open("proxies.txt") as f:
            proxy_urls = [None] + f.read().splitlines()
        return proxy_urls

    proxies = []
    print("No proxies found, fetching proxies from api.proxyscrape.com...")

    # Fetch proxies
    r = requests.get(
        "https://api.proxyscrape.com/?request=getproxies&proxytype=https&timeout=10000&country=all&ssl=all&anonymity=all")
    proxies += r.text.splitlines()
    r = requests.get(
        "https://api.proxyscrape.com/?request=getproxies&proxytype=http&timeout=10000&country=all&ssl=all&anonymity=all")
    proxies += r.text.splitlines()

    working_proxies = []
    print(f"Checking {len(proxies)} proxies...")

    session = FuturesSession(max_workers=100)
    futures = []

    for proxy in proxies:
        future = session.get('https://api.myip.com', proxies={'https': f'http://{proxy}'}, timeout=5)
        future.proxy = proxy
        futures.append(future)

    for future in tqdm(as_completed(futures), total=len(futures)):
        try:
            future.result()
            working_proxies.append(future.proxy)
        except KeyboardInterrupt:
            sys.exit()
        except:
            continue

    with open("proxies.txt", "w") as f:
        f.write("\n".join(working_proxies))

    os.system("cls")
    return [None] + working_proxies

async def get_paper_metadata(doi=None, url=None, proxy=None):
    if not any([doi, url]):
        raise ValueError("At least one of 'doi' or 'url' must be provided.")

    # Prepare the proxy string
    proxy_url = f"http://{proxy}" if proxy else None

    # Use httpx.AsyncClient with the proxy parameter directly
    async with httpx.AsyncClient(proxies={"http": proxy_url, "https": proxy_url} if proxy else None) as client:
        if doi:
            doi = doi.replace("https://doi.org/", "")
            api_url = f"https://api.openalex.org/works/doi:{doi}"
        elif url:
            # Make sure URL is correctly formatted for OpenAlex API
            api_url = f"https://api.openalex.org/works/{url}"

        try:
            response = await client.get(api_url)
            response.raise_for_status()  # This will raise an error for HTTP error responses
            metadata = response.json()
        except httpx.HTTPStatusError as e:
            # Log detailed error information
            print(f"HTTP Status Error: {e.response.status_code} for URL: {api_url}")
            raise Exception(f"HTTP Status Error: {e.response.status_code} for {doi or url}")
        except httpx.RequestError as e:
            # Log detailed error information
            print(f"Request Error: {e} for URL: {api_url}")
            raise Exception(f"Request Error: Failed for {doi or url}: {e}")

    # Process the metadata and prepare it for return
    doi = metadata.get("doi", None)
    if doi:
        doi = doi[len("https://doi.org/"):]
    title = metadata.get("display_name", "No Title Available")

    return {
        "title": title,
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


async def process_article_batch(article_batch, proxy):
    processed_papers = []
    for article in article_batch:
        for paperlink in article['paperlinks']:
            try:
                paper_data = await get_paper_metadata(url=paperlink, proxy=proxy)
                paper_data['source_article_title'] = article['title']
                processed_papers.append(paper_data)
            except Exception as e:
                print(f"Error processing {paperlink}: {e}")
                processed_papers.append({
                    "url": paperlink,
                    "error": str(e)
                })
    return processed_papers

async def process_all_articles(articleinfos_path, output_path, proxies):
    with open(articleinfos_path, 'r') as f:
        articles = json.load(f)

    all_processed_papers = []
    proxy_cycle = (proxy for proxy in proxies)

    batch_size = 200
    for i in range(0, len(articles), batch_size):
        article_batch = articles[i:i + batch_size]
        proxy = next(proxy_cycle)
        processed_papers = await process_article_batch(article_batch, proxy)
        all_processed_papers.extend(processed_papers)

    with open(output_path, 'w') as f:
        json.dump(all_processed_papers, f, indent=4)

    print(f"Paper metadata saved to {output_path}")

if __name__ == "__main__":
    proxies = get_working_proxies(refresh=False)
    asyncio.run(process_all_articles('articleinfostwo.json', 'processed_papers.json', proxies))
