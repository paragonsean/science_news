import aiohttp
import asyncio
import logging
import json
from urllib.parse import urlparse, urlunparse
from utils import get_working_proxies
# Configure logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR)

class OpenAlexScraper:
    def __init__(self, proxies=None, metadata_file='all_metadata.json'):
        self.proxies = get_working_proxies(refresh=True)
        self.request_count = 0
        self.metadata_file = metadata_file
        self.metadata_cache = self.load_metadata_cache()

    def load_metadata_cache(self):
        """Load previous metadata from all_metadata.json if it exists."""
        try:
            with open(self.metadata_file, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            logging.info(f"Metadata file {self.metadata_file} not found. Starting with an empty cache.")
            return {}
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from {self.metadata_file}. Starting with an empty cache.")
            return {}

    async def save_metadata_cache(self):
        """Save the current metadata cache to all_metadata.json."""
        with open(self.metadata_file, 'w') as file:
            json.dump(self.metadata_cache, file, indent=4)

    async def get_paper_metadata(self, doi=None, url=None):
        """Fetch metadata of a paper from OpenAlex API using DOI or URL asynchronously."""
        if not any([doi, url]):
            raise ValueError("At least one of 'doi' or 'url' must be provided.")

        # Check if metadata is already in the cache
        if doi and doi in self.metadata_cache:
            logging.info(f"Returning cached metadata for DOI {doi}")
            return self.metadata_cache[doi]

        async with aiohttp.ClientSession() as session:
            if doi:
                doi = doi.replace("https://doi.org/", "")
                api_url = f"https://api.openalex.org/works/doi:{doi}"
            elif url:
                final_url = await self.follow_redirects(url)
                final_url = self.clean_url(final_url)  # Clean the final URL
                api_url = f"https://api.openalex.org/works/{final_url}"

            try:
                async with session.get(api_url) as response:
                    if response.status == 404:
                        logging.error(f"Error: Received status code 404 for {doi or url}. Resource not found.")
                        return {"error": f"Could not find metadata for {doi or url}"}
                    if response.status != 200:
                        logging.error(f"Error: Received status code {response.status} for {doi or url}.")
                        return {"error": f"Error: Received status code {response.status} for {doi or url}"}
                    metadata = await response.json()
            except Exception as e:
                logging.error(f"Error fetching metadata for {doi or url}: {e}")
                return {"error": f"Could not fetch metadata for {doi or url}"}

        doi = metadata.get("doi", None)
        if doi:
            doi = doi[len("https://doi.org/"):]  # Strip the DOI URL prefix
        title = metadata.get("display_name", "No Title Available")

        self.request_count += 1

        # Cache the fetched metadata
        if doi:
            self.metadata_cache[doi] = metadata
            await self.save_metadata_cache()

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

    async def follow_redirects(self, url):
        """Follow redirects to get the final URL."""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, allow_redirects=True) as response:
                    final_url = str(response.url)  # Get the final URL after redirects
                    return final_url
            except Exception as e:
                logging.error(f"Error following URL {url}: {str(e)}")
                return url  # Return the original URL if there's an error

    def clean_url(self, url):
        """Clean the URL by removing query parameters, fragments, and specific cases with urllib and custom logic."""
        if url.startswith("https://psycnet.apa.org/doiLanding?doi="):
            return url.split('=')[-1]  # Extract DOI part

        url = url.split('/full')[0]
        url = url.split('/abstract')[0]

        parsed_url = urlparse(url)
        cleaned_url = urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            '',
            ''
        ))

        return cleaned_url

    async def process_link(self, link, proxy=None):
        """Process each link and fetch metadata using a specific proxy."""
        try:
            doi = link.get('doi', None)
            url = link.get('url', None)

            # Fetch metadata using either DOI or URL
            paper_data = await self.get_paper_metadata(doi=doi, url=url)

            # Check if there was an error in fetching metadata
            if "error" in paper_data:
                paper_data['message'] = f"Could not find metadata for {'DOI: ' + doi if doi else 'URL: ' + url}"
            return {doi or url: paper_data}

        except Exception as e:
            # Handle and log any exceptions that occur during the process
            logging.error(f"Error processing {'DOI: ' + doi if doi else 'URL: ' + url}: {e}")
            return {
                doi or url: {
                    "url": url or "No URL",
                    "doi": doi or "No DOI",
                    "error": str(e),
                    "message": f"Could not find metadata for {'DOI: ' + doi if doi else 'URL: ' + url}"
                }
            }

    async def process_all_links(self, links):
        """Process a list or set of links and return results as a dictionary."""
        if not isinstance(links, (list, set)):
            raise ValueError("Links must be provided as a list or set.")

        if not links:  # Bail out early if there are no links to process
            logging.info("No links provided to process.")
            return {}

        all_processed_papers = {}
        proxy_index = 0
        failed_proxies = set()  # Keep track of proxies that fail

        tasks = []
        for link in links:
            # Skip failed proxies to avoid retrying them
            if self.proxies:
                while self.proxies and proxy_index < len(self.proxies):
                    proxy = self.proxies[proxy_index]
                    if proxy in failed_proxies:
                        proxy_index = (proxy_index + 1) % len(self.proxies)
                        continue
                    break
                else:
                    proxy = None
            else:
                proxy = None

            tasks.append(self.process_link(link, proxy))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Error processing link: {result}")
            else:
                # Detect if the result indicates proxy failure and log it accordingly
                for key, paper_data in result.items():
                    if "error" in paper_data and "proxy failure" in paper_data.get("message", ""):
                        failed_proxies.add(proxy)  # Add failed proxy to the blacklist
                    all_processed_papers.update(result)

        return all_processed_papers



# To run the scraper
