import pprint
import sys
import json
import logging  # Import logging module
import requests_cache
from habanero import Crossref
import aiohttp
import aiofiles
import asyncio
import pathlib
import time
from utils import get_working_proxies


# Set up requests-cache
requests_cache.install_cache('crossref_cache', expire_after=86400)  # Cache expires after 1 day (86400 seconds)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crossref_scraper.log"),  # Log to file
        logging.StreamHandler(sys.stdout)  # Also log to console
    ]
)
logger = logging.getLogger(__name__)  # Set up logger


class CrossrefScraper:
    def __init__(self, fix_uppercase=False, max_retries=3):
        self.fix_uppercase = fix_uppercase
        self.max_retries = max_retries

    def get_names(self, crossref_record, field):
        """Extract (author/editor) names from the Crossref record."""
        try:
            name_records = crossref_record[field]
        except KeyError:
            return None
        if self.fix_uppercase:
            return [
                f"{a['family'].title()}, {a['given'].title()}"
                for a in name_records
            ]
        else:
            return [
                f"{a['family']}, {a['given']}" for a in name_records
            ]

    def get_journal(self, crossref_record):
        """Extract journal from the Crossref record."""
        keys = ['short-container-title', 'container-title']
        name_candidates = [
            name for key in keys for name in crossref_record.get(key, [])
        ]
        for journal_name in name_candidates:
            if journal_name is not None:
                return journal_name
        return None

    def get_page(self, crossref_record):
        """Get page or article number from Crossref record."""
        try:
            page = crossref_record['article-number']
        except KeyError:
            page = crossref_record.get('page', None)
        return page

    def get_json(self, crossref_record):
        """Generate a JSON structure for the given Crossref record."""
        try:
            first_author = crossref_record['author'][0]['family'].capitalize()
        except (KeyError, IndexError):
            first_author = None

        author = self.get_names(crossref_record, 'author')
        title = crossref_record.get('title', [None])[0]
        try:
            year = crossref_record['issued']['date-parts'][0][0]
        except (KeyError, IndexError):
            year = None

        doi = crossref_record.get('DOI', None)
        journal = self.get_journal(crossref_record)
        pages = self.get_page(crossref_record)
        volume = crossref_record.get('volume', None)
        number = crossref_record.get('issue', None)

        return {
            "title": title,
            "first_author": first_author,
            "authors": author,
            "year": year,
            "doi": doi,
            "journal": journal,
            "pages": pages,
            "volume": volume,
            "number": number
        }

    async def get_json_from_doi(self, doi, session, proxy):
        """Generate a JSON entry for the given DOI asynchronously with retries."""
        url = f"https://api.crossref.org/works/{doi}"
        logger.info(f"Fetching metadata for DOI: {doi}")

        proxy_str = None
        if proxy:
            proxy_str = f"http://{proxy}"

        for attempt in range(self.max_retries):
            try:
                async with session.get(url, proxy=proxy_str) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch data for DOI: {doi} (HTTP Status: {response.status})")
                        continue

                    res = await response.json()
                    crossref_record = res['message']
                    metadata = self.get_json(crossref_record)
                    logger.info(f"Successfully fetched metadata for DOI: {doi}")
                    return metadata

            except Exception as exc_info:
                logger.error(f"Error processing metadata for DOI {doi}: {exc_info}")
                logger.info(f"Retrying {doi}, attempt {attempt + 1} of {self.max_retries}...")

        logger.error(f"Failed to fetch metadata for DOI {doi} after {self.max_retries} attempts.")
        return None

    async def fetch_all_metadata(self, dois, proxies):
        """Fetch metadata for all DOIs asynchronously using proxy rotation."""
        total_dois = len(dois)
        logger.info(f"Total DOIs to process: {total_dois}")

        async with aiohttp.ClientSession() as session:
            tasks = []
            for index, doi in enumerate(dois):
                # Switch proxy every 20 requests
                if index % 20 == 0:
                    proxy_index = (index // 20) % len(proxies)
                    proxy = proxies[proxy_index]
                    logger.info(f"Switching to proxy: {proxy}")

                tasks.append(self.get_json_from_doi(doi, session, proxy))
                # Print manual counting of processed DOIs
                logger.info(f"Processed DOI {index + 1} of {total_dois}")

            return await asyncio.gather(*tasks)

    async def save_to_json(self, data, filename="output.json"):
        """Save the JSON data to a file asynchronously."""
        async with aiofiles.open(filename, 'w') as json_file:
            await json_file.write(json.dumps(data, indent=4))
        logger.info(f"Data saved to {filename}")


# New function to encapsulate main logic
async def clean_and_fetch_metadata(doi_list, proxies):
    """Main logic to clean URLs and fetch metadata."""
    scraper = CrossrefScraper()

    # Fetch metadata using proxy rotation
    metadata = await scraper.fetch_all_metadata(doi_list, proxies)

    # Return metadata for further processing (if needed)
    return metadata


async def main(proxies):
    # Create the scraper instance here
    scraper = CrossrefScraper()

    # Read the urldictclean.json file asynchronously using aiofiles
    try:
        async with aiofiles.open("urldictclean.json", "r") as json_file:
            content = await json_file.read()
            articles = json.loads(content)
    except FileNotFoundError:
        logger.error("The file urldictclean.json was not found.")
        sys.exit(1)

    if not proxies:
        logger.error("No proxies found.")
        sys.exit(1)

    # Collect all DOIs directly from the JSON
    all_dois = []
    for article in articles:
        for link in article.get("paperlinks", []):
            doi = link.get('doi')
            if doi:
                all_dois.append(doi)

    if not all_dois:
        logger.error("No DOIs found in the input file.")
        sys.exit(1)

    # Show the total number of DOIs
    logger.info(f"Total number of DOIs: {len(all_dois)}")

    # Use the clean_and_fetch_metadata function to fetch metadata
    all_metadata = await clean_and_fetch_metadata(all_dois, proxies)

    # Remove None values (if any DOIs failed)
    all_metadata = [metadata for metadata in all_metadata if metadata]

    # Save all metadata to a single JSON file asynchronously
    await scraper.save_to_json(all_metadata, filename="all_metadata.json")

    logger.info("Metadata retrieval completed and saved to 'all_metadata.json'.")


if __name__ == "__main__":
    # Get working proxies
    logger.info("Starting the proxy fetch and DOI metadata extraction process.")
    proxies = get_working_proxies(refresh=True)  # Get proxies asynchronously

    # Pass proxies into the main function
    asyncio.run(main(proxies))
