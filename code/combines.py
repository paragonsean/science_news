import json
import logging
import sys
import aiohttp
import aiofiles
import asyncio
from random import choice
from utils import get_working_proxies

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


class BaseScraper:
    def __init__(self, proxies, fix_uppercase=False, max_retries=3):
        self.proxies = proxies
        self.fix_uppercase = fix_uppercase
        self.max_retries = max_retries

    async def run_with_proxy_async(self, func, *args, retries=3, **kwargs):
        """
        Runs an async function with a random proxy. Retries the function with a new proxy if it fails.

        Parameters:
        - func: The async function to run.
        - args: Arguments to pass to the function.
        - retries: Number of retries if the proxy fails.
        - kwargs: Keyword arguments to pass to the function.
        """
        for attempt in range(retries):
            proxy = choice(self.proxies)
            try:
                # Use the proxy for the request if it's not None
                if proxy:
                    proxy_dict = {
                        "http": f"http://{proxy}",
                        "https": f"http://{proxy}"
                    }
                    kwargs['proxy'] = proxy_dict
                else:
                    kwargs['proxy'] = None

                # Execute the async function with the current proxy
                result = await func(*args, **kwargs)
                return result

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed with proxy {proxy}. Retrying...")
                continue

        raise Exception("All proxy attempts failed.")


class MetadataScraper(BaseScraper):
    def __init__(self, proxies, fix_uppercase=False, max_retries=3):
        super().__init__(proxies, fix_uppercase, max_retries)

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

    async def fetch_metadata(self, doi, session):
        """Fetch metadata for a single DOI asynchronously."""
        url = f"https://api.crossref.org/works/{doi}"
        logger.info(f"Fetching metadata for DOI: {doi}")

        async def fetch_doi(url, session):
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch data for DOI: {doi} (HTTP Status: {response.status})")
                    return None

                res = await response.json()
                crossref_record = res['message']
                return self.get_json(crossref_record)

        return await self.run_with_proxy_async(fetch_doi, url, session)


class FileHandler:
    @staticmethod
    async def save_to_json(data, filename="output.json"):
        """Save the JSON data to a file asynchronously."""
        async with aiofiles.open(filename, 'w') as json_file:
            await json_file.write(json.dumps(data, indent=4))
        logger.info(f"Data saved to {filename}")


async def main(proxies):
    # Create the scraper instance here
    scraper = MetadataScraper(proxies)

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

    # Fetch metadata
    async with aiohttp.ClientSession() as session:
        all_metadata = await asyncio.gather(
            *[scraper.fetch_metadata(doi, session) for doi in all_dois]
        )

    # Remove None values (if any DOIs failed)
    all_metadata = [metadata for metadata in all_metadata if metadata]

    # Save all metadata to a single JSON file asynchronously
    await FileHandler.save_to_json(all_metadata, filename="all_metadata.json")

    logger.info("Metadata retrieval completed and saved to 'all_metadata.json'.")


if __name__ == "__main__":
    # Get working proxies
    logger.info("Starting the proxy fetch and DOI metadata extraction process.")
    proxies = get_working_proxies(refresh=True)  # Get proxies asynchronously

    # Pass proxies into the main function
    asyncio.run(main(proxies))