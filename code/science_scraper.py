import aiohttp
import aiofiles
import asyncio
from bs4 import BeautifulSoup
import json
import re
import urllib.parse as urlparse
from w3lib.url import url_query_cleaner
from url_normalize import url_normalize
from loguru import logger
from open_alex_scraper import OpenAlexScraper


class ScienceAlertScraper:
    def __init__(self, json_input_file, json_output_file, headers, open_alex_scraper: OpenAlexScraper):
        self.json_input_file = json_input_file
        self.json_output_file = json_output_file
        self.headers = headers
        self.data = {}
        self.open_alex_scraper = open_alex_scraper  # Store the OpenAlexScraper instance

        # Setup loguru logger
        logger.add("scraper.log", format="{time} {level} {message}", level="INFO")

    async def load_json_data(self):
        """Load the input data from urldict.json asynchronously using aiofiles."""
        try:
            async with aiofiles.open(self.json_input_file, mode='r') as file:
                file_content = await file.read()
                self.data = json.loads(file_content)
                logger.info(f"Loaded data from {self.json_input_file}")
        except FileNotFoundError:
            logger.error(f"Error: {self.json_input_file} does not exist.")
        except json.JSONDecodeError:
            logger.error(f"Error: Failed to decode JSON from {self.json_input_file}.")

    async def fetch_final_url(self, session, url):
        """Fetch the final URL after redirects using aiohttp."""
        try:
            async with session.get(url, headers=self.headers, allow_redirects=True) as response:
                final_url = str(response.url)  # Capture the final redirected URL
                logger.info(f"Fetched final URL for {url}: {final_url}")
                return self.canonical_url(final_url)  # Clean and return the final URL
        except Exception as e:
            logger.error(f"Error fetching final URL for {url}: {str(e)}")
            return self.canonical_url(url)  # Return cleaned original URL if there's an error

    async def fetch_article_details(self, session, url):
        """Fetch article details from ScienceAlert asynchronously using aiohttp."""
        final_url = await self.fetch_final_url(session, url)
        try:
            async with session.get(final_url, headers=self.headers) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')

                    title = soup.find('title').text.replace(" : ScienceAlert", "") if soup.find(
                        'title') else "No title found"
                    author_tag = soup.find('meta', {'name': 'author'})
                    author = author_tag['content'] if author_tag else "No author found"

                    # Extract DOI and non-DOI URLs
                    doi_urls = set(self.extract_doi_urls(soup))  # Convert to set for efficient comparison
                    non_doi_urls = set(self.extract_non_doi_urls(soup))

                    article_details = {
                        "title": title,
                        "url": final_url,  # Save the final cleaned URL
                        "author": author,
                        "doi_urls": list(doi_urls),  # Store as list for JSON serialization
                        "non_doi_urls": list(non_doi_urls)  # List of other non-DOI URLs
                    }

                    logger.info(f"Fetched article: {title} from {final_url}")

                    # Now, let's compare doi_urls with paperlinks
                    if doi_urls:
                        paperlinks = self.extract_paperlinks(soup)  # Assuming this is where you get the paper links
                        for paperlink in paperlinks:
                            paperlink_doi = paperlink.get('doi')  # Assume paperlink has a 'doi' field
                            if paperlink_doi in doi_urls:
                                paperlink['exists_in_doi_urls'] = True
                            else:
                                paperlink['exists_in_doi_urls'] = False

                        article_details['paperlinks'] = paperlinks

                    return article_details

                else:
                    logger.warning(f"Failed to fetch {final_url}: HTTP {response.status}")
                    return {"title": "Error", "url": final_url, "author": "Error", "doi_urls": [], "non_doi_urls": []}

        except Exception as e:
            logger.error(f"Error fetching article from {final_url}: {str(e)}")
            return {"title": "Error", "url": final_url, "author": "Error", "doi_urls": [], "non_doi_urls": []}

    async def fetch_doi_metadata(self, doi):
        try:
            # Directly call OpenAlexScraper's method without proxy management
            metadata = await self.open_alex_scraper.get_paper_metadata(doi=doi)

            # Log the metadata to inspect its structure
            logger.info(f"Metadata for DOI {doi}: {metadata}")

            # Check if the response is in the expected format before accessing indices
            if isinstance(metadata, dict):
                return metadata
            else:
                logger.error(f"Unexpected format for metadata response for DOI {doi}: {type(metadata)}")
                return {}
        except Exception as e:
            logger.error(f"Error fetching metadata for DOI {doi}: {str(e)}")
            return {}

    async def worker(self, session, queue, output_data):
        """Process each URL from the queue asynchronously."""
        while True:
            task = await queue.get()
            url, entry, key = task
            try:
                article_details = await self.fetch_article_details(session, url)
                article_details['index'] = key

                if entry is not None:
                    paperlinks = self.extract_paperlinks(entry)
                    article_details['paperlinks'] = paperlinks
                    article_details['count'] = entry.get('count', 0)

                    # Collect all DOI links from the article
                    doi_links = [paperlink['doi'] for paperlink in article_details['paperlinks'] if 'doi' in paperlink]

                    if doi_links:
                        # Use process_all_links to fetch metadata for all DOI links
                        doi_metadata_results = await open_alex.process_all_links(doi_links)

                        # Append the fetched metadata to the corresponding paperlinks
                        for paperlink in article_details['paperlinks']:
                            doi = paperlink.get('doi')
                            if doi and doi in doi_metadata_results:
                                paperlink['metadata'] = doi_metadata_results[doi]

                else:
                    article_details['paperlinks'] = []
                    article_details['count'] = 0

                output_data.append(article_details)
                logger.info(f"Processed article: {article_details['title']} (index: {key})")

            except Exception as e:
                logger.error(f"Error processing article at {url}: {str(e)}")
            finally:
                queue.task_done()

    async def run(self):
        """Main method to orchestrate scraping and saving data asynchronously."""
        logger.info("Starting to load JSON data...")
        await self.load_json_data()

        queue = asyncio.Queue()
        output_data = []

        logger.info("Loading URLs into the queue...")
        async with aiohttp.ClientSession() as session:
            for key, entry in self.data.items():
                if isinstance(entry, dict) and 'sciencealert' in entry:
                    sciencealert_url = self.canonical_url(entry['sciencealert'])
                    await queue.put((sciencealert_url, entry, key))
                elif isinstance(entry, str):
                    clean_url = self.canonical_url(entry)
                    await queue.put((clean_url, None, key))

            logger.info(f"Loaded {queue.qsize()} URLs into the queue. Starting workers...")

            # Create workers
            workers = [asyncio.create_task(self.worker(session, queue, output_data)) for _ in range(10)]

            # Wait for the queue to be fully processed
            await queue.join()

            # Cancel all workers once the job is done
            for worker in workers:
                worker.cancel()

        logger.info("Saving processed data to JSON...")
        await self.save_to_json(output_data)
        logger.info("All data saved successfully.")

    async def save_to_json(self, output_data):
        """Save the output data to urldictcleandoistwo.json asynchronously using aiofiles."""
        try:
            # Load existing data from the file if it exists
            async with aiofiles.open(self.json_output_file, mode='r') as file:
                existing_data = json.loads(await file.read())
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []

        # Append new data to the existing data
        existing_data.extend(output_data)

        # Save the combined data back to the file
        async with aiofiles.open(self.json_output_file, mode='w') as file:
            await file.write(json.dumps(existing_data, indent=4))
            logger.info(f"Data saved to {self.json_output_file}")

    def extract_doi_urls(self, soup):
        """Extract all DOI URLs inside the <article> tag."""
        doi_urls = []
        article_tag = soup.find('article')
        if article_tag:
            links = article_tag.find_all('a', href=True)
            for link in links:
                href = link['href']
                if href.startswith('https://doi.org/'):
                    doi_urls.append(self.canonical_url(href))
        return doi_urls

    def extract_non_doi_urls(self, soup):
        """Extract all non-DOI URLs from the article."""
        non_doi_urls = []
        article_tag = soup.find('article')
        if article_tag:
            links = article_tag.find_all('a', href=True)
            for link in links:
                href = link['href']
                if not href.startswith('https://doi.org/'):
                    non_doi_urls.append(self.canonical_url(href))
        return non_doi_urls

    def canonical_url(self, u):
        """Normalize and clean the URL, following 'u=' embedded URLs if present."""
        parsed_url = urlparse.urlparse(u)
        query_params = urlparse.parse_qs(parsed_url.query)
        if 'u' in query_params:
            embedded_url = query_params['u'][0]
            u = embedded_url
        u = url_normalize(u)
        u = url_query_cleaner(u, parameterlist=['utm_source', 'utm_medium', 'utm_campaign', 'gclid', 'fbclid'],
                              remove=True)
        if u.endswith("/"):
            u = u[:-1]
        return u

    def clean_doi(self, doi):
        """Clean the DOI by removing specific combinations and individual parts with or without slashes."""
        doi = doi.lstrip('/')
        parts_to_remove = [
            'doi/full/', 'doi/pdf/', 'pdf/full/', 'doi:', 'doi.org/', 'doi/', 'abstract/',
            'full/', 'pdf/', 'epdf/', 'abs/', '/doi', '/abstract', '/full', '/pdf', '/epdf', '/abs', '/html', 'html/'
        ]
        for part in parts_to_remove:
            doi = doi.replace(part, '')
        doi = doi.split('?')[0]  # Remove query parameters
        return doi

    def extract_paperlinks(self, entry):
        """Extract all paperlink URLs and clean DOIs from the 'urls' array in the entry."""
        paperlinks = []

        if "urls" in entry and isinstance(entry['urls'], list):
            for url_obj in entry['urls']:
                # Extract and clean paperlink and DOI, log warnings if missing
                paperlink = url_obj.get('paperlink')
                doi = url_obj.get('doi')

                if not paperlink:
                    logger.warning(f"Missing paperlink in entry: {url_obj}")
                    continue  # Skip this entry if paperlink is missing

                if not doi:
                    logger.warning(f"Missing DOI in entry: {url_obj}")
                    continue  # Skip this entry if DOI is missing

                # Clean the paperlink and DOI before adding to paperlinks list
                cleaned_paperlink = self.canonical_url(paperlink)
                cleaned_doi = self.clean_doi(doi)

                paperlinks.append({
                    "paperlink": cleaned_paperlink,
                    "doi": cleaned_doi
                })

        return paperlinks


if __name__ == "__main__":
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    open_alex = OpenAlexScraper()
    json_input_file = 'urldict.json'
    json_output_file = 'urldictcleando.json'
    scraper = ScienceAlertScraper(json_input_file=json_input_file, json_output_file=json_output_file, headers=headers, open_alex_scraper=open_alex)
    asyncio.run(scraper.run())