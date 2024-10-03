from abc import ABC, abstractmethod
import asyncio
import aiohttp
import aiofiles
import json
from bs4 import BeautifulSoup

class BaseScraper(ABC):
    def __init__(self, json_input_file, json_output_file, headers):
        self.json_input_file = json_input_file
        self.json_output_file = json_output_file
        self.headers = headers
        self.data = {}

    @abstractmethod
    async def load_json_data(self):
        """Load data from a JSON file."""
        pass

    @abstractmethod
    async def save_to_json(self, output_data):
        """Save data to a JSON file."""
        pass

    @abstractmethod
    async def fetch_article_details(self, session, url):
        """Fetch article details from a given URL."""
        pass

    @abstractmethod
    def extract_doi_urls(self, soup):
        """Extract DOI URLs from BeautifulSoup object."""
        pass

    @abstractmethod
    def canonical_url(self, u):
        """Normalize and clean URLs."""
        pass

    @abstractmethod
    def clean_doi(self, doi):
        """Clean and standardize DOI strings."""
        pass

    async def run(self):
        """Main method to orchestrate the scraping process."""
        await self.load_json_data()

        async with aiohttp.ClientSession() as session:
            queue = asyncio.Queue()
            tasks = []
            output_data = []

            # Enqueue URLs and associated data
            for key, entry in self.data.items():
                url = entry.get('url')
                if url:
                    queue.put_nowait((url, entry, key))

            # Start worker tasks
            for _ in range(10):  # Number of worker tasks
                task = asyncio.create_task(self.worker(session, queue, output_data))
                tasks.append(task)

            # Wait until all tasks are processed
            await queue.join()

            # Cancel worker tasks
            for task in tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            await self.save_to_json(output_data)

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

                else:
                    article_details['paperlinks'] = []
                    article_details['count'] = 0

                output_data.append(article_details)
            except Exception as e:
                print(f"Error processing article at {url}: {str(e)}")
            finally:
                queue.task_done()

    def extract_paperlinks(self, entry):
        """Extract paper links and DOIs from the entry."""
        paperlinks = []
        if "urls" in entry:
            for url_obj in entry['urls']:
                paperlink = self.canonical_url(url_obj.get('paperlink', 'No paperlink found'))
                doi = self.clean_doi(url_obj.get('doi', 'No DOI found'))
                paperlinks.append({"paperlink": paperlink, "doi": doi})
        return paperlinks
