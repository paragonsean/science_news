import os
import sys
import json
import aiohttp
import aiofiles
import asyncio
from bs4 import BeautifulSoup
import urllib.parse as urlparse
from w3lib.url import url_query_cleaner
from url_normalize import url_normalize
from loguru import logger
from open_alex_scraper import OpenAlexScraper
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from collections import defaultdict
from urllib.parse import parse_qs as parse_qs

# Data Classes
@dataclass
class PaperLink:
    paperlink: Optional[str] = None
    doi: Optional[str] = None  # DOI can be 'N/A' or missing

@dataclass
class Article:
    title: Optional[str] = None
    url: Optional[str] = None
    author: Optional[str] = None
    doi_urls: Optional[List[str]] = field(default_factory=list)
    non_doi_urls: Optional[List[str]] = field(default_factory=list)
    index: Optional[str] = None
    paperlinks: Optional[List[PaperLink]] = field(default_factory=list)
    count: Optional[int] = None
    dois: Optional[List[str]] = field(default_factory=list)

@dataclass
class ArticleCollection:
    articles: Optional[List[Article]] = field(default_factory=list)

    def add_article(self, article: Article):
        self.articles.append(article)

    def to_dict(self):
        """Convert the dataclass structure to a dictionary for JSON serialization."""
        return {'articles': [asdict(article) for article in self.articles]}

    @classmethod
    def from_dict(cls, data: dict):
        """Create an ArticleCollection object from your data structure."""
        articles = []
        if isinstance(data, dict):
            data_items = data.items()
        elif isinstance(data, list):
            data_items = enumerate(data)
        else:
            raise ValueError("Unsupported JSON data structure. Must be a list or a dictionary.")

        for key, article_data in data_items:
            paperlinks = []
            for url_obj in article_data.get('urls', []):
                paperlink = url_obj.get('paperlink')
                doi = url_obj.get('doi')
                paperlinks.append(PaperLink(paperlink=paperlink, doi=doi))

            article = Article(
                title=article_data.get('title'),
                url=article_data.get('url') or article_data.get('sciencealert'),
                author=article_data.get('author'),
                index=key,
                paperlinks=paperlinks,
                count=article_data.get('count'),
                doi_urls=article_data.get('doi_urls', []),
                non_doi_urls=article_data.get('non_doi_urls', []),
                dois=article_data.get('dois', []),
            )
            articles.append(article)
        return cls(articles=articles)

    @classmethod
    async def from_json(cls, filename: str):
        """Asynchronously read from a JSON file and convert to an ArticleCollection object."""
        async with aiofiles.open(filename, 'r') as file:
            data = await file.read()
            json_data = json.loads(data)
            return cls.from_dict(json_data)

    async def to_json(self, filename: str):
        """Asynchronously write the ArticleCollection object to a JSON file."""
        async with aiofiles.open(filename, 'w') as file:
            json_data = json.dumps(self.to_dict()['articles'], indent=4)
            await file.write(json_data)

# ScienceAlertScraper Class
class ScienceAlertScraper:
    def __init__(self, json_input_file, json_output_file, headers, open_alex_scraper: OpenAlexScraper, article_template: Article):
        self.json_input_file = json_input_file
        self.json_output_file = json_output_file
        self.headers = headers
        self.article_collection = None  # We'll load this as an ArticleCollection
        self.open_alex_scraper = open_alex_scraper  # Store the OpenAlexScraper instance
        self.article_template = article_template  # Template Article object

        # Setup loguru logger
        logger.add("scraper.log", format="{time} {level} {message}", level="INFO")

    async def load_json_data(self):
        """Load the input data from JSON asynchronously using aiofiles and ArticleCollection."""
        try:
            self.article_collection = await ArticleCollection.from_json(self.json_input_file)
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
        article = Article(url=final_url)  # Start with an empty article to populate dynamically
        try:
            async with session.get(final_url, headers=self.headers) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')

                    # Conditionally fetch and populate fields based on the template
                    if self.article_template.title is not None:
                        title = soup.find('title').text.replace(" : ScienceAlert", "") if soup.find('title') else "No title found"
                        article.title = title

                    if self.article_template.author is not None:
                        author_tag = soup.find('meta', {'name': 'author'})
                        article.author = author_tag['content'] if author_tag else "No author found"

                    if self.article_template.doi_urls is not None:
                        doi_urls = set(self.extract_doi_urls(soup))  # Convert to set for efficient comparison
                        article.doi_urls = list(doi_urls)  # Store as list for JSON serialization

                    if self.article_template.non_doi_urls is not None:
                        non_doi_urls = set(self.extract_non_doi_urls(soup))
                        article.non_doi_urls = list(non_doi_urls)

                    logger.info(f"Fetched article: {article.title} from {final_url}")

                    return article

                else:
                    logger.warning(f"Failed to fetch {final_url}: HTTP {response.status}")
                    return Article(title="Error", url=final_url, author="Error", doi_urls=[], non_doi_urls=[])

        except Exception as e:
            logger.error(f"Error fetching article from {final_url}: {str(e)}")
            return Article(title="Error", url=final_url, author="Error", doi_urls=[], non_doi_urls=[])

    async def worker(self, session, queue, output_data):
        """Process each URL from the queue asynchronously."""
        while True:
            task = await queue.get()
            entry, key = task
            try:
                article_details = await self.fetch_article_details(session, entry.url)
                article_details.index = key

                # Update the entry with fetched details
                if self.article_template.title is not None:
                    entry.title = article_details.title

                # URL is always updated
                entry.url = article_details.url

                if self.article_template.author is not None:
                    entry.author = article_details.author

                if self.article_template.doi_urls is not None:
                    entry.doi_urls = article_details.doi_urls

                if self.article_template.non_doi_urls is not None:
                    entry.non_doi_urls = article_details.non_doi_urls

                output_data.append(entry)
                logger.info(f"Processed article: {entry.title} (index: {key})")

            except Exception as e:
                logger.error(f"Error processing article at {entry.url}: {str(e)}")
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
            for key, entry in enumerate(self.article_collection.articles):
                if entry.url:
                    await queue.put((entry, key))

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
        """Save the output data to JSON asynchronously using aiofiles."""
        new_article_collection = ArticleCollection(articles=output_data)
        await new_article_collection.to_json(self.json_output_file)
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
        query_params = parse_qs(parsed_url.query)
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
        """Return the existing paperlinks from the entry."""
        return entry.paperlinks

# URLCleaner Class
class URLCleaner:
    def __init__(self):
        # Set up logging with loguru
        logger.add("file_cleaner.log", level="INFO", rotation="1 week", compression="zip")

    async def check_redirect(self, url, session):
        """Check if a URL redirects."""
        logger.info(f"Checking URL: {url}")
        try:
            async with session.head(url, allow_redirects=False) as response:
                if 300 <= response.status < 400:  # Status codes for redirects
                    final_url = str(response.headers.get('Location', url))
                    logger.info(f"Redirect found: {url} -> {final_url}")
                    return True, url, final_url  # Return original and final redirect location
                logger.info(f"No redirect: {url}")
                return False, url, url  # No redirect, return the original URL
        except Exception as e:
            logger.error(f"Error checking URL {url}: {e}")
            return False, url, url  # On error, assume no redirect

    async def check_all_urls(self, articles):
        """Process the URLs in the articles."""
        redirect_counter = defaultdict(list)
        non_redirect_counter = defaultdict(list)

        async with aiohttp.ClientSession() as session:
            tasks = []
            task_index_map = {}

            # Iterate over each article
            for article_index, article in enumerate(articles):
                # Check article URL
                if article.url:
                    task = self.check_redirect(article.url, session)
                    task_index_map[task] = (article_index, 'url', article.url)
                    tasks.append(task)

                # Check DOI URLs
                for doi_url_index, doi_url in enumerate(article.doi_urls):
                    task = self.check_redirect(doi_url, session)
                    task_index_map[task] = (article_index, f'doi_urls[{doi_url_index}]', doi_url)
                    tasks.append(task)

                # Check non-DOI URLs
                for non_doi_url_index, non_doi_url in enumerate(article.non_doi_urls):
                    task = self.check_redirect(non_doi_url, session)
                    task_index_map[task] = (article_index, f'non_doi_urls[{non_doi_url_index}]', non_doi_url)
                    tasks.append(task)

                # Check paperlinks
                for paperlink_index, paperlink in enumerate(article.paperlinks):
                    if paperlink.paperlink:
                        task = self.check_redirect(paperlink.paperlink, session)
                        task_index_map[task] = (article_index, f'paperlinks[{paperlink_index}]', paperlink.paperlink)
                        tasks.append(task)

            # Gather all tasks
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Separate redirects from non-redirects and update the data
            for result, task in zip(results, tasks):
                if isinstance(result, Exception):
                    logger.error(f"Exception during checking URL: {result}")
                    continue
                is_redirect, original_url, final_url = result
                article_id, key, url_checked = task_index_map[task]

                article = articles[article_id]

                # Update the structure
                if key == 'url':
                    article.url = final_url
                    if is_redirect:
                        redirect_counter[final_url].append((article_id, original_url))
                    else:
                        non_redirect_counter[original_url].append((article_id, original_url))
                elif key.startswith('doi_urls'):
                    doi_url_index = int(key.split('[')[1].strip(']'))
                    article.doi_urls[doi_url_index] = final_url
                    if is_redirect:
                        redirect_counter[final_url].append((article_id, original_url))
                    else:
                        non_redirect_counter[original_url].append((article_id, original_url))
                elif key.startswith('non_doi_urls'):
                    non_doi_url_index = int(key.split('[')[1].strip(']'))
                    article.non_doi_urls[non_doi_url_index] = final_url
                    if is_redirect:
                        redirect_counter[final_url].append((article_id, original_url))
                    else:
                        non_redirect_counter[original_url].append((article_id, original_url))
                elif key.startswith('paperlinks'):
                    paperlink_index = int(key.split('[')[1].strip(']'))
                    article.paperlinks[paperlink_index].paperlink = final_url
                    if is_redirect:
                        redirect_counter[final_url].append((article_id, original_url))
                    else:
                        non_redirect_counter[original_url].append((article_id, original_url))

        return redirect_counter, non_redirect_counter

    def get_doi_from_url(self, url, prefix="https://doi.org/"):
        """Remove 'https://doi.org/' prefix from DOI URLs."""
        if prefix and url.startswith(prefix):
            return url[len(prefix):]
        return url

    def clean_doi_urls(self, doi_urls, prefix="https://doi.org/"):
        """Remove duplicates and 'prefix' from DOI URLs using a set."""
        cleaned_dois = set(self.get_doi_from_url(url, prefix=prefix) for url in doi_urls)
        return list(cleaned_dois)

    async def extract_dois_from_urls(self, doi_urls):
        """Extract DOIs from DOI URLs."""
        return [url.split('/')[-1] for url in doi_urls]

    async def clean_data(self, articles):
        """Clean the articles by checking redirects and cleaning DOIs."""
        logger.info("Starting the cleaning process...")

        try:
            # Check redirects and update URLs in the articles
            redirect_counter, non_redirect_counter = await self.check_all_urls(articles)

            # Extract DOIs from DOI URLs and update the articles
            for article in articles:
                if article.doi_urls:
                    # Extract and remove duplicates for DOI URLs
                    article.dois = self.clean_doi_urls(article.doi_urls)
                    article.doi_urls = self.clean_doi_urls(article.doi_urls, prefix=None)

            logger.info("Cleaning process completed successfully.")

            # Log results
            logger.info("Redirecting URLs:")
            for final_url, occurrences in redirect_counter.items():
                for article_id, original_url in occurrences:
                    logger.info(f"Article ID: {article_id}, Original URL: {original_url}, Redirects to: {final_url}")

            logger.info("\nNon-Redirecting URLs:")
            for original_url, occurrences in non_redirect_counter.items():
                for article_id, _ in occurrences:
                    logger.info(f"Article ID: {article_id}, Original URL: {original_url}")

        except Exception as e:
            logger.error(f"Error during the cleaning process: {e}")

# Helper Functions for User Input
def get_user_input(prompt: str) -> bool:
    """Ask the user if they want to perform a certain action."""
    response = input(f"{prompt} (Enter for Yes, type 'no' for No): ")
    return response.strip().lower() != 'no'

def setup_article_template() -> Article:
    """Dynamically ask the user which fields to scrape and create an Article template."""
    title = "Template Title" if get_user_input("Do you want to scrape the title?") else None
    author = "Template Author" if get_user_input("Do you want to scrape the author?") else None
    doi_urls = [] if get_user_input("Do you want to scrape the DOI URLs?") else None
    non_doi_urls = [] if get_user_input("Do you want to scrape the non-DOI URLs?") else None

    return Article(
        title=title,
        author=author,
        doi_urls=doi_urls,
        non_doi_urls=non_doi_urls
    )

def main():
    print("Welcome to the ScienceAlert Scraper and Analyzer!")
    print("What would you like to do?")
    print("1. Scrape articles from ScienceAlert")
    print("2. Analyze existing JSON data")
    print("3. Clean data (check redirects, clean DOIs)")
    choice = input("Enter the number of your choice (1, 2, or 3): ").strip()

    if choice == '1':
        # Scrape articles
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/91.0.4472.124 Safari/537.36'
        }
        open_alex = OpenAlexScraper()
        json_input_file = input("Enter the name of the input JSON file (e.g., 'urldict.json'): ").strip()
        if not json_input_file:
            json_input_file = 'urldict.json'  # Default input file name

        # Prompt for the output file name
        json_output_file = input("Enter the name of the output JSON file (e.g., 'output.json'): ").strip()
        if not json_output_file:
            json_output_file = 'output.json'

        # Dynamically set up the article template by asking the user what to scrape
        article_template = setup_article_template()

        scraper = ScienceAlertScraper(
            json_input_file=json_input_file,
            json_output_file=json_output_file,
            headers=headers,
            open_alex_scraper=open_alex,
            article_template=article_template  # Pass in the template article
        )
        asyncio.run(scraper.run())

    elif choice == '3':
        # Clean data
        json_file = input("Enter the name of the JSON file to clean (e.g., 'urldict.json'): ").strip()
        if not json_file:
            json_file = 'urldict.json'  # Default file name

        output_file = input("Enter the name of the output JSON file (e.g., 'cleaned_data.json'): ").strip()
        if not output_file:
            output_file = 'cleaned_data.json'

        # Load data
        try:
            article_collection = asyncio.run(ArticleCollection.from_json(json_file))
        except Exception as e:
            logger.error(f"Error loading input file {json_file}: {e}")
            return

        # Create URLCleaner instance
        cleaner = URLCleaner()

        # Clean data
        asyncio.run(cleaner.clean_data(article_collection.articles))

        # Save cleaned data
        asyncio.run(article_collection.to_json(output_file))
        logger.info(f"Cleaned data saved to {output_file}")

    else:
        print("Invalid choice. Please run the script again and select 1 or 3.")
        sys.exit(1)

if __name__ == "__main__":
    main()
