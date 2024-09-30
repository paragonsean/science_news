# science_alert_scraper.py

import os
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from loguru import logger
# Make sure to import the necessary modules for URL parsing
from urllib.parse import urlparse, parse_qs
from w3lib.url import url_query_cleaner
from url_normalize import url_normalize
from base_classes import BaseScraper, BaseCleaner
from data_models import Article, ArticleCollection
from open_alex_scraper import OpenAlexScraper  # Ensure this is available or adjust accordingly


class ScienceAlertScraper(BaseScraper):
    def __init__(
        self,
        json_input_file,
        json_output_file,
        headers,
        open_alex_scraper: OpenAlexScraper,
        article_template: Article,
        cleaner: BaseCleaner,
    ):
        super().__init__(json_input_file, json_output_file, headers)
        self.open_alex_scraper = open_alex_scraper
        self.article_template = article_template
        self.cleaner = cleaner  # Instance of a cleaner class

        # Initialize data structures
        self.article_collection = ArticleCollection()
        self.output_data = []
        self.cached_articles = {}

        # Setup loguru logger
        logger.add("scraper.log", format="{time} {level} {message}", level="INFO", rotation="1 week", compression="zip")

    async def load_json_data(self):
        """Load the input data from JSON asynchronously using aiofiles and ArticleCollection."""
        try:
            self.article_collection = await ArticleCollection.from_json(self.json_input_file)
            logger.info(f"Loaded data from {self.json_input_file}")
        except FileNotFoundError:
            logger.error(f"Error: {self.json_input_file} does not exist.")
        except json.JSONDecodeError:
            logger.error(f"Error: Failed to decode JSON from {self.json_input_file}.")

    async def load_existing_output(self):
        """Load existing output data if the output file already exists."""
        if os.path.exists(self.json_output_file):
            try:
                existing_collection = await ArticleCollection.from_json(self.json_output_file)
                self.output_data = existing_collection.articles
                # Build a cache for quick lookup
                self.cached_articles = {article.url: article for article in self.output_data if article.url}
                logger.info(f"Loaded existing output data from {self.json_output_file}")
            except Exception as e:
                logger.error(f"Error loading existing output file {self.json_output_file}: {e}")
        else:
            logger.info(f"No existing output file {self.json_output_file} found. Starting fresh.")
            self.output_data = []
            self.cached_articles = {}

    async def fetch_final_url(self, session, url):
        """Fetch the final URL after redirects using aiohttp."""
        try:
            async with session.get(url, headers=self.headers, allow_redirects=True) as response:
                final_url = str(response.url)  # Capture the final redirected URL
                logger.info(f"Fetched final URL for {url}: {final_url}")
                # Use cleaner's canonical_url method
                return self.cleaner.canonical_url(final_url)
        except Exception as e:
            logger.error(f"Error fetching final URL for {url}: {str(e)}")
            return self.cleaner.canonical_url(url)  # Return cleaned original URL if there's an error

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
                        title_tag = soup.find('meta', property='og:title')
                        article.title = title_tag['content'] if title_tag else "N/A"

                    if self.article_template.author is not None:
                        author_tag = soup.find('meta', {'name': 'author'})
                        article.author = author_tag['content'] if author_tag else "N/A"

                    if self.article_template.doi_urls is not None:
                        doi_urls = set(self.extract_doi_urls(soup))  # Convert to set for efficient comparison
                        article.doi_urls = list(doi_urls) if doi_urls else ["N/A"]  # Store as list for JSON serialization

                    if self.article_template.non_doi_urls is not None:
                        non_doi_urls = set(self.extract_non_doi_urls(soup))
                        article.non_doi_urls = list(non_doi_urls) if non_doi_urls else ["N/A"]

                    # Initialize paperlinks and DOIs
                    article.paperlinks = []
                    article.dois = []

                    # Extract paperlinks and DOIs
                    paperlinks_data = self.extract_paperlinks(soup)
                    for pl in paperlinks_data:
                        paperlink_obj = Article.PaperLink(
                            paperlink=pl['paperlink'],
                            doi=pl['doi']
                        )
                        article.paperlinks.append(paperlink_obj)
                        article.dois.append(pl['doi'])

                    # Remove duplicate DOIs
                    article.dois = list(set(article.dois))

                    logger.info(f"Fetched article: {article.title} from {final_url}")

                    return article

                else:
                    logger.warning(f"Failed to fetch {final_url}: HTTP {response.status}")
                    # Set fields to "N/A" since scraping failed
                    return self.create_na_article(final_url)

        except Exception as e:
            logger.error(f"Error fetching article from {final_url}: {str(e)}")
            # Set fields to "N/A" since scraping failed
            return self.create_na_article(final_url)

    def create_na_article(self, url):
        """Create an Article object with 'N/A' for missing fields."""
        return Article(
            title="N/A" if self.article_template.title is not None else None,
            url=url,
            author="N/A" if self.article_template.author is not None else None,
            doi_urls=["N/A"] if self.article_template.doi_urls is not None else None,
            non_doi_urls=["N/A"] if self.article_template.non_doi_urls is not None else None,
            paperlinks=[],
            dois=[]
        )

    def article_has_na_fields(self, article):
        """Check if any of the specified fields in the article are 'N/A'."""
        fields_to_check = []
        if self.article_template.title is not None:
            fields_to_check.append('title')
        if self.article_template.author is not None:
            fields_to_check.append('author')
        if self.article_template.doi_urls is not None:
            fields_to_check.append('doi_urls')
        if self.article_template.non_doi_urls is not None:
            fields_to_check.append('non_doi_urls')

        for field in fields_to_check:
            value = getattr(article, field, None)
            if value == "N/A":
                return True
            if isinstance(value, list) and "N/A" in value:
                return True
        return False

    async def worker(self, session, queue):
        """Process each URL from the queue asynchronously."""
        while True:
            task = await queue.get()
            if task is None:
                queue.task_done()
                break

            entry, key = task
            try:
                # Check if the article is already in the cache
                use_cached_article = False
                if entry.url in self.cached_articles:
                    cached_article = self.cached_articles[entry.url]
                    # Check if any of the fields are "N/A"
                    if not self.article_has_na_fields(cached_article):
                        self.output_data.append(cached_article)
                        logger.info(f"Using cached article for URL: {entry.url}")
                        use_cached_article = True
                    else:
                        logger.info(f"Cached article for URL: {entry.url} has 'N/A' fields, re-fetching")

                if not use_cached_article:
                    # Fetch the article details
                    article_details = await self.fetch_article_details(session, entry.url)
                    article_details.index = str(key)
                    article_details.count = len(article_details.paperlinks)

                    # Update the entry with fetched details
                    entry.title = article_details.title
                    entry.author = article_details.author
                    entry.url = article_details.url
                    entry.doi_urls = article_details.doi_urls
                    entry.non_doi_urls = article_details.non_doi_urls
                    entry.paperlinks = article_details.paperlinks
                    entry.dois = article_details.dois
                    entry.index = article_details.index
                    entry.count = article_details.count

                    # Update the cache
                    self.cached_articles[entry.url] = entry

                    self.output_data.append(entry)
                    logger.info(f"Processed article: {entry.title} (index: {key})")
            except Exception as e:
                logger.error(f"Error processing article at {entry.url}: {str(e)}")
                # Create an article with "N/A" values
                na_article = self.create_na_article(entry.url)
                na_article.index = str(key)
                self.output_data.append(na_article)
                # Update the cache with the 'N/A' article
                self.cached_articles[entry.url] = na_article
            finally:
                queue.task_done()

    async def run(self):
        """Main method to orchestrate scraping and saving data asynchronously."""
        logger.info("Starting to load JSON data...")
        await self.load_json_data()

        logger.info("Checking for existing output data...")
        await self.load_existing_output()

        queue = asyncio.Queue()

        logger.info("Loading URLs into the queue...")
        async with aiohttp.ClientSession() as session:
            for key, entry in enumerate(self.article_collection.articles):
                if entry.url:
                    # Check if URL is already in cached output data
                    await queue.put((entry, key))

            logger.info(f"Loaded {queue.qsize()} URLs into the queue. Starting workers...")

            # Create workers
            num_workers = 10  # Adjust the number of workers as needed
            workers = [asyncio.create_task(self.worker(session, queue)) for _ in range(num_workers)]

            # Put None into the queue to signal the workers to exit
            for _ in workers:
                await queue.put(None)

            # Wait for the queue to be fully processed
            await queue.join()

            # Cancel all workers once the job is done
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        logger.info("Saving processed data to JSON...")
        await self.save_to_json()
        logger.info("All data saved successfully.")

    async def save_to_json(self):
        """Save the output data to JSON asynchronously using aiofiles."""
        new_article_collection = ArticleCollection(articles=self.output_data)
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
                    # Use cleaner's canonical_url method
                    doi_url = self.cleaner.canonical_url(href)
                    doi_urls.append(doi_url)
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
                    # Use cleaner's canonical_url method
                    non_doi_url = self.cleaner.canonical_url(href)
                    non_doi_urls.append(non_doi_url)
        return non_doi_urls

    def extract_paperlinks(self, soup):
        """Extract all paperlink URLs and clean DOIs from the article content."""
        paperlinks = []

        article_tag = soup.find('article')
        if article_tag:
            links = article_tag.find_all('a', href=True)
            for link in links:
                href = link['href']
                # Use cleaner's canonical_url method
                cleaned_href = self.cleaner.canonical_url(href)

                if 'doi.org' in cleaned_href:
                    # It's a DOI URL, extract the DOI
                    doi = self.clean_doi(cleaned_href)
                    paperlinks.append({
                        'paperlink': cleaned_href,
                        'doi': doi
                    })

        return paperlinks

    def clean_doi(self, doi_url):
        """Clean the DOI by extracting and cleaning the path from the DOI URL."""
        # Extract the path from the DOI URL
        parsed_url = urlparse(doi_url)
        doi = parsed_url.path
        doi = doi.lstrip('/')
        parts_to_remove = [
            'doi/full/', 'doi/pdf/', 'pdf/full/', 'doi:', 'doi.org/', 'doi/', 'abstract/',
            'full/', 'pdf/', 'epdf/', 'abs/', '/doi', '/abstract', '/full', '/pdf', '/epdf', '/abs', '/html', 'html/'
        ]
        for part in parts_to_remove:
            doi = doi.replace(part, '')
        doi = doi.split('?')[0]  # Remove query parameters
        return doi



    def canonical_url(self, u):
        """Normalize and clean the URL, following 'u=' embedded URLs if present."""
        parsed_url = urlparse(u)
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
