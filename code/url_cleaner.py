# url_cleaner.py


import asyncio
import aiohttp
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
from w3lib.url import url_query_cleaner
from url_normalize import url_normalize

from loguru import logger

from base_classes import BaseCleaner
from data_models import Article


class URLCleaner(BaseCleaner):
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
        """Process the URLs in the articles and check for redirects."""
        redirect_counter = defaultdict(list)
        non_redirect_counter = defaultdict(list)

        async with aiohttp.ClientSession() as session:
            tasks = []
            task_index_map = {}

            # Iterate over each article
            for article_id, article in enumerate(articles):
                # Check article URL
                if article.url:
                    sciencealert_link = article.url
                    task = self.check_redirect(sciencealert_link, session)
                    task_index_map[task] = (article_id, "url", sciencealert_link)
                    tasks.append(task)

                # Process paperlinks
                for i, paperlink in enumerate(article.paperlinks):
                    link_url = paperlink.paperlink
                    if link_url:
                        task = self.check_redirect(link_url, session)
                        task_index_map[task] = (article_id, f"paperlinks[{i}]", link_url)
                        tasks.append(task)

            # Gather all tasks
            results = await asyncio.gather(*tasks)

            # Separate redirects from non-redirects and update the articles
            for result, task in zip(results, tasks):
                is_redirect, original_url, final_url = result
                article_id, key, url_checked = task_index_map[task]

                article = articles[article_id]

                # Update the structure for article.url or paperlinks
                if key == "url":
                    # Overwrite the original URL with the redirected URL (if found)
                    article.url = final_url
                    if is_redirect:
                        redirect_counter[final_url].append((article_id, original_url))
                    else:
                        non_redirect_counter[original_url].append((article_id, original_url))
                elif key.startswith("paperlinks"):
                    index = int(key.split("[")[1].strip("]"))  # Extract index
                    article.paperlinks[index].paperlink = final_url
                    if is_redirect:
                        redirect_counter[final_url].append((article_id, original_url))
                    else:
                        non_redirect_counter[original_url].append((article_id, original_url))

        return redirect_counter, non_redirect_counter

    def extract_doi_from_url(self, url, prefix="https://doi.org/"):
        """Remove 'https://doi.org/' prefix from DOI URLs."""
        if url.startswith(prefix):
            return url[len(prefix):]
        return url

    def clean_doi_urls(self, doi_urls, prefix="https://doi.org/"):
        """Remove duplicates and 'prefix' from DOI URLs using a set."""
        cleaned_dois = set(self.extract_doi_from_url(url, prefix=prefix) for url in doi_urls)
        return list(cleaned_dois)

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

    # Optional: Implement canonical_url if needed by other parts of the code
    def canonical_url(self, u):
        """Normalize and clean the URL, following 'u=' embedded URLs if present."""
        if not u:
            logger.warning("Received None or empty string in canonical_url")
            return u  # Return None or empty string as appropriate

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
