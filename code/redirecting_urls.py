import json
from collections import defaultdict
import aiohttp
import asyncio
from loguru import logger

# Set up logging with loguru
logger.add("file_cleaner.log", level="INFO", rotation="1 week", compression="zip")

# Function to check if a URL redirects
async def check_redirect(url, session):
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

# Function to process the URLs in the JSON data
async def check_all_urls(data):
    redirect_counter = defaultdict(list)
    non_redirect_counter = defaultdict(list)

    async with aiohttp.ClientSession() as session:
        tasks = []
        task_index_map = {}

        # Iterate over each article in the data
        for article_id, article_data in enumerate(data):
            # Check ScienceAlert links
            if "url" in article_data:
                sciencealert_link = article_data["url"]
                task = check_redirect(sciencealert_link, session)
                task_index_map[task] = (article_id, "url", sciencealert_link)
                tasks.append(task)

            # Process DOIs and URLs in the article
            paperlinks = article_data.get("paperlinks", [])
            for i, link in enumerate(paperlinks):
                paperlink = link.get("paperlink", None)
                if paperlink:
                    task = check_redirect(paperlink, session)
                    task_index_map[task] = (article_id, f"paperlinks[{i}]", paperlink)
                    tasks.append(task)

        # Gather all tasks
        results = await asyncio.gather(*tasks)

        # Separate redirects from non-redirects and update the data
        for result, task in zip(results, tasks):
            is_redirect, original_url, final_url = result
            article_id, key, url_checked = task_index_map[task]

            # Update the structure for ScienceAlert or paperlinks
            if key == "url":
                # Overwrite the original URL with the redirected URL (if found)
                data[article_id]["url"] = final_url
                if is_redirect:
                    redirect_counter[final_url].append((article_id, original_url))
                else:
                    non_redirect_counter[original_url].append((article_id, original_url))
            elif key.startswith("paperlinks"):
                index = int(key.split("[")[1].strip("]"))  # Extract index
                data[article_id]["paperlinks"][index]["paperlink"] = final_url
                if is_redirect:
                    redirect_counter[final_url].append((article_id, original_url))
                else:
                    non_redirect_counter[original_url].append((article_id, original_url))

    return redirect_counter, non_redirect_counter, data

# Function to remove duplicate DOIs from a list
def get_doi_from_url(url, prefix="https://doi.org/"):
    """Remove 'https://doi.org/' prefix from DOI URLs."""
    prefix = prefix
    if url.startswith(prefix):
        return url[len(prefix):]
    return url

# Function to clean DOI URLs by removing duplicates and 'https://doi.org/' prefix
def clean_doi_urls(doi_urls, prefix="https://doi.org/"):
    """Remove duplicates and 'prefix' from DOI URLs using a set."""
    cleaned_dois = set(get_doi_from_url(url, prefix=prefix) for url in doi_urls)
    return list(cleaned_dois)

# Function to extract DOIs from DOI URLs
async def extract_dois_from_urls(doi_urls):
    """Extract DOIs from DOI URLs."""
    return [url.split('/')[-1] for url in doi_urls]

# Function to clean the file by extracting DOIs and checking URLs
async def clean_file(input_filename, output_filename):
    """Clean the input file by extracting DOIs and checking URLs."""
    logger.info("Starting the cleaning process...")

    try:
        # Load the JSON data
        with open(input_filename, 'r') as file:
            data = json.load(file)
    except Exception as e:
        logger.error(f"Error loading input file {input_filename}: {e}")
        return

    try:
        # Check redirects and update URLs in the data
        redirect_counter, non_redirect_counter, updated_data = await check_all_urls(data)

        # Extract DOIs from DOI URLs and update the data
        for article in updated_data:
            if 'doi_urls' in article:
                # Extract and remove duplicates for DOI URLs
                article['dois'] = clean_doi_urls(article['doi_urls'])
                article['doi_urls'] = clean_doi_urls(article['doi_urls'], prefix=None)

        # Save the cleaned data to a new file (overwrite old URLs with final redirected URLs)
        with open(output_filename, 'w') as file:
            json.dump(updated_data, file, indent=4)

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

# Main function to run the cleaning process

