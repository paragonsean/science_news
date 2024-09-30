import os
import json
import aiohttp
import aiofiles
import asyncio
from urllib.parse import urlparse, urlunparse
from tqdm.asyncio import tqdm


# Function to clean URLs
def clean_url(url):
    """Clean the URL by removing query parameters, fragments, and removing '/full' and '/abstract'."""

    # Handle specific case for psycnet
    if url.startswith("https://psycnet.apa.org/doiLanding?doi="):
        return url.split('=')[-1]  # Extract DOI part

    # Remove "/full" and "/abstract" from the URL path explicitly
    url = url.replace("/full", "")
    url = url.replace("/abstract", "")

    # Parse the URL into components
    parsed_url = urlparse(url)

    # Rebuild the URL without the query and fragment parts
    cleaned_url = urlunparse((
        parsed_url.scheme,  # Keep scheme (e.g., https)
        parsed_url.netloc,  # Keep domain (e.g., www.example.com)
        parsed_url.path,  # Keep the path (but without /full or /abstract)
        parsed_url.params,  # Keep URL parameters (if any)
        '',  # Remove query parameters (anything after '?')
        ''  # Remove fragments (anything after '#')
    ))

    return cleaned_url


# Function to follow the links and get the final URL
async def follow_link_and_get_final_url(url):
    """Follow the link and return the final URL after redirects."""

    # Clean the URL first
    cleaned_url = clean_url(url)

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(cleaned_url, allow_redirects=True) as response:
                final_url = str(response.url)  # Get the final URL after redirects
                return final_url
        except Exception as e:
            print(f"Error following {cleaned_url}: {e}")
            return cleaned_url  # Return the cleaned URL if there's an error


# Process a chunk of URLs
async def process_url_chunk(urls_chunk):
    """Process a chunk of URLs and return a list of final URLs."""
    tasks = [follow_link_and_get_final_url(url) for url in urls_chunk]
    return await asyncio.gather(*tasks)


# Clean and follow the URLs for each article with progress tracking
async def clean_urls_in_article(articles):
    """Clean the URLs for all articles and follow the redirects."""
    total_links = sum(len(article.get('paperlinks', [])) for article in articles)  # Count total paperlinks
    with tqdm(total=total_links, desc="Processing URLs", unit="url") as progress_bar:
        for article in articles:
            paperlinks = article.get('paperlinks', [])

            # Process URLs in chunks of 40
            chunk_size = 30
            for i in range(0, len(paperlinks), chunk_size):
                urls_chunk = paperlinks[i:i + chunk_size]
                final_urls = await process_url_chunk(urls_chunk)
                article['paperlinks'][i:i + chunk_size] = final_urls
                progress_bar.update(len(urls_chunk))  # Update progress bar after each chunk is processed


# Clean and follow all URLs in the provided JSON file
async def clean_json_file_follows(articleinfos_path, output_path):
    """Open articleinfos.json, clean URLs, follow redirects, and save the final result to a new JSON file."""
    # Open and load articleinfos.json using aiofiles
    async with aiofiles.open(articleinfos_path, 'r') as f:
        content = await f.read()
        articles = json.loads(content)

    # Clean the URLs in the articles and follow redirects
    await clean_urls_in_article(articles)

    # Save the result to a new JSON file
    async with aiofiles.open(output_path, 'w') as f:
        await f.write(json.dumps(articles, indent=4))

    print(f"Cleaned and followed URLs saved to {output_path}")


# Main function
if __name__ == "__main__":
    asyncio.run(clean_json_file_follows('articleinfo.json', 'cleaned_articleinfos.json'))
