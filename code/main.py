# main.py

import sys
import asyncio
from loguru import logger
import json
from collections import defaultdict
from typing import Dict, List, Union, Any

from data_models import Article, ArticleCollection
from base_classes import BaseCleaner
from url_cleaner import URLCleaner
from science_alert_scraper import ScienceAlertScraper
from open_alex_scraper import OpenAlexScraper  # Updated import

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
    print("2. Analyze JSON files for duplicates")
    print("3. Clean data (check redirects, clean DOIs)")
    print("4. Fetch metadata from OpenAlex using existing DOIs and URLs")
    choice = input("Enter the number of your choice (1, 2, 3, or 4): ").strip()

    if choice == '1':
        # Scrape articles
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/91.0.4472.124 Safari/537.36'
        }
        json_input_file = input("Enter the name of the input JSON file (e.g., 'urldict.json'): ").strip()
        if not json_input_file:
            json_input_file = 'urldict.json'  # Default input file name

        # Prompt for the output file name
        json_output_file = input("Enter the name of the output JSON file (e.g., 'output.json'): ").strip()
        if not json_output_file:
            json_output_file = 'output.json'

        # Dynamically set up the article template by asking the user what to scrape
        article_template = setup_article_template()

        # Instantiate the cleaner
        cleaner = URLCleaner()

        scraper = ScienceAlertScraper(
            json_input_file=json_input_file,
            json_output_file=json_output_file,
            headers=headers,
            open_alex_scraper=None,  # Not used in this context
            article_template=article_template,  # Pass in the template article
            cleaner=cleaner  # Pass in the cleaner instance
        )
        asyncio.run(scraper.run())

    elif choice == '2':
        # Analyze JSON files for duplicates
        json_file = input("Enter the name of the JSON file to analyze (e.g., 'urldict.json'): ").strip()
        if not json_file:
            json_file = 'urldict.json'  # Default file name

        print("Choose the data structure of your JSON file:")
        print("1. First data structure (e.g., from 'urldict.json')")
        print("2. Second data structure (e.g., from 'urldictcleandoistwo.json')")
        data_structure_choice = input("Enter the number of your choice (1 or 2): ").strip()

        if data_structure_choice == '1':
            key_paths = {
                'doi': ['urls', ['doi']],
                'url': ['urls', ['paperlink']],
                'sciencealert': ['sciencealert']
            }
        elif data_structure_choice == '2':
            key_paths = {
                'doi': ['paperlinks', ['doi']],
                'url': ['paperlinks', ['paperlink']],
                'sciencealert': ['url']
            }
        else:
            print("Invalid choice. Returning to main menu.")
            return

        analyze_json_duplicates(json_file, key_paths)

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

    elif choice == '4':
        # Fetch metadata from OpenAlex
        json_input_file = input("Enter the name of the input JSON file (e.g., 'cleaned_data.json'): ").strip()
        if not json_input_file:
            json_input_file = 'cleaned_data.json'  # Default input file name

        output_file = input("Enter the name of the output JSON file (e.g., 'metadata_output.json'): ").strip()
        if not output_file:
            output_file = 'metadata_output.json'

        # Load data
        try:
            article_collection = asyncio.run(ArticleCollection.from_json(json_input_file))
        except Exception as e:
            logger.error(f"Error loading input file {json_input_file}: {e}")
            return

        # Instantiate OpenAlexScraper
        openalex_scraper = OpenAlexScraper()

        # Process articles and fetch metadata
        asyncio.run(openalex_scraper.process_all_articles(article_collection.articles, output_file))

        logger.info(f"Metadata saved to {output_file}")

    else:
        print("Invalid choice. Please run the script again and select 1, 2, 3, or 4.")
        sys.exit(1)

def analyze_json_duplicates(json_file: str, key_paths: Dict[str, List[Union[str, List[str]]]]):
    """
    Analyze a JSON file for duplicates based on specified keys.

    Parameters:
    - json_file (str): The path to the JSON file.
    - key_paths (dict): A dictionary specifying the keys to analyze.
      The keys of this dictionary are the names of the counters (e.g., 'doi', 'url').
      The values are lists that represent the path to the value in the JSON data.
      Each element in the list can be a string (key) or a list (for iterating over a list of items).

    Example of key_paths:
    {
        'doi': ['urls', ['doi']],
        'url': ['urls', ['paperlink']],
        'sciencealert': ['sciencealert']
    }
    """
    # Load the JSON data
    try:
        with open(json_file, 'r') as file:
            data = json.load(file)
    except Exception as e:
        logger.error(f"Error loading JSON file {json_file}: {e}")
        return

    # Initialize counters
    counters = {key: defaultdict(list) for key in key_paths.keys()}

    # Handle both list and dict data structures
    if isinstance(data, dict):
        data_items = data.items()
    elif isinstance(data, list):
        data_items = enumerate(data)
    else:
        logger.error("Unsupported JSON data structure. Must be a list or a dictionary.")
        return

    # Iterate over each item in the data
    for item_id, item_data in data_items:
        # For each key we want to analyze
        for counter_name, path in key_paths.items():
            values = extract_values_from_path(item_data, path)
            for value in values:
                if value:
                    counters[counter_name][value].append(item_id)

    # Find duplicates
    duplicates = {
        key: {value: ids for value, ids in counter.items() if len(ids) > 1}
        for key, counter in counters.items()
    }

    # Calculate totals
    totals = {
        f"total_{key}_duplicates": sum(len(ids) for ids in duplicate_counter.values())
        for key, duplicate_counter in duplicates.items()
    }

    # Print the results
    for key, duplicate_counter in duplicates.items():
        print(f"\nDuplicate {key.upper()}s with Item IDs:")
        for value, ids in duplicate_counter.items():
            print(f"{value}: {len(ids)} occurrences, found in items: {ids}")

    # Print total duplicates
    for key, total in totals.items():
        print(f"\nTotal duplicates for {key.split('_')[1].upper()}s: {total}")

def extract_values_from_path(data_item: Any, path: List[Union[str, List[str]]]) -> List[Any]:
    """
    Extract values from a data item based on the provided path.

    Parameters:
    - data_item (Any): The data item (dict) to extract values from.
    - path (List[Union[str, List[str]]]): The path to navigate through the data item.

    Returns:
    - List[Any]: A list of extracted values.
    """
    values = [data_item]
    for key in path:
        if isinstance(key, list):
            # We need to iterate over a list in the data
            new_values = []
            for value in values:
                if isinstance(value, list):
                    for item in value:
                        for sub_key in key:
                            extracted_value = item.get(sub_key)
                            new_values.append(extracted_value)
                elif isinstance(value, dict):
                    for sub_key in key:
                        extracted_value = value.get(sub_key)
                        new_values.append(extracted_value)
            values = new_values
        else:
            # Navigate to the next key in all current values
            new_values = []
            for value in values:
                if isinstance(value, dict):
                    new_value = value.get(key)
                    if new_value is not None:
                        new_values.append(new_value)
            values = new_values
    return values

if __name__ == "__main__":
    main()
