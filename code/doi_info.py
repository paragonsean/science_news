import argparse
import json
from typing import Optional
import requests


class NotFoundError(Exception):
    pass


def get_paper_metadata(doi: Optional[str] = None, name: Optional[str] = None, url: Optional[str] = None):
    """Fetches metadata of a paper from OpenAlex and stores specific fields in a JSON file."""
    # Determine which field to use for the request (DOI, name, or URL)
    if name:
        api_res = requests.get(
            f"https://api.openalex.org/works?search={name}&per-page=1&page=1&sort=relevance_score:desc"
        )
    elif doi:
        api_res = requests.get(f"https://api.openalex.org/works/https://doi.org/{doi}")

    elif url:
        api_res = requests.get(f"https://api.openalex.org/works/{url}")

    else:
        raise ValueError("Either DOI, name, or URL must be provided.")

    # Check if the request was successful
    if api_res.status_code != 200:
        raise NotFoundError(f"API request failed with status code {api_res.status_code}")

    metadata = api_res.json()
    print(metadata)

    # Extract the relevant fields
    paper_metadata = {
        "doi": metadata.get("doi", "Not available"),
        "mag": metadata.get("ids", {}).get("mag", "Not available"),
        "pmid": metadata.get("ids", {}).get("pmid", "Not available"),
        "title": metadata.get("title", "Not available"),
        "display_name": metadata.get("display_name", "Not available"),
        "publication_year": metadata.get("publication_year", "Not available"),
        "publication_date": metadata.get("publication_date", "Not available"),
        "biblio": metadata.get("biblio", {}),
        "cited_by_percentile_year": metadata.get("cited_by_percentile_year", {}),
        "institutions": metadata.get("institutions", []),
        "raw_author_name": metadata.get("authorships", [{}])[0].get("raw_author_name", "Not available"),
        "domain": metadata.get("primary_topic", {}).get("domain", {}).get("display_name", "Not available"),
        "field": metadata.get("primary_topic", {}).get("field", {}).get("display_name", "Not available")
    }

    # Save the extracted data to a JSON file
    with open("paper_metadata.json", "w") as json_file:
        json.dump(paper_metadata, json_file, indent=4)

    print(f"Specific paper metadata stored in 'paper_metadata.json'")
    return paper_metadata


def main():
    parser = argparse.ArgumentParser(
        description="Retrieves specific metadata of a research paper based on DOI, name, or URL and stores it in a JSON file."
    )

    parser.add_argument("--doi", type=str, help="DOI of the research paper.", metavar="DOI")
    parser.add_argument("--name", type=str, help="Name of the research paper.", metavar="name")
    parser.add_argument("--url", type=str, help="URL of the research paper.", metavar="url")

    args = parser.parse_args()

    # Ensure at least one argument is passed
    if not any([args.doi, args.name, args.url]):
        parser.error("At least one of --doi, --name, --url must be specified.")
    if len([arg for arg in (args.doi, args.name, args.url) if arg is not None]) > 1:
        parser.error("Only one of --doi, --name, --url must be specified.")

    # Retrieve metadata and store it in a JSON file
    get_paper_metadata(doi=args.doi, name=args.name, url=args.url)


if __name__ == "__main__":
    main()
