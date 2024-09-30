import pprint
import sys
import json
from habanero import Crossref
import habanero


class CrossrefScraper:
    def __init__(self, fix_uppercase=False):
        self.cr = Crossref()  # Initialize the Crossref client
        self.fix_uppercase = fix_uppercase

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

    def get_event_location(self, crossref_record):
        try:
            return crossref_record['event']['location']
        except KeyError:
            return None

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
        crossreftype = crossref_record.get('type', None)
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
            "number": number,
        }

    def debug_crossref_record(self, crossref_record):
        """Pretty-print the given JSON record to stderr."""
        crossref_record = crossref_record.copy()
        try:
            # including all references makes the record very verbose
            del crossref_record['reference']
        except KeyError:
            pass
        pprint.pprint(crossref_record, stream=sys.stderr)

    def _check_response(self, res):
        if isinstance(res, dict):
            if 'status' in res:
                if res['status'] != 'ok':
                    raise IOError(
                        "Crossref query returned status %r" % res['status']
                    )
            else:
                raise IOError("Crossref query returned invalid %r" % res)
        else:
            raise IOError("Crossref query returned %r" % res)

    def get_json_from_doi(self, doi, debug_record=False):
        """Generate a JSON entry for the given DOI."""
        res = self.cr.works(ids=doi)
        self._check_response(res)
        crossref_record = res['message']
        if debug_record:
            self.debug_crossref_record(crossref_record)
        try:
            return self.get_json(crossref_record)
        except NotImplementedError as exc_info:
            print(f"WARNING: {exc_info}", file=sys.stderr)
            return habanero.cn.content_negotiation(ids=doi)

    def save_to_json(self, data, filename="output.json"):
        """Save the JSON data to a file."""
        with open(filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data saved to {filename}")


if __name__ == "__main__":
    scraper = CrossrefScraper(fix_uppercase=True)

    # Open the urldictclean.json file
    try:
        with open("urldictclean.json", "r") as json_file:
            articles = json.load(json_file)
    except FileNotFoundError:
        print("The file urldictclean.json was not found.")
        sys.exit(1)

    # List to collect all metadata
    all_metadata = []

    # Iterate through each article in the JSON
    for article in articles:
        paperlinks = article.get("paperlinks", [])

        # Loop through each paperlink (extracting DOI from each dictionary object)
        for link in paperlinks:
            doi = link.get('doi')
            if doi:
                print(f"Fetching metadata for DOI: {doi}")
                try:
                    # Fetch metadata by DOI
                    metadata = scraper.get_json_from_doi(doi)

                    if metadata:
                        # Append the metadata to the list
                        all_metadata.append(metadata)
                except Exception as e:
                    print(f"Error fetching metadata for DOI '{doi}': {e}")

    # Save all metadata to a single JSON file
    scraper.save_to_json(all_metadata, filename="all_metadata.json")

    print("Metadata retrieval completed and saved to 'all_metadata.json'.")
