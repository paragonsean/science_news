import json
from collections import defaultdict

# Load the JSON data
with open('urldictcleandoistwo.json', 'r') as file:
    data = json.load(file)

# Initialize variables for analysis
doi_counter = defaultdict(list)
url_counter = defaultdict(list)
sciencealert_counter = defaultdict(list)

# Iterate over each article in the data using the article ID
for article in data:
    # Count the ScienceAlert link for this article
    sciencealert_link = article.get("url")
    if sciencealert_link and "sciencealert.com" in sciencealert_link:
        sciencealert_counter[sciencealert_link].append(article["index"])

    # Iterate over paperlinks
    paperlinks = article.get("paperlinks", [])
    for link_info in paperlinks:
        paperlink = link_info.get("paperlink")
        doi = link_info.get("doi")

        # Track DOI occurrences and their corresponding article IDs
        if doi and doi != "N/A":
            doi_counter[doi].append(article["index"])

        # Track URL occurrences and their corresponding article IDs
        if paperlink:
            url_counter[paperlink].append(article["index"])

# Find duplicate DOIs and URLs
duplicate_dois = {doi: articles for doi, articles in doi_counter.items() if len(articles) > 1}
duplicate_urls = {url: articles for url, articles in url_counter.items() if len(articles) > 1}
duplicate_sciencealert = {link: articles for link, articles in sciencealert_counter.items() if len(articles) > 1}

# Calculate total duplicates
total_duplicate_dois = sum(len(articles) for articles in duplicate_dois.values())
total_duplicate_urls = sum(len(articles) for articles in duplicate_urls.values())
total_duplicate_sciencealert = sum(len(articles) for articles in duplicate_sciencealert.values())

# Print the duplicate DOIs, URLs, and ScienceAlert links with the corresponding article IDs
print("Duplicate DOIs with Article IDs:")
for doi, articles in duplicate_dois.items():
    print(f"{doi}: {len(articles)} occurrences, found in articles: {articles}")

print("\nDuplicate URLs with Article IDs:")
for url, articles in duplicate_urls.items():
    print(f"{url}: {len(articles)} occurrences, found in articles: {articles}")

print("\nDuplicate ScienceAlert Links with Article IDs:")
for link, articles in duplicate_sciencealert.items():
    print(f"{link}: {len(articles)} occurrences, found in articles: {articles}")

# Print total duplicates
print(f"\nTotal duplicates for DOIs: {total_duplicate_dois}")
print(f"Total duplicates for URLs: {total_duplicate_urls}")
print(f"Total duplicates for ScienceAlert links: {total_duplicate_sciencealert}")
