# data_models.py

import json
import aiofiles
from dataclasses import dataclass, field, asdict
from typing import List, Optional


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
