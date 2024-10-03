# base_classes.py

from abc import ABC, abstractmethod


class BaseScraper(ABC):
    def __init__(self, json_input_file, json_output_file, headers):
        self.json_input_file = json_input_file
        self.json_output_file = json_output_file
        self.headers = headers
        self.article_collection = None  # Will be loaded in subclass
        self.output_data = []
        self.cached_articles = {}

    @abstractmethod
    async def load_json_data(self):
        pass

    @abstractmethod
    async def load_existing_output(self):
        pass

    @abstractmethod
    async def fetch_article_details(self, session, url):
        pass

    @abstractmethod
    async def worker(self, session, queue):
        pass

    @abstractmethod
    async def run(self):
        pass

    @abstractmethod
    async def save_to_json(self):
        pass
    @abstractmethod
    def clean_doi(self, doi_url):
        pass


class BaseCleaner(ABC):
    @abstractmethod
    def canonical_url(self, url):
        pass

    @abstractmethod
    async def check_redirect(self, url, session):
        pass



    @abstractmethod
    def extract_doi_from_url(self, url):
        pass

    @abstractmethod
    def clean_doi_urls(self, doi_urls):
        pass
