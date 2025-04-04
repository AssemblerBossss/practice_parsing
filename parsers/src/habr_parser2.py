import aiohttp
import asyncio
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from loggers import setup_logger, DEFAULT_HABR_LOG_FILE
from storage import DataStorage


class HabrParser:
    def __init__(self, username: str, max_pages: int = 2):
        self.username = username
        self.max_pages = max_pages
        self.base_url = 'https://habr.com/'
        self.articles = []
        self.ua = UserAgent()
        self.logger = setup_logger('habr_logger', log_file='habr')

    async def (self):