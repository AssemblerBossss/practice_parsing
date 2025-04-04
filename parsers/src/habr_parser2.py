import aiohttp
import asyncio
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from typing import Optional

from loggers import setup_logger, DEFAULT_HABR_LOG_FILE
from storage import DataStorage
from storage.data_storage import logger


class HabrParser:
    def __init__(self, username: str, max_pages: int = 2):
        self.username = username
        self.max_pages = max_pages
        self.base_url = 'https://habr.com/'
        self.articles = []
        self.ua = UserAgent()
        self.logger = setup_logger('habr_logger', log_file='habr')
        self.session = None
        self.headers = {
            "User-Agent": self.ua.chrome,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }

    async def __aenter__(self):
        """Инициализирует асинхронную HTTP-сессию при входе в контекстный блок"""

        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=10))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Вызывается при выходе из блока async with
           Закрывает HTTP-сессию (aiohttp.ClientSession) """

        if self.session:
            await self.session.close()

    async def fetch_page(self, page: int) -> Optional[str]:
        url = f"{self.base_url}/ru/users/{self.username}/posts/page{page}/"
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    self.logger.error(f"Ошибка HTTP: {response.status}")
                    return None
                logger.info(f"Страница {page}: статус {response.status}")
                return await response.text()
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке страницы {page}: {str(e)}")
            return None
