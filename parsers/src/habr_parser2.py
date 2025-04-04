import aiohttp
import asyncio
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from typing import Optional, List, Dict

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

    async def parse_page(self, html: str) -> List[Dict[str, str]]:
        soup = BeautifulSoup(html, 'lxml')
        posts = soup.find_all('article', class_='tm-articles-list__item_no-padding')

        for post in posts:
            try:
                title_tag = posts.find('strong') if posts.find('strong') else None
                time_tag = posts.find('time') or posts.find('span', class_='tm-publication-date')

                content = ''

                for p in post.find_all('p'):
                    content += p.get_text(separator=" ")

                if not title_tag or not time_tag:
                    self.logger.warning("Не найдены теги в статье")
                    continue

                self.articles.append({
                    'title': title_tag.text.strip(),
                    'date': time_tag['datetime'] if 'datetime' in time_tag.attrs else time_tag.text.strip(),
                    'content': content
                })
                self.logger.info(f"Найдена статья: {title_tag.text.strip()}")
            except Exception as e:
                logger.error(f"Ошибка обработки статьи: {str(e)}")
                logger.debug(f"Проблемная статья:\n{post.prettify()[:300]}...")
                continue
