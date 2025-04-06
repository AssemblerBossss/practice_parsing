import aiohttp
import asyncio
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from typing import Optional, List, Dict
from hashlib import md5

from loggers import setup_logger, DEFAULT_HABR_LOG_FILE
from storage import DataStorage
from storage.data_storage import logger


class HabrParser:
    def __init__(self, username: str, max_pages: int = 2):
        self.username = username
        self.max_pages = max_pages
        self.base_url = "https://habr.com"
        self.articles = []
        self.ua = UserAgent()
        self.unique_hashes = set()
        self.logger = setup_logger('habr_logger', log_file=DEFAULT_HABR_LOG_FILE)
        self.session = None
        self.headers = {
            "User-Agent": self.ua.chrome,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }

    def _get_content_hash(self, content: str) -> str:
        """Генерирует MD5 хеш контента статьи"""
        return md5(content.strip().encode("utf-8")).hexdigest()

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
        """Загружает указанную страницу статей автора.

        Возвращает HTML-текст страницы или None при ошибке.
        """
        url = f"{self.base_url}/ru/users/{self.username}/posts/page{page}/"

        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    self.logger.error(f"Ошибка HTTP: {response.status}")
                    return None
                self.logger.info(f"Страница {page}: статус {response.status}")
                return await response.text()
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке страницы {page}: {str(e)}")
            return None

    async def parse_page(self, html: str) -> List[Dict[str, str]]:
        """Извлекает данные статей из HTML.
        Возвращает список словарей с title, date и content.
        Продолжает работу при ошибках парсинга отдельных статей."""

        soup = BeautifulSoup(html, 'lxml')
        posts = soup.find_all('article', class_='tm-articles-list__item_no-padding')
        articles = []
        for post in posts:
            try:
                title_tag = post.find('strong') if post.find('strong') else None
                time_tag = post.find('time') or post.find('span', class_='tm-publication-date')

                content = ''

                for p in post.find_all('p'):
                    content += p.get_text(separator=" ")

                if not title_tag or not time_tag:
                    self.logger.warning("Не найдены теги в статье")
                    continue

                # Проверка на дубликат
                content_hash: str = self._get_content_hash(content)
                if content_hash in self.unique_hashes:
                    self.logger.warning(f"Найден дубликат статьи: {title_tag.text.strip()}")
                    continue

                articles.append({
                    'title': title_tag.text.strip(),
                    'date': time_tag['datetime'] if 'datetime' in time_tag.attrs else time_tag.text.strip(),
                    'content': content
                })
                self.logger.info(f"Найдена статья: {title_tag.text.strip()}")
            except Exception as e:
                logger.error(f"Ошибка обработки статьи: {str(e)}")
                logger.debug(f"Проблемная статья:\n{post.prettify()[:300]}...")
                continue
        return articles

    async def get_articles(self) -> List[Dict[str, str]]:
        for page in range(1, self.max_pages + 1):
            html = await self.fetch_page(page)
            if not html:
                continue

            articles = await self.parse_page(html)
            if not articles:
                logger.warning(f"Не найдено статей на странице {page}")
                continue

            self.articles.extend(articles)
            DataStorage.save_as_json(self.articles, 'habr')
            await asyncio.sleep(2)
        return self.articles


async def start_habr(username: str = 'DevFM'):
    async with HabrParser(username) as parser:
        articles = await parser.get_articles()

        if not articles:
            logger.warning("Не найдено ни одной статьи! Проверьте:")
        else:
            logger.info(f"\nНайдено статей: {len(articles)}")


if __name__ == "__main__":
    asyncio.run(start_habr())
