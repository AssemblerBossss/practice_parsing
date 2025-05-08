import asyncio
import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from typing import Optional, List, Dict
from hashlib import md5

from loggers import setup_logger, DEFAULT_HABR_LOG_FILE
from storage import DataStorage
from storage.data_storage import logger
from models import HabrPost


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

    @staticmethod
    def _get_content_hash(content: str) -> str:
        """
        Генерирует MD5 хеш контента статьи
        :param content: Текст статьи
        """
        return md5(content.strip().encode("utf-8")).hexdigest()

    def _is_duplicate(self, content: str) -> bool:
        """
        Проверяет, является ли статья дубликатом
        :param content: Текст статьи
        """
        content_hash = self._get_content_hash(content)
        if content_hash in self.unique_hashes:
            return True
        self.unique_hashes.add(content_hash)
        return False

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
        """
        Загружает указанную страницу статей автора.
        :param page: Номер страницы
        """
        url = f"{self.base_url}/ru/users/{self.username}/posts/page{page}/"

        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    self.logger.error("Ошибка HTTP: %d", response.status)
                    return None

                self.logger.debug("Страница %d: статус %d", page, response.status)
                return await response.text()
        except Exception as e:
            self.logger.error("Ошибка при загрузке страницы %d: %s", page, str(e))
            return None

    def parse_page(self, html: str) -> List[Dict[str, str]]:
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

                content = "\n".join(p.get_text(separator=' ') for p in post.find_all('p'))

                if not title_tag or not time_tag:
                    self.logger.warning("Не найдены обязательные теги в статье")
                    continue

                if self._is_duplicate(content):
                    self.logger.warning(f"Найден дубликат статьи: {title_tag.text.strip()}")
                    continue

                article = HabrPost(
                    title=title_tag.text.strip(),
                    date=time_tag['datetime'] if 'datetime' in time_tag.attrs else time_tag.text.strip(),
                    content=content
                )
                articles.append(article)

                self.logger.info("Найдена статья: %s", article.title)
            except Exception as e:
                logger.error("Ошибка обработки статьи: %s", str(e))
                continue
        return articles

    async def get_articles(self) -> List[Dict[str, str]]:
        """Загружает статьи со всех страниц параллельно"""
        tasks = [self.fetch_page(page) for page in range(1, self.max_pages + 1)]
        html_pages = await asyncio.gather(*tasks)

        for page, html in enumerate(html_pages, start=1):
            if not html:
                continue

            articles = self.parse_page(html)
            if not articles:
                self.logger.warning("Не найдено статей на странице %d", page)
                continue

            self.articles.extend(articles)
            DataStorage.save_as_json(self.articles, 'habr', channel_url="")

        return self.articles


async def start_habr(username: str = 'DevFM'):
    """
    Основная функция для запуска парсинга статей.

    Args:
        username: Логин автора на Habr (по умолчанию 'DevFM')
    """
    async with HabrParser(username) as parser:
        task = asyncio.create_task(parser.get_articles())
        await asyncio.gather(task)
        articles = task.result()

        if not articles:
            logger.warning("Не найдено ни одной статьи! Проверьте:")
        else:
            logger.info("Найдено статей: %d", len(articles))


if __name__ == "__main__":
    asyncio.run(start_habr())
