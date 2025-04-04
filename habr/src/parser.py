import requests
from bs4 import BeautifulSoup
from time import sleep
from fake_useragent import UserAgent
from loggers import setup_logger
from storage import DataStorage

ua = UserAgent()
logger = setup_logger("habr_logger")


def get_author_posts(username: str, max_pages: int = 2) -> list[dict[str, str]]:
    """Парсинг статей автора с Хабра"""
    base_url = "https://habr.com"
    articles = []

    for page in range(1, max_pages + 1):
        url = f"{base_url}/ru/users/{username}/posts/page{page}/"

        headers = {
            "User-Agent": ua.chrome,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            logger.info(f"Страница {page}: статус {response.status_code}")
            #logger.info(response.text)

            if response.status_code != 200:
                logger.error(f"Ошибка HTTP: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            posts = soup.find_all('article', class_='tm-articles-list__item_no-padding')

            if not posts:
                logger.warning(f"Не найдено статей на странице {page}")
                #logger.debug(f"HTML страницы:\n{response.text[:500]}...")  # Логируем часть HTML
                continue

            for post in posts:
                try:
                    title_tag = post.find('strong') if  post.find('strong') else None
                    time_tag = post.find('time') or post.find('span', class_='tm-publication-date')
                    content = ""

                    #print("\nСпособ 2 (по абзацам):")
                    for p in post.find_all('p'):
                        content += p.get_text(separator=" ")

                    if not title_tag or not time_tag:
                        #logger.warning(f"Не найдены теги в статье:\n{post.prettify()[:300]}...")
                        logger.warning(f"Не найдены теги в статье:")
                        continue

                    #article_url = urljoin(base_url, title_tag['href'])
                    articles.append({
                        'title': title_tag.text.strip(),
                        #'link': article_url,
                        'date': time_tag['datetime'] if 'datetime' in time_tag.attrs else time_tag.text.strip(),
                        'content': content
                    })


                    logger.info(f"Найдена статья: {title_tag.text.strip()}")

                except Exception as e:
                    logger.error(f"Ошибка обработки статьи: {str(e)}")
                    logger.debug(f"Проблемная статья:\n{post.prettify()[:300]}...")
                    continue
            DataStorage.save_as_json(articles, "habr.json")
            sleep(2)

        except Exception as e:
            logger.error(f"Ошибка при обработке страницы {page}: {str(e)}")
            continue

    return articles


if __name__ == "__main__":
    author_username = "DevFM"
    logger.info(f"Начинаем парсинг статей автора {author_username}")

    articles = get_author_posts(author_username)

    if not articles:
        logger.warning("Не найдено ни одной статьи! Проверьте:")
    else:
        logger.info(f"\nНайдено статей: {len(articles)}")
        # for idx, article in enumerate(articles, 1):
            # print(f"\n{idx}. {article['title']}")
            # print(f"   Дата: {article['date']}")
            # print(f"   Содержание: {article['content']}")
            #print(f"   Ссылка: {article['link']}")