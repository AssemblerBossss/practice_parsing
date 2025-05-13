import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from storage import DataStorage
from models import PikabuPostModel
from loggers import setup_logger, DEFAULT_PIKABU_LOG_FILE

# Параметры ниже зависят от качества интернет соединения(если интернет-соединение хорошее - можно уменьшить параметры)
SCROLL_NUM = 20             # Кол-во скроллов для прогрузки содержимого (Оптимально - 5)
DELAY_BETWEEN_REQUESTS = 3  # Время - задержка между скроллами (Оптимально - 5)
COMMENT_EXPAND_DELAY = 5
MAX_RETRIES = 10

logger = setup_logger('pikabu_logger', log_file=DEFAULT_PIKABU_LOG_FILE)


def extract_date(date_str: str) -> str | None:
    """Преобразует ISO-дату в формат YYYY-MM-DD"""
    try:
        return datetime.fromisoformat(date_str).strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None
#
#
# def expand_comment_branches(driver):
#     """Рекурсивно раскрывает все уровни вложенных комментариев"""
#     retries = 0
#     last_count = 0
#
#     while retries < MAX_RETRIES:
#         # Ищем все доступные кнопки раскрытия
#         buttons = driver.find_elements(
#             By.CSS_SELECTOR,
#             'a.comment-toggle-children_collapse:not([style*="display: none"])'
#         )
#
#         if not buttons or len(buttons) == last_count:
#             retries += 1
#             time.sleep(COMMENT_EXPAND_DELAY)
#             continue
#
#         last_count = len(buttons)
#         retries = 0
#
#         for btn in buttons:
#             try:
#                 # Прокрутка и клик с ожиданием загрузки
#                 driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
#                 WebDriverWait(driver, 10).until(EC.element_to_be_clickable(btn))
#                 btn.click()
#
#                 # Ожидание появления дочерних комментариев
#                 WebDriverWait(driver, 10).until(
#                     EC.presence_of_element_located(
#                         (By.CSS_SELECTOR, ".comment__children:not([hidden])")
#                     )
#                 )
#                 time.sleep(COMMENT_EXPAND_DELAY)
#
#
#             except (TimeoutException, StaleElementReferenceException) as e:
#                 logger.warning(f"Ошибка при раскрытии комментария: {str(e)}")
#                 continue
#         # Рекурсивный вызов для новых уровней
#         expand_comment_branches(driver)
#
#
# def parse_comments(soup):
#     """Рекурсивный парсинг комментариев с учетом вложенности"""
#     comments = []
#
#     for comment in soup.select('.comment'):
#         try:
#             # Базовые данные
#             comment_data = {
#                 'id': comment.get('id'),
#                 'author': comment.select_one('.user__nick').text.strip(),
#                 'text': comment.select_one('.comment__content').text.strip(),
#                 'date': comment.select_one('.comment__datetime').get('datetime'),
#                 'rating': comment.select_one('.comment__rating-count').text.strip(),
#                 'replies': []
#             }
#
#             # Рекурсивный парсинг ответов
#             children_div = comment.select_one('.comment__children:not([hidden])')
#             if children_div:
#                 replies_soup = BeautifulSoup(children_div.prettify(), 'html.parser')
#                 comment_data['replies'] = parse_comments(replies_soup)
#
#             comments.append(comment_data)
#
#         except Exception as e:
#             continue
#
#     return comments
#

# def parse_post_comments(driver, post_url):
#     driver.get(post_url)
#     time.sleep(2)
#
#     try:
#         # Раскрываем все ветки
#         expand_comment_branches(driver)
#
#         # Получаем обновленный HTML
#         soup = BeautifulSoup(driver.page_source, 'html.parser')
#         return parse_comments(soup)
#
#     except Exception as e:
#         print(f"Ошибка парсинга: {e}")
#         return []


def parse_user_profile() -> list[PikabuPostModel]:
    profile_name = input("Введите имя профиля Pikabu: ")
    profile_url = f'https://pikabu.ru/@{profile_name}'

    # настр-ка параметров бразера Chrome
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    driver.get(profile_url)

    try:
        # Имитация скроллинга
        last_count = 0
        pause_count = 0
        MAX_SCROLL_PAUSE = 10
        SCROLL_TIMEOUT = 1

        while pause_count < MAX_SCROLL_PAUSE:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_TIMEOUT)

            soup = BeautifulSoup(driver.page_source, 'lxml')
            current_count = len(soup.find_all('article', class_='story'))

            if current_count == last_count:
                pause_count += 1
                logger.debug(
                    f"Новых постов не найдено, пауза  %s",
                   pause_count / MAX_SCROLL_PAUSE
                )
            else:
                pause_count = 0
                last_count = current_count
                logger.debug(f"Обнаружено постов: {current_count}")

        # парсинг содержимого страницы
        soup = BeautifulSoup(driver.page_source, 'lxml')
        stories: list[PikabuPostModel] = []

        # поиск всех статей
        count_posts = 0
        for story in soup.find_all('article', class_='story'):
            count_posts += 1
            title_elem = story.find('h2', class_='story__title')
            title = title_elem.text.strip()[:-2] if title_elem else None

            link_elem = story.find('a', class_='story__title-link')
            link = urljoin(profile_url, link_elem['href']) if link_elem else None

            content_elem = story.find('div', class_='story__content-inner')
            content = content_elem.get_text(separator='\n', strip=True) if content_elem else None

            date_elem = story.find('time', class_='story__datetime')
            date = extract_date(date_elem['datetime']) if date_elem else None

            rating_elem = story.find('div', class_='story__rating-count')
            rating = rating_elem.text.strip() if rating_elem else None

            if (title is None) and (link is None):
                continue

            # comments = parse_post_comments(driver, link)
            # print(comments)

            post = PikabuPostModel(
                id=count_posts,
                title=title,
                post_url=link,
                content=content,
                date=date,
                rating=rating,
                url_profile=profile_url,
            )
            stories.append(post)
            logger.debug(f"Статья #{count_posts} успешно обработана: {title}")
        if stories:
            logger.info(f"Успешно обработано {len(stories)} статей из {count_posts}")
            DataStorage.save_as_json(stories, 'pikabu', channel_url=stories[0].url_profile)
            logger.info("Данные сохранены в JSON файл")
            return stories
        else:
            logger.warning("Не удалось обработать ни одной статьи")
    except Exception as e:
        logger.error(f"Критическая ошибка при парсинге профиля: {str(e)}", exc_info=True)
    finally:
        driver.quit()


if __name__ == "__main__":
    parse_user_profile()
