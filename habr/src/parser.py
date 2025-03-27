import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from time import sleep
from fake_useragent import UserAgent

ua = UserAgent()


def get_author_articles(username, max_pages=2):
    base_url = "https://habr.com"
    articles = []

    for page in range(1, max_pages + 1):
        url = f"{base_url}/ru/users/{username}/posts/page{page}/"

        # Имитируем браузерные заголовки
        headers = {
            "User-Agent": ua.chrome,  # или ua.firefox, ua.opera
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
            "Referer": "https://habr.com/",
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            print(f"Страница {page}: статус {response.status_code}")

            if response.status_code != 200:
                print(f"Ошибка: {response.status_code}")
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            posts = soup.find_all('article', class_='tm-articles-list__item')

            if not posts:
                print("Статьи не найдены, возможно, страница закончилась.")
                break

            for post in posts:
                title_tag = post.find('a', class_='tm-title__link')
                if not title_tag:
                    continue

                title = title_tag.text.strip()
                link = urljoin(base_url, title_tag['href'])
                date = post.find('time')['datetime']

                articles.append({
                    'title': title,
                    'link': link,
                    'date': date
                })

            sleep(2)  # Задержка между запросами (чтобы не блокировали)

        except Exception as e:
            print(f"Ошибка при запросе: {e}")
            break

    return articles


if __name__ == "__main__":
    author_username = "DevFM"  # Убедитесь, что пользователь существует!
    articles = get_author_articles(author_username)

    for idx, article in enumerate(articles, 1):
        print(f"{idx}. {article['title']}")
        print(f"   Дата: {article['date']}")
        print(f"   Ссылка: {article['link']}\n")