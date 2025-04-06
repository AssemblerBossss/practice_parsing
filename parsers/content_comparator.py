import re
from collections import defaultdict, Counter
from math import log

from loggers import setup_logger
from storage import DataStorage
from .comporator_config import (STOP_NGRAMS,
                                MIN_ABSOLUTE_THRESHOLD,
                                MIN_RELATIVE_THRESHOLD,
                                NGRAM_SIZE)


def preprocess_text(text: str) -> str:
    """
    - Приведение к нижнему регистру
    - Удаление всей пунктуации
    - Нормализацию пробелов (удаление лишних пробелов)
    - Удаление начальных/конечных пробелов
    """
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)  # Удаляем пунктуацию
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def generate_ngrams(text: str, n: int) -> list[str]:
    """
    Генерирует n-граммы из текста с фильтрацией стоп-фраз.
    1. Разбивает текст на слова
    2. Генерирует все возможные последовательности длины N
    3. Фильтрует n-граммы, присутствующие в STOP_NGRAMS
    """
    if n <= 0:
        raise ValueError("n must be positive integer")

    words = text.split()
    ngrams = [' '.join(words[i:i + n]) for i in range(len(words) - n + 1)]
    return [ngram for ngram in ngrams if ngram not in STOP_NGRAMS]


def compute_tfidf_weights(documents: list[dict]) -> dict[str, float]:
    """
    Вычисляет IDF веса для всех n-грамм в коллекции документов.

    Args:
        documents: Список документов, где каждый документ - это словарь,
                 содержащий текст в одном из полей: 'content', 'text' или 'title'.

    Returns:
        Словарь, где ключи - n-граммы, значения - их IDF веса.
    """
    # Инициализация счетчика встречаемости n-грамм в документах
    ngram_document_frequency = Counter()
    total_documents_count = len(documents)

    # Сбор статистики встречаемости n-грамм по всем документам
    for document in documents:
        # Извлечение текста из документа с учетом приоритета полей
        document_text = (document.get('content', '')
                         or document.get('text', '')
                         or document.get('title', ''))

        # Предобработка текста и генерация уникальных n-грамм
        processed_text = preprocess_text(document_text)
        document_ngrams = set(generate_ngrams(processed_text, NGRAM_SIZE))

        # Обновление счетчика документов, содержащих каждую n-грамму
        ngram_document_frequency.update(document_ngrams)

    # Вычисление IDF весов для каждой n-граммы
    inverse_document_frequency_weights = {
        ngram: log(total_documents_count / (document_frequency + 1))  # +1 для сглаживания
        for ngram, document_frequency in ngram_document_frequency.items()
    }

    return inverse_document_frequency_weights


def find_similar_posts(
        habr_posts: list[dict],
        telegram_posts: list[dict],
        tfidf_weights: dict[str, float]
) -> list[tuple]:
    """
    Находит схожие посты между Habr и Telegram на основе n-грамм с TF-IDF взвешиванием.

    Args:
        habr_posts: Список постов с Habr в формате словарей
        telegram_posts: Список постов из Telegram в формате словарей
        tfidf_weights: Словарь весов n-грамм (рассчитанный через compute_tfidf_weights)

    Returns:
        Список кортежей с информацией о найденных совпадениях:
        (платформа, заголовок_habr, дата_habr, id_telegram, дата_telegram,
         оценка_сходства, кол-во_нграмм_telegram, кол-во_нграмм_habr)
    """
    # Создаем индекс Habr постов по n-граммам
    habr_ngram_index = defaultdict(list)
    for habr_post in habr_posts:
        post_content = habr_post.get('content', '') or habr_post.get('title', '')
        processed_text = preprocess_text(post_content)
        post_ngrams = set(generate_ngrams(processed_text, NGRAM_SIZE))

        for ngram in post_ngrams:
            habr_ngram_index[ngram].append((habr_post, post_ngrams))

    # Поиск схожих постов в Telegram
    matched_posts = []

    for telegram_post in telegram_posts:
        telegram_text = preprocess_text(telegram_post.get('text', ''))
        telegram_ngrams = set(generate_ngrams(telegram_text, NGRAM_SIZE))

        # Храним совпадения с Habr постами
        habr_post_matches = defaultdict(float)
        processed_habr_posts = set()

        for ngram in telegram_ngrams:
            if ngram in habr_ngram_index:
                for habr_post, habr_post_ngrams in habr_ngram_index[ngram]:
                    post_identifier = (habr_post['title'], habr_post.get('date', ''))

                    if post_identifier not in processed_habr_posts:
                        # Вычисляем оценку сходства
                        common_ngrams = telegram_ngrams & habr_post_ngrams
                        similarity_score = sum(
                            tfidf_weights.get(ngram, 0)
                            for ngram in common_ngrams
                        )
                        habr_post_matches[post_identifier] = similarity_score
                        processed_habr_posts.add(post_identifier)

        # Фильтрация по порогу сходства
        for (habr_title, habr_date), score in habr_post_matches.items():
            min_ngrams_count = min(len(telegram_ngrams), len(habr_post_ngrams))
            similarity_threshold = max(
                MIN_ABSOLUTE_THRESHOLD,
                MIN_RELATIVE_THRESHOLD * min_ngrams_count
            )

            if score >= similarity_threshold:
                matched_posts.append((
                    'habr',
                    habr_title,
                    habr_date,
                    telegram_post.get('id', ''),
                    telegram_post.get('date', ''),
                    score,
                    len(telegram_ngrams),
                    len(habr_post_ngrams)
                ))

    return matched_posts


def main():
    try:
        habr_posts = DataStorage.read_json('habr')
        telegram_posts = DataStorage.read_json('telegram')
    except Exception as e:
        print(f"Ошибка загрузки данных: {e}")
        return

    # Предварительно вычисляем TF-IDF веса по всей коллекции
    all_posts = habr_posts + telegram_posts
    tfidf_weights = compute_tfidf_weights(all_posts)

    # Поиск схожих постов
    similar_posts = find_similar_posts(habr_posts, telegram_posts, tfidf_weights)

    # Вывод результатов
    print(
        f"Найдено {len(similar_posts)} пар схожих постов (порог: абсолютный={MIN_ABSOLUTE_THRESHOLD}, относительный={MIN_RELATIVE_THRESHOLD}):")
    print("-" * 100)
    for i, (source, h_title, h_date, t_id, t_date, score, t_len, h_len) in enumerate(similar_posts, 1):
        print(f"Пара #{i}:")
        print(f"Habr: '{h_title}' ({h_date}) | {h_len} n-грамм")
        print(f"Telegram (ID: {t_id}): {t_date} | {t_len} n-грамм")
        print(f"Взвешенная оценка схожести: {score:.2f}")
        print("-" * 100)


if __name__ == "__main__":
    main()