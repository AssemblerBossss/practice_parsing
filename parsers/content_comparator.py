import re
from collections import defaultdict, Counter
from math import log
from logging import DEBUG
from loggers import setup_logger
from storage import DataStorage
from parsers.comporator_config import (STOP_NGRAMS,
                                MIN_ABSOLUTE_THRESHOLD,
                                MIN_RELATIVE_THRESHOLD,
                                NGRAM_SIZE)
logger = setup_logger("comporator", log_file="comporator.log", log_level=DEBUG)

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

    Args:
        text: Исходный текст
        n: Размер n-граммы
    Returns: Массив n-грамм

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


def index_habr_posts(habr_posts: list[dict], ngram_size: int) -> dict[str, list[tuple[dict, set]]]:
    """
    Создает индекс постов Habr по n-граммам.

    Args:
        habr_posts: Список постов с Habr.
        ngram_size: Размер n-грамм.

    Returns:
        Индекс n-грамм для постов Habr.
    """
    habr_ngram_index = defaultdict(list)
    for habr_post in habr_posts:
        post_content = habr_post.get('content', '') or habr_post.get('title', '')
        processed_text = preprocess_text(post_content)
        post_ngrams = set(generate_ngrams(processed_text, ngram_size))

        for ngram in post_ngrams:
            habr_ngram_index[ngram].append((habr_post, post_ngrams))

    return habr_ngram_index


def find_matching_posts(
        telegram_posts: list[dict],
        habr_ngram_index: dict[str, list[tuple[dict, set]]],
        tfidf_weights: dict[str, float],
        ngram_size: int
) -> list[tuple]:
    """
    Находит уникальные соответствия между постами Telegram и Habr.
    Каждому посту Habr соответствует не более одного поста Telegram с максимальной оценкой схожести.
    Каждый пост Telegram может быть сопоставлен только с одним постом Habr.
    """
    # Словарь для хранения наилучших соответствий для каждого поста Habr
    habr_post_to_best_match = {}

    for telegram_post in telegram_posts:
        telegram_text = preprocess_text(telegram_post.get('text', ''))
        telegram_post_ngrams = set(generate_ngrams(telegram_text, ngram_size))

        # Временное хранилище совпадений для текущего поста Telegram
        current_telegram_matches = {}

        for ngram in telegram_post_ngrams:
            if ngram in habr_ngram_index:
                for habr_post, habr_post_ngrams in habr_ngram_index[ngram]:
                    habr_post_key = (habr_post['title'], habr_post.get('date', ''))

                    # Вычисляем оценку схожести
                    common_ngrams = telegram_post_ngrams & habr_post_ngrams
                    similarity_score = sum(
                        tfidf_weights.get(ngram, 0)
                        for ngram in common_ngrams
                    )

                    # Сохраняем лучшее соответствие для текущего поста Telegram
                    if (habr_post_key not in current_telegram_matches or
                            similarity_score > current_telegram_matches[habr_post_key][1]):
                        current_telegram_matches[habr_post_key] = (telegram_post, similarity_score)

        # Фильтруем по порогу и обновляем глобальные наилучшие соответствия
        for habr_post_key, (telegram_post, score) in current_telegram_matches.items():
            min_ngrams_count = min(len(telegram_post_ngrams), len(habr_post_ngrams))
            similarity_threshold = max(
                MIN_ABSOLUTE_THRESHOLD,
                MIN_RELATIVE_THRESHOLD * min_ngrams_count
            )

            if score >= similarity_threshold:
                # Обновляем если нашли лучшее соответствие для поста Habr
                if (habr_post_key not in habr_post_to_best_match or
                        score > habr_post_to_best_match[habr_post_key][2]):
                    habr_post_to_best_match[habr_post_key] = (
                        habr_post_key[0],  # title
                        habr_post_key[1],  # date
                        score,
                        telegram_post.get('id', ''),
                        telegram_post.get('date', ''),
                        len(telegram_post_ngrams),
                        len(habr_post_ngrams)
                    )

    # Формируем итоговый список уникальных соответствий
    unique_matches = []
    used_telegram_post_ids = set()

    # Сортируем соответствия по убыванию оценки схожести
    sorted_matches = sorted(habr_post_to_best_match.values(), key=lambda x: -x[2])

    for match in sorted_matches:
        habr_title, habr_date, score, telegram_id, telegram_date, telegram_ngram_count, habr_ngram_count = match

        # Проверяем что пост Telegram еще не использован
        if telegram_id not in used_telegram_post_ids:
            unique_matches.append((
                'habr',
                habr_title,
                habr_date,
                telegram_id,
                telegram_date,
                score,
                telegram_ngram_count,
                habr_ngram_count
            ))
            used_telegram_post_ids.add(telegram_id)

    return unique_matches

def find_similar_posts(
        habr_posts: list[dict],
        telegram_posts: list[dict],
        tfidf_weights: list[str, float],
        ngram_size: int = 3
) -> list[tuple]:
    """
    Находит схожие посты между Habr и Telegram на основе n-грамм с TF-IDF взвешиванием.

    Args:
        habr_posts: Список постов с Habr.
        telegram_posts: Список постов из Telegram.
        tfidf_weights: Словарь весов n-грамм.
        ngram_size: Размер n-грамм.

    Returns:
        Список кортежей с информацией о найденных совпадениях.
    """
    habr_ngram_index = index_habr_posts(habr_posts, ngram_size)
    matched_posts = find_matching_posts(telegram_posts, habr_ngram_index, tfidf_weights, ngram_size)
    return matched_posts


def comporator_start():
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
    logger.info("Найдено %d пар схожих постов (порог: абсолютный=%s, относительный=%s):",
                len(similar_posts),
                MIN_ABSOLUTE_THRESHOLD,
                MIN_RELATIVE_THRESHOLD)
    logger.info("-" * 100)
    for i, (source, h_title, h_date, t_id, t_date, score, t_len, h_len) in enumerate(similar_posts, 1):
        logger.debug(f"Пара #{i}:")
        logger.debug(f"Habr: '{h_title}' ({h_date}) | {h_len} n-грамм")
        logger.debug(f"Telegram (ID: {t_id}),: {t_date} | {t_len} n-грамм")
        logger.debug(f"Взвешенная оценка схожести: {score:.2f}")
        logger.debug("-" * 100)

    # Сохраняем найденные пары
    DataStorage.save_to_excel(similar_posts)

    # === ДОБАВЛЕНО: Сохранение неиспользованных Telegram постов ===
    matched_telegram_ids = {match[3] for match in similar_posts}  # Индекс 3 = telegram_id
    unmatched_telegram_posts = [
        post for post in telegram_posts if post.get('id', '') not in matched_telegram_ids
    ]

    logger.info("Не найдено пары для %d Telegram постов.", len(unmatched_telegram_posts))

    # Можно сохранить как JSON
    # DataStorage.write_json(unmatched_telegram_posts, 'unmatched_telegram')

    # Или дополнительно — как Excel, если нужно визуально посмотреть
    DataStorage.save_telegram_to_excel(unmatched_telegram_posts, filename='unmatched_telegram.xlsx')



if __name__ == "__main__":
    comporator_start()