import json
from collections import defaultdict, Counter
import re
from math import log
from typing import List, Dict, Tuple, Set

# Конфигурация
STOP_NGRAMS = {"как я", "в этой", "для того чтобы", "что", "как", "на", "в", "и", "с"}  # Пример стоп-нграмм
MIN_ABSOLUTE_THRESHOLD = 60  # Минимальное абсолютное число совпадений
MIN_RELATIVE_THRESHOLD = 0.9  # 5% от длины меньшего поста
NGRAM_SIZE = 3  # Размер n-граммы


def preprocess_text(text: str) -> str:
    """Предварительная обработка текста"""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)  # Удаляем пунктуацию
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def generate_ngrams(text: str, n: int) -> List[str]:
    """Генерация n-грамм с фильтрацией стоп-фраз"""
    words = text.split()
    ngrams = [' '.join(words[i:i + n]) for i in range(len(words) - n + 1)]
    return [ngram for ngram in ngrams if ngram not in STOP_NGRAMS]


def compute_tfidf_weights(posts: List[Dict]) -> Dict[str, float]:
    """Вычисление TF-IDF весов для всех n-грамм в коллекции"""
    ngram_doc_freq = Counter()
    total_docs = len(posts)

    # Считаем DF (document frequency) для каждой n-граммы
    for post in posts:
        content = post.get('content', '') or post.get('text', '') or post.get('title', '')
        text = preprocess_text(content)
        ngrams = set(generate_ngrams(text, NGRAM_SIZE))
        ngram_doc_freq.update(ngrams)

    # Вычисляем IDF
    tfidf_weights = {}
    for ngram, df in ngram_doc_freq.items():
        idf = log(total_docs / (df + 1))  # +1 чтобы избежать деления на 0
        tfidf_weights[ngram] = idf

    return tfidf_weights


def find_similar_posts(habr_posts: List[Dict], telegram_posts: List[Dict], tfidf_weights: Dict[str, float]) -> List[
    Tuple]:
    """Поиск схожих постов с динамическим порогом и TF-IDF взвешиванием"""
    # Индексируем посты с Habr
    habr_index = defaultdict(list)
    for post in habr_posts:
        content = post.get('content', '') or post.get('title', '')
        text = preprocess_text(content)
        ngrams = set(generate_ngrams(text, NGRAM_SIZE))
        for ngram in ngrams:
            habr_index[ngram].append((post, ngrams))

    # Ищем схожие посты в Telegram
    similar_posts = []
    for t_post in telegram_posts:
        t_text = preprocess_text(t_post.get('text', ''))
        t_ngrams = set(generate_ngrams(t_text, NGRAM_SIZE))

        # Считаем взвешенные совпадения для каждого поста Habr
        habr_matches = defaultdict(float)
        matched_posts = set()

        for ngram in t_ngrams:
            if ngram in habr_index:
                for h_post, h_ngrams in habr_index[ngram]:
                    post_key = (h_post['title'], h_post.get('date', ''))
                    if post_key not in matched_posts:
                        # Вычисляем пересечение n-грамм с учетом весов
                        common_ngrams = t_ngrams & h_ngrams
                        score = sum(tfidf_weights.get(ng, 0) for ng in common_ngrams)
                        habr_matches[post_key] = score
                        matched_posts.add(post_key)

        # Применяем динамический порог
        for (h_title, h_date), score in habr_matches.items():
            min_length = min(len(t_ngrams), len(h_ngrams))
            relative_threshold = max(
                MIN_ABSOLUTE_THRESHOLD,
                MIN_RELATIVE_THRESHOLD * min_length
            )

            if score >= relative_threshold:
                similar_posts.append((
                    'habr', h_title, h_date,
                    t_post.get('id', ''), t_post.get('date', ''),
                    score, len(t_ngrams), len(h_ngrams)
                ))

    return similar_posts


def main():
    try:
        with open('habr.json', 'r', encoding='utf-8') as f:
            habr_posts = json.load(f).get('posts', [])

        with open('telegram.json', 'r', encoding='utf-8') as f:
            telegram_posts = json.load(f).get('posts', [])
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