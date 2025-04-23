import json
import torch
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
from itertools import combinations

from storage import DataStorage

# === Глобальные переменные ===
EMBEDDINGS_CACHE = {}
VISITED_POSTS_CACHE = {}

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# === Загрузка модели SentenceTransformers с прогрессом ===
print("🔄 Загрузка модели SentenceTransformers...")
# Используем модель paraphrase-multilingual-MiniLM-L12-v2
model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
model = model.to(DEVICE)
print("✅ Модель загружена.")

# Функция для получения эмбеддингов текста с возможностью кэширования
def get_embedding(text, post_id=None):
    """
    Получаем эмбеддинг текста с возможностью кэширования.
    Если пост уже был обработан, возвращаем его эмбеддинг из кэша.
    """
    if post_id and post_id in EMBEDDINGS_CACHE:
        return EMBEDDINGS_CACHE[post_id]

    emb = model.encode(text, device=str(DEVICE))

    if post_id:
        EMBEDDINGS_CACHE[post_id] = emb

    return emb

# Функция для вычисления схожести между двумя текстами
def calculate_similarity(text1, text2):
    emb1 = get_embedding(text1)
    emb2 = get_embedding(text2)
    return cosine_similarity([emb1], [emb2])[0][0]

# Удаление дубликатов в Telegram постах
def remove_telegram_duplicates(telegram_posts, threshold=0.97):
    filtered_posts = []
    seen = set()

    print("🧹 Удаление дубликатов из Telegram-постов...")

    for i, post_i in enumerate(tqdm(telegram_posts)):
        post_id_i = post_i['id']

        # Кэшируем эмбеддинги для каждого поста
        emb_i = get_embedding(post_i['text'], post_id_i)

        if i in seen:
            continue
        best_j = i
        for j in range(i + 1, len(telegram_posts)):
            post_j = telegram_posts[j]
            post_id_j = post_j['id']

            # Получаем эмбеддинги из кэша или вычисляем заново
            emb_j = get_embedding(post_j['text'], post_id_j)
            sim = cosine_similarity([emb_i], [emb_j])[0][0]

            if sim > threshold:
                views_j = post_j.get('views') or 0
                views_best = telegram_posts[best_j].get('views') or 0
                best_j = j if views_j > views_best else best_j
                seen.add(j)

        filtered_posts.append(telegram_posts[best_j])
        seen.add(best_j)

    return filtered_posts

# Сопоставление постов между Habr и Telegram
def match_posts(habr_posts, telegram_posts, threshold=0.9):
    matches = []
    unmatched_habr = []
    used_telegram_ids = set()

    print("🔍 Сопоставление постов...")

    for habr in tqdm(habr_posts):
        best_match = None
        best_score = 0

        habr_content = habr['content']
        for tele in telegram_posts:
            if tele['id'] in used_telegram_ids:
                continue

            score = calculate_similarity(habr_content, tele['text'])
            if score > best_score:
                best_score = score
                best_match = tele

        if best_score >= threshold:
            matches.append({
                "habr_title": habr['title'],
                "habr_date": habr['date'],
                "telegram_id": best_match['id'],
                "telegram_date": best_match['date'],
                "similarity": best_score,
                "habr_text": habr['content'],
                "telegram_text": best_match['text']
            })
            used_telegram_ids.add(best_match['id'])
        else:
            unmatched_habr.append(habr)

    return matches, unmatched_habr

# Сохранение результатов в Excel
def save_to_excel(matched, unmatched, matched_path='matched_posts.xlsx', unmatched_path='unmatched_habr.xlsx'):
    matched_df = pd.DataFrame(matched)
    unmatched_df = pd.DataFrame(unmatched)

    matched_df.to_excel(matched_path, index=False)
    unmatched_df.to_excel(unmatched_path, index=False)
    print(f"✅ Сопоставленные пары записаны в {matched_path}")
    print(f"📄 Несопоставленные хабр-посты записаны в {unmatched_path}")

# === Загрузка данных ===
habr_posts = DataStorage.read_json('habr')
telegram_posts = DataStorage.read_json('telegram')

# === Предобработка ===
telegram_posts = remove_telegram_duplicates(telegram_posts)

# === Сопоставление постов ===
matched, unmatched = match_posts(habr_posts, telegram_posts)

# === Сохранение ===
save_to_excel(matched, unmatched)
