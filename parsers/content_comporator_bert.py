import json
import torch
import re
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
model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
model = model.to(DEVICE)
print("✅ Модель загружена.")

from razdel import sentenize

def normalize_text(text):
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip().lower()

def get_embeddings_for_posts(posts, key='text'):
    texts = [normalize_text(post[key]) for post in posts]
    with torch.no_grad():
        embeddings = model.encode(texts, batch_size=16, show_progress_bar=True, device=str(DEVICE))
    return embeddings

# Удаление дубликатов в Telegram постах
def remove_telegram_duplicates(telegram_posts, threshold=0.90):
    filtered_posts = []
    seen = set()

    print("🧹 Удаление дубликатов из Telegram-постов...")

    embeddings = get_embeddings_for_posts(telegram_posts, key='text')

    for i, post_i in enumerate(tqdm(telegram_posts)):
        if i in seen:
            continue
        best_j = i
        for j in range(i + 1, len(telegram_posts)):
            if j in seen:
                continue
            sim = cosine_similarity([embeddings[i]], [embeddings[j]])[0][0]
            if sim > threshold:
                views_j = telegram_posts[j].get('views') or 0
                views_best = telegram_posts[best_j].get('views') or 0
                best_j = j if views_j > views_best else best_j
                seen.add(j)

        filtered_posts.append(telegram_posts[best_j])
        seen.add(best_j)

    return filtered_posts

# Сопоставление постов между Habr и Telegram
def match_posts(habr_posts, telegram_posts, threshold=0.65):
    matches = []
    unmatched_habr = []
    used_telegram_ids = set()

    #print("\ud83d\udd0d \u0421\u043e\u043f\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u043f\u043e\u0441\u0442\u043e\u0432...")

    habr_embeddings = get_embeddings_for_posts(habr_posts, key='content')
    telegram_embeddings = get_embeddings_for_posts(telegram_posts, key='text')

    for i, habr in enumerate(tqdm(habr_posts)):
        best_match_idx = None
        best_score = 0

        for j, tele in enumerate(telegram_posts):
            if tele['id'] in used_telegram_ids:
                continue

            score = cosine_similarity([habr_embeddings[i]], [telegram_embeddings[j]])[0][0]
            if score > best_score:
                best_score = score
                best_match_idx = j

        if best_score >= threshold:
            best_match = telegram_posts[best_match_idx]
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

from openpyxl.utils import get_column_letter

def auto_adjust_column_width(ws, dataframe):
    for i, column in enumerate(dataframe.columns, 1):
        max_length = max(
            [len(str(cell)) for cell in dataframe[column].values] + [len(column)]
        )
        adjusted_width = min(max_length + 2, 100)
        ws.column_dimensions[get_column_letter(i)].width = adjusted_width

def save_to_excel(matched, unmatched, matched_path='matched_posts.xlsx', unmatched_path='unmatched_habr.xlsx'):
    matched_df = pd.DataFrame(matched)
    unmatched_df = pd.DataFrame(unmatched)
    matched_df['telegram_text'] = matched_df['telegram_text'].str.replace('#', '', regex=False)
    matched_df['habr_text'] = matched_df['habr_text'].str.replace('#', '', regex=False)

    with pd.ExcelWriter(matched_path, engine='openpyxl') as writer:
        matched_df.to_excel(writer, index=False, sheet_name='Matched')
        ws = writer.sheets['Matched']
        auto_adjust_column_width(ws, matched_df)

    with pd.ExcelWriter(unmatched_path, engine='openpyxl') as writer:
        unmatched_df.to_excel(writer, index=False, sheet_name='Unmatched')
        ws = writer.sheets['Unmatched']
        auto_adjust_column_width(ws, unmatched_df)

    print(f"✅ Сопоставленные пары записаны в {matched_path}")
    print(f"📄 Несопоставленные хабр-посты записаны в {unmatched_path}")

# === Загрузка данных ===
habr_posts = DataStorage.read_json('habr')
telegram_posts = DataStorage.read_json('telegram')

# === \u041f\u0440\u0435\u0434\u043e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 ===
telegram_posts = remove_telegram_duplicates(telegram_posts)

# === \u0421\u043e\u043f\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u043f\u043e\u0441\u0442\u043e\u0432 ===
matched, unmatched = match_posts(habr_posts, telegram_posts)

# === \u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u0438\u0435 ===
save_to_excel(matched, unmatched)