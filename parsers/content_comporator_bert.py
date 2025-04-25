import re
import torch
from logging import DEBUG
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

from storage import DataStorage
from loggers import setup_logger

logger = setup_logger(__name__, log_file="content_comporator_bert.log", log_level=DEBUG)

EMBEDDINGS_CACHE:    dict[str, torch.tensor] = {}
VISITED_POSTS_CACHE: dict[str, bool] = {}


class PostMatcher:
    def __init__(self, threshold_duplicate_: float = 0.9, threshold_match_: float = 0.65):
        self.threshold_duplicate = threshold_duplicate_
        self.threshold_match = threshold_match_

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        logger.info("🔄 Загрузка модели SentenceTransformers...")
        self.model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
        self.model = self.model.to(self.device)
        logger.info("✅ Модель загружена.")

    @staticmethod
    def normalize_text(text: str) -> str:
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'#', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip().lower()

    def get_embeddings_for_posts(self, posts: list[dict], key: str ='text') -> list[torch.Tensor]:
        texts = [self.normalize_text(post[key]) for post in posts]
        with torch.no_grad():
             return self.model.encode(
                 texts, batch_size=16, show_progress_bar=True, ddevice=str(self.device)
             )

    def remove_telegram_duplicates(self, telegram_posts: list[dict], threshold=0.90) -> list[dict]:
        logger.info("🧹 Удаление дубликатов из Telegram-постов...")
        filtered_posts = []
        seen = set()

        embeddings = self.get_embeddings_for_posts(telegram_posts, key='text')

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


    def match_posts(self, habr_posts: list[dict], telegram_posts: list[dict]):
        logger.info(f"📥 Получено {len(habr_posts)} постов из Habr и {len(telegram_posts)} из Telegram.")
        logger.info("🔍 Сопоставление постов Habr и Telegram...")

        matches = []
        unmatched_habr = []
        unmatched_telegram = []
        used_telegram_ids = set()

        habr_embeddings = self.get_embeddings_for_posts(habr_posts, key='content')
        telegram_embeddings = self.get_embeddings_for_posts(telegram_posts, key='text')

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

            if best_score >= self.threshold_match:
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

                logger.debug(f"# Найдена пара #:")
                logger.debug("Habr:  %s:  %s ", habr['title'], habr['date'])
                logger.debug(f"Telegram (ID: %s),: %s", best_match['id'],  best_match['date'])
                logger.debug(f"Оценка схожести: %s", {best_score:.2})
                logger.debug("-" * 100)
            else:
                unmatched_habr.append(habr)
        logger.info(f"✅ Сопоставлено {len(matches)} пар.")
        logger.info(f"❌ Не найдено пары для {len(unmatched_habr)} habr-постов.")

        logger.info("🔍 Поиск постов Telegram, которым не нашлось пары...")
        for post in tqdm(telegram_posts):
            if post.get('id') not in used_telegram_ids:
                unmatched_telegram.append(post)
        logger.info(f"❌ Не найдено пары для {len(unmatched_telegram)} telegram-постов.")

        return matches, unmatched_habr, unmatched_telegram

def start():
    habr_posts = DataStorage.read_json('habr')

    telegram_posts = DataStorage.read_json('telegram')

    matcher = PostMatcher()
    telegram_posts = matcher.remove_telegram_duplicates(telegram_posts)
    matched, unmatched_habr, unmatched_telegram  = matcher.match_posts(habr_posts, telegram_posts)
    DataStorage.save_to_excel(matched, unmatched_habr, unmatched_telegram)

if __name__ == '__main__':
    start()
