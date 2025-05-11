import re
import torch
from logging import DEBUG
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

from storage import DataStorage
from loggers import setup_logger
from models import HabrPostModel, TelegramPostModel, PikabuPostModel

logger = setup_logger(__name__, log_file="content_comporator_bert.log", log_level=DEBUG)

EMBEDDINGS_CACHE: dict[str, torch.tensor] = {}
VISITED_POSTS_CACHE: dict[str, bool] = {}


class PostMatcher:
    """
    Класс для сопоставления постов между Habr, Telegram и Pikabu на основе семантического анализа.

    Использует модель Sentence Transformers для получения векторных представлений текстов
    и вычисляет их косинусную схожесть для нахождения дубликатов и парных постов.

    Attributes:
        threshold_duplicate (float): Порог схожести для определения дубликатов
        threshold_match (float): Порог схожести для сопоставления постов
        device (torch.device): Устройство для вычислений (CPU/GPU)
        model (SentenceTransformer): Модель для получения эмбеддингов текста
    """

    def __init__(self, threshold_duplicate_: float = 0.9, threshold_match_: float = 0.65):
        """
        Инициализирует PostMatcher с заданными параметрами.

        :param threshold_duplicate_: Порог схожести для дубликатов (по умолчанию 0.9)
        :param threshold_match_: Порог схожести для сопоставления (по умолчанию 0.65)
        """
        self.threshold_duplicate = threshold_duplicate_
        self.threshold_match = threshold_match_

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        logger.info("🔄 Загрузка модели SentenceTransformers...")
        self.model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
        self.model = self.model.to(self.device)
        logger.info("✅ Модель загружена.")

    @staticmethod
    def normalize_text(text: str) -> str:
        """Нормализует текст: заменяет множественные пробелы на один, обрезает
         и приводит к нижнему регистру."""
        text = re.sub(r'\s+', ' ', text)
        return text.strip().lower()

    def get_embeddings_for_posts(self, posts: list) -> list[torch.Tensor]:
        """
        Генерирует векторные представления для списка постов.

        :param posts: Список объектов HabrPostModel или TelegramPostModel
        :return: Список эмбеддингов
        """

        texts = [self.normalize_text(post.content) for post in posts]
        with torch.no_grad():
            embeddings = self.model.encode(
                texts,
                batch_size=16,
                show_progress_bar=True,
                device=str(self.device)
            )
            return [torch.from_numpy(embedding) for embedding in embeddings]

    def remove_duplicates(self, posts: list) -> list:
        """
        Удаляет дубликаты постов Telegram на основе семантической схожести.

        :param posts: Список постов для фильтрации
        :return: Отфильтрованный список уникальных постов
        """
        logger.info("🧹 Удаление дубликатов из %s постов...", len(posts))
        filtered_posts = []
        seen = set()

        embeddings = self.get_embeddings_for_posts(posts)

        seen = set()

        for i, post in enumerate(tqdm(posts)):
            if i in seen:
                continue

            best_idx = i  # Initialize with current index
            for j in range(i + 1, len(posts)):
                if j in seen:
                    continue

                sim = cosine_similarity([embeddings[i]], [embeddings[j]])[0][0]
                if sim > self.threshold_duplicate:
                    # Для Telegram выбираем пост с большим количеством просмотров
                    if hasattr(posts[j], 'views') and hasattr(posts[best_idx], 'views'):
                        if (posts[j].views or 0) > (posts[best_idx].views or 0):
                            best_idx = j
                    seen.add(j)

            # Ensure we only add valid posts
            if best_idx < len(posts):  # Additional safety check
                filtered_posts.append(posts[best_idx])
                seen.add(best_idx)

        logger.info(f"✅ Оставлено {len(filtered_posts)} уникальных постов.")
        return filtered_posts

    def match_all_posts(self,
                        habr_posts: list[HabrPostModel],
                        telegram_posts: list[TelegramPostModel],
                        pikabu_posts: list[PikabuPostModel]) \
            -> tuple:
        """
        Сопоставляет посты между платформами Habr и Telegram на основе их семантической схожести.

        Использует векторные представления контента постов и рассчитывает косинусное сходство для нахождения пар постов.

        :param habr_posts:     Список объектов HabrPostModel
        :param telegram_posts: Список объектов TelegramPostModel
        :param pikabu_posts:   Список объектов PikabuPostModel

        :return:
            Кортеж из:
            - Список сопоставленных постов Habr с найденными соответствиями
            - Список несопоставленных постов Habr
            - Список несопоставленных постов Telegram
            - Список несопоставленных постов Pikabu
        """

        logger.info("📥 Получено %s постов Habr, %s Telegram, %s Pikabu",
                    len(habr_posts), len(telegram_posts), len(pikabu_posts))

        telegram_posts = self.remove_duplicates(telegram_posts)
        pikabu_posts = self.remove_duplicates(pikabu_posts)

        habr_embeddings = self.get_embeddings_for_posts(habr_posts)
        telegram_embeddings = self.get_embeddings_for_posts(telegram_posts)
        pikabu_embeddings = self.get_embeddings_for_posts(pikabu_posts)

        matched_habr = []
        unmatched_habr = []
        used_telegram = set()
        used_pikabu = set()

        for i, habr_post in enumerate(tqdm(habr_posts, desc="🔍 Сопоставление постов Habr, Telegram и Pikabu...")):
            habr_emb = habr_embeddings[i]

            # Поиск лучшего Telegram поста
            best_telegram = None
            best_telegram_score = 0
            for j, tele_post in enumerate(telegram_posts):
                if j in used_telegram:
                    continue
                score = cosine_similarity([habr_emb], [telegram_embeddings[j]])[0][0]
                if score > best_telegram_score and score >= self.threshold_match:
                    best_telegram_score = score
                    best_telegram = tele_post
                    best_telegram_index = j

            # Поиск лучшего Pikabu поста
            best_pikabu = None
            best_pikabu_score = 0
            for k, pika_post in enumerate(pikabu_posts):
                if k in used_pikabu:
                    continue
                score = cosine_similarity([habr_emb], [pikabu_embeddings[k]])[0][0]
                if score > best_pikabu_score and score >= self.threshold_match:
                    best_pikabu_score = score
                    best_pikabu = pika_post
                    best_pikabu_index = k

            if best_telegram or best_pikabu:
                matched_habr.append({
                    "habr_title": habr_post.title,
                    "habr_date": habr_post.date,
                    "habr_content": habr_post.content,
                    "telegram_url": best_telegram.post_url if best_telegram else None,
                    "telegram_date": best_telegram.date if best_telegram else None,
                    "telegram_content": best_telegram.content if best_telegram else None,
                    "telegram_similarity": best_telegram_score if best_telegram else 0,
                    "pikabu_title": best_pikabu.title if best_pikabu else None,
                    "pikabu_date": best_pikabu.date if best_pikabu else None,
                    "pikabu_url": best_pikabu.post_url if best_pikabu else None,
                    "pikabu_content": best_pikabu.content if best_pikabu else None,
                    "pikabu_similarity": best_pikabu_score if best_pikabu else 0
                })
                if best_telegram:
                    used_telegram.add(best_telegram_index)
                if best_pikabu:
                    used_pikabu.add(best_pikabu_index)
            else:
                unmatched_habr.append(habr_post)

        unmatched_telegram = [post for i, post in enumerate(telegram_posts) if i not in used_telegram]
        unmatched_pikabu = [post for i, post in enumerate(pikabu_posts) if i not in used_pikabu]

        logger.info("📊 Результаты сопоставления:")
        logger.info(f"✅ Сопоставлено постов Habr: {len(matched_habr)}")
        logger.info(f"❌ Несопоставлено постов Habr: {len(unmatched_habr)}")
        logger.info(f"❌ Несопоставлено постов Telegram: {len(unmatched_telegram)}")
        logger.info(f"❌ Несопоставлено постов Pikabu: {len(unmatched_pikabu)}")

        return matched_habr, unmatched_habr, unmatched_telegram, unmatched_pikabu

def start(habr_posts: list[HabrPostModel],
          telegram_posts: list[TelegramPostModel],
          pikabu_posts: list[PikabuPostModel]):
    """
    Основная функция для обработки и сопоставления постов.

    :param habr_posts:     Список постов с Habr
    :param telegram_posts: Список постов с Telegram
    :param pikabu_posts:   Список постов с Pikabu
    """
    matcher = PostMatcher()

    # Сопоставляем посты
    matched, unmatched_habr, unmatched_telegram, unmatched_pikabu = matcher.match_all_posts(
        habr_posts, telegram_posts, pikabu_posts)

    # Сохраняем результаты
    DataStorage.save_to_excel(matched, unmatched_habr, unmatched_telegram, unmatched_pikabu)
