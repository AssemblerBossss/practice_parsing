import re
import torch
from logging import DEBUG
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

from storage import DataStorage
from loggers import setup_logger
from models import HabrPostModel, TelegramPostModel

logger = setup_logger(__name__, log_file="content_comporator_bert.log", log_level=DEBUG)

EMBEDDINGS_CACHE:    dict[str, torch.tensor] = {}
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

        Args:
            threshold_duplicate_: Порог схожести для дубликатов (по умолчанию 0.9)
            threshold_match_: Порог схожести для сопоставления (по умолчанию 0.65)
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

    def get_embeddings_for_posts(self, posts: list, key: str = 'text') -> list[torch.Tensor]:
        """
        Генерирует векторные представления для списка постов.

        :param posts: Список объектов HabrPostModel или TelegramPostModel
        :param key: Ключ, указывающий на поле с текстом (например, 'content' для Habr и 'text' для Telegram)
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

    def remove_telegram_duplicates(self, telegram_posts: list[TelegramPostModel], threshold=0.90) -> list[TelegramPostModel]:
        """
        Удаляет дубликаты постов Telegram на основе семантической схожести.

        :param telegram_posts: Список постов для фильтрации
        :param threshold: Порог схожести для определения дубликатов
        :return: Отфильтрованный список уникальных постов
        """
        logger.info("🧹 Удаление дубликатов из Telegram-постов...")
        filtered_posts = []
        seen = set()

        embeddings = self.get_embeddings_for_posts(telegram_posts, key='text')

        for i, _ in enumerate(tqdm(telegram_posts)):
            if i in seen:
                continue
            best_j = i
            for j in range(i + 1, len(telegram_posts)):
                if j in seen:
                    continue
                sim = cosine_similarity([embeddings[i]], [embeddings[j]])[0][0]
                if sim > threshold:
                    views_j = telegram_posts[j].views or 0
                    views_best = telegram_posts[best_j].views or 0
                    best_j = j if views_j > views_best else best_j
                    seen.add(j)

            filtered_posts.append(telegram_posts[best_j])
            seen.add(best_j)

        return filtered_posts

    def match_posts(self, habr_posts: list[HabrPostModel], telegram_posts: list[TelegramPostModel]):
        """
        Сопоставляет посты между платформами Habr и Telegram на основе их семантической схожести.

        Использует векторные представления контента постов и рассчитывает косинусное сходство для нахождения пар постов.

        :param habr_posts: Список объектов HabrPostModel
        :param telegram_posts: Список объектов TelegramPostModel
        :return: Кортеж из:
            - Списка найденных пар (со всеми данными о постах)
            - Списка несопоставленных постов с платформы Habr
            - Списка несопоставленных постов с платформы Telegram
        """
        logger.info("📥 Получено %s постов из Habr и %s из Telegram.",
                    len(habr_posts), len(telegram_posts)
                    )
        logger.info("🔍 Сопоставление постов Habr и Telegram...")

        matches = []  # Список для хранения найденных пар
        unmatched_habr = []  # Список для несопоставленных постов Habr
        unmatched_telegram = []  # Список для несопоставленных постов Telegram
        used_telegram_ids = set()  # Множество использованных идентификаторов постов Telegram

        habr_embeddings = self.get_embeddings_for_posts(habr_posts, key='content')
        telegram_embeddings = self.get_embeddings_for_posts(telegram_posts, key='text')

        for i, habr in enumerate(tqdm(habr_posts)):
            best_match_idx = None
            best_score = 0

            for j, tele in enumerate(telegram_posts):
                if tele.id in used_telegram_ids:
                    continue

                score = cosine_similarity([habr_embeddings[i]], [telegram_embeddings[j]])[0][0]
                if score > best_score:
                    best_score = score
                    best_match_idx = j

            if best_score >= self.threshold_match:
                best_match = telegram_posts[best_match_idx]
                matches.append({
                    "habr_title": habr.title,
                    "habr_date": habr.date,
                    "telegram_id": best_match.post_url,
                    "telegram_date": best_match.date,
                    "similarity": best_score,
                    "habr_content": habr.content,
                    "telegram_content": best_match.content
                })
                used_telegram_ids.add(best_match.id)

                logger.debug("# Найдена пара #:")
                logger.debug("Habr:  %s:  %s ", habr.title, habr.date)
                logger.debug("Telegram (ID: %s),: %s", best_match.id, best_match.date)
                logger.debug("Оценка схожести: %s", {best_score: .2})
                logger.debug("-" * 100)
            else:
                unmatched_habr.append(habr)
        logger.info("✅ Сопоставлено %s пар.", len(matches))
        logger.info("❌ Не найдено пары для %s habr-постов.", len(unmatched_habr))

        logger.info("🔍 Поиск постов Telegram, которым не нашлось пары...")
        for post in tqdm(telegram_posts):
            if post.id not in used_telegram_ids:
                unmatched_telegram.append(post)
        logger.info("❌ Не найдено пары для %s telegram-постов.", len(unmatched_telegram))

        return matches, unmatched_habr, unmatched_telegram


def start(telegram_posts: list[TelegramPostModel], habr_posts: list[HabrPostModel]):
    """
    Основная функция для обработки и сопоставления постов Habr и Telegram.

    Читает посты из JSON-файлов, удаляет дубликаты в Telegram-постах,
    сопоставляет посты между платформами и сохраняет результаты в Excel.
    """

    matcher = PostMatcher()
    telegram_posts = matcher.remove_telegram_duplicates(telegram_posts)
    matched, unmatched_habr, unmatched_telegram  = matcher.match_posts(habr_posts, telegram_posts)
    #DataStorage.save_to_excel(matched, unmatched_habr, unmatched_telegram)
