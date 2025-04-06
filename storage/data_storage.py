import json
from typing import Literal
from datetime import datetime
from loggers import setup_logger
from storage.storage_config import (DATA_DIR, ALLOWED_FILES)

logger = setup_logger("saving_logger")


class DataStorage:
    @staticmethod
    def save_as_json(posts, filename: Literal['habr', 'pikabu', 'telegram']) -> bool:
        """
        Сохраняет посты в JSON файл.
        Args:
            posts: Данные для сохранения
            filename: Имя файла (без расширения)
        Raises:
            ValueError: При недопустимом имени файла
        """
        DATA_DIR.mkdir(exist_ok=True, parents=True)

        if filename not in ALLOWED_FILES:
            logger.error("Указано неверное имя для сохранения в json: %s. Допустимые: %s",
                         filename, list(ALLOWED_FILES.keys()))
            raise ValueError(f"Invalid filename. Allowed names: {list(ALLOWED_FILES.keys())}")
        filename = filename + '.json'
        file_path = DATA_DIR / filename

        output_data = {
            'metadata': {
                'generated_at' : datetime.now().isoformat(),
                "posts_count": len(posts)
            },
            'posts': posts
        }

        try:
            with open(file_path , 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=4)
            logger.info("Saved %d posts to %s", len(posts), file_path)
            return True
        except Exception as e:
            logger.error("Failed to save posts: %s", str(e))
            return False

    @staticmethod
    def read_json(source: Literal['habr', 'pikabu', 'telegram']) -> list[dict[str, str]]:
        """
        Чтение посты из JSON файла.
        Args:
            source: Источник данных для чтения
        Returns:
            list[dict]: Список постов или None при ошибке
        Raises:
            ValueError: При недопустимом имени источника
        """

        if source not in ALLOWED_FILES:
            logger.error("Недопустимый источник: %s. Допустимые: %s",
                         source, list(ALLOWED_FILES.keys()))
            raise ValueError(f"Invalid source. Allowed: {list(ALLOWED_FILES.keys())}")

        file_name = ALLOWED_FILES[source]
        file_path = DATA_DIR / file_name

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f).get('posts', [])
            logger.info("Successfully read data from %s", file_path)
            return data
        except FileNotFoundError:
            logger.error("File not found: %s", file_path)
            return None
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format in %s: %s", file_path, str(e))
            return None
        except Exception as e:
            logger.error("Error reading file %s: %s", file_path, str(e))
            return None
