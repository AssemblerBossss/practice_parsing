import json
from typing import Literal
from loggers import setup_logger
from datetime import datetime
from storage.storage_config import (DATA_DIR, ALLOWED_FILES)

logger = setup_logger("saving_logger")


class DataStorage:
    @staticmethod
    def save_as_json(posts, filename: Literal['habr', 'pikabu', 'telegram']) -> bool:
        DATA_DIR.mkdir(exist_ok=True, parents=True)

        if filename not in ALLOWED_FILES:
            logger.error("Указано неверное имя для сохранения в json")
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
            logger.info(f"Saved {len(output_data)} posts to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save posts: {str(e)}")
            return False

    @staticmethod
    def read_json(source: Literal['habr', 'pikabu', 'telegram']) -> dict[dict, list[dict[str, str]]]:
        """Чтение данных из JSON файла"""

        if source not in ALLOWED_FILES:
            raise ValueError(f"Invalid source. Allowed: {list(ALLOWED_FILES.keys())}")

        file_name = ALLOWED_FILES[source]
        file_path = DATA_DIR / file_name

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Successfully read data from {file_path}")
            return data
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {file_path}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            return None
