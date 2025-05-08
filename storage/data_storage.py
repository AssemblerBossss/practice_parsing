import json
import openpyxl
import pandas as pd
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
from typing import Literal
from datetime import datetime
from loggers import setup_logger
from storage.storage_config import (DATA_DIR, ALLOWED_FILES)

logger = setup_logger("saving_logger", log_file="saving.log")


class DataStorage:
    @staticmethod
    def save_as_json(posts: list, filename: Literal['habr', 'pikabu', 'telegram'], channel_url: str = None) -> bool:
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
                "posts_count": len(posts),
                "channel_url": channel_url
            },
            'posts': posts
        }

        try:
            with open(file_path , 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=4)
            logger.info("Saved %d posts to %s", len(posts), filename)
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
            logger.info("Successfully read data from %s", file_name)
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

    @staticmethod
    def extract_channel_url(source: Literal['habr', 'pikabu', 'telegram']) -> str | None:
        """
        Извлекает значение 'channel_url' из блока 'metadata' JSON-файла.

        :param source: Путь к JSON-файлу
        :return: Ссылка на канал или None, если не найдена
        """

        if source not in ALLOWED_FILES:
            logger.error("Недопустимый источник: %s. Допустимые: %s",
                         source, list(ALLOWED_FILES.keys()))
            raise ValueError(f"Invalid source. Allowed: {list(ALLOWED_FILES.keys())}")

        file_name = ALLOWED_FILES[source]
        file_path = DATA_DIR / file_name

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                metadata = data.get("metadata", {})
                return metadata.get("channel_url")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Ошибка при чтении JSON: {e}")
            return None

    @staticmethod
    def auto_adjust_column_width(ws, df: pd.DataFrame) -> None:
        for i, column in enumerate(df.columns, 1):
            max_length = max([len(str(cell)) for cell in df[column].values] + [len(column)])
            ws.column_dimensions[get_column_letter(i)].width = min(max_length + 2, 100)

    @staticmethod
    def save_to_excel(matched: list[dict],
                      unmatched_habr: list[dict],
                      unmatched_telegram: list[dict],
                      matched_path: str = 'matched_posts.xlsx',
                      unmatched_habr_path: str = 'unmatched_habr.xlsx',
                      unmatched_telegram_path: str = 'unmatched_telegram.xlsx'

                      ) -> None:
        """
        Сохраняет результаты сопоставления постов в отдельные Excel-файлы.

        Создаёт три файла:
        - matched_path: файл с сопоставленными парами Habr и Telegram постов.
        - unmatched_habr_path: файл с Habr-постами, которым не нашлось пары.
        - unmatched_telegram_path: файл с Telegram-постами, которым не нашлось пары.

        Также очищает тексты от символа '#' и автоматически подбирает ширину колонок в таблицах.

        Args:
            matched (list[dict]): Список словарей с совпавшими постами.
            unmatched_habr (list[dict]): Список словарей с Habr-постами без пары.
            unmatched_telegram (list[dict]): Список словарей с Telegram-постами без пары.
            matched_path (str): Имя файла для сохранения совпавших пар.
            unmatched_habr_path (str): Имя файла для несопоставленных Habr-постов.
            unmatched_telegram_path (str): Имя файла для несопоставленных Telegram-постов.

        Returns:
            None
        """


        DATA_DIR.mkdir(exist_ok=True, parents=True)

        matched_path = DATA_DIR / matched_path
        unmatched_habr_path = DATA_DIR / unmatched_habr_path
        unmatched_telegram_path = DATA_DIR / unmatched_telegram_path

        matched_df = pd.DataFrame(matched)
        unmatched_df = pd.DataFrame(unmatched_habr)
        unmatched_telegram_df = pd.DataFrame(unmatched_telegram)

        matched_df['telegram_text'] = matched_df['telegram_text'].str.replace('#', '', regex=False)
        matched_df['habr_text'] = matched_df['habr_text'].str.replace('#', '', regex=False)
        unmatched_telegram_df['text'] = unmatched_telegram_df['text'].str.replace('#', '', regex=False)

        with pd.ExcelWriter(matched_path, engine='openpyxl') as writer:
            matched_df.to_excel(writer, index=False, sheet_name='Matched')
            DataStorage.auto_adjust_column_width(writer.sheets['Matched'], matched_df)

        with pd.ExcelWriter(unmatched_habr_path, engine='openpyxl') as writer:
            unmatched_df.to_excel(writer, index=False, sheet_name='Unmatched_habr')
            DataStorage.auto_adjust_column_width(writer.sheets['Unmatched_habr'], unmatched_df)

        with pd.ExcelWriter(unmatched_telegram_path, engine='openpyxl') as writer:
            unmatched_telegram_df.to_excel(writer, index=False, sheet_name='Unmatched_telegram')
            DataStorage.auto_adjust_column_width(writer.sheets['Unmatched_telegram'], unmatched_telegram_df)

        logger.info(f"✅ Сопоставленные пары записаны в {matched_path}")
        logger.info(f"📄 Несопоставленные habr-посты записаны в {unmatched_habr_path}")
        logger.info(f"📄 Несопоставленные telegram-посты записаны в {unmatched_telegram_path}")
