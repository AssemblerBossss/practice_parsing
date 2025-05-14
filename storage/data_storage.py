import dataclasses
import json
import openpyxl
import pandas as pd
from openpyxl.utils import get_column_letter
from typing import Literal
from datetime import datetime

from loggers import setup_logger
from models import HabrPostModel, TelegramPostModel, PikabuPostModel
from storage.storage_config import (DATA_DIR, ALLOWED_FILES)

logger = setup_logger("saving_logger", log_file="saving.log")


class DataStorage:
    @staticmethod
    def check_is_dataclass(post) -> bool:
        return dataclasses.is_dataclass(post)

    @staticmethod
    def convert_to_dict(posts: list) -> list[dict]:
        return [dataclasses.asdict(post) if dataclasses.is_dataclass(post) else post for post in posts]

    @staticmethod
    def save_as_json(posts: list, filename: Literal['habr', 'pikabu', 'telegram'], channel_url: str = None) -> bool:
        """
        Сохраняет посты в JSON файл.

        :param posts: Данные для сохранения
        :param filename: Имя файла (без расширения)
        :param channel_url: Ссылка на канал(пользователя)

        :return True при успешном сохранении постов
                False иначе
        """
        DATA_DIR.mkdir(exist_ok=True, parents=True)

        if posts is None:
            logger.error("Передан пустой список постов для сохранения в json")
            return False

        if filename not in ALLOWED_FILES:
            logger.error("Указано неверное имя для сохранения в json: %s. Допустимые: %s",
                         filename, list(ALLOWED_FILES.keys()))
            return False

        filename = filename + '.json'
        file_path = DATA_DIR / filename

        if DataStorage.check_is_dataclass(posts[0]):
            posts = DataStorage.convert_to_dict(posts)

        output_data = {
            'metadata': {
                'generated_at' : datetime.now().strftime('%Y-%m-%d %H:%M'),
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
    def auto_adjust_column_width(ws, df: pd.DataFrame) -> None:
        for i, column in enumerate(df.columns, 1):
            max_length = max([len(str(cell)) for cell in df[column].values] + [len(column)])
            ws.column_dimensions[get_column_letter(i)].width = min(max_length + 2, 100)

    @staticmethod
    def save_to_excel(matched: list[dict],
                      unmatched_habr: list[HabrPostModel],
                      unmatched_telegram: list[TelegramPostModel],
                      unmatched_pikabu: list[PikabuPostModel],
                      matched_path: str = 'matched_posts.xlsx',
                      unmatched_habr_path: str = 'unmatched_habr.xlsx',
                      unmatched_telegram_path: str = 'unmatched_telegram.xlsx',
                      unmatched_pikabu_path: str = "unmatched_pikabu.xlsx"
                      ) -> None:
        """
        Сохраняет результаты сопоставления постов в отдельные Excel-файлы.

        Создаёт три файла:
        - matched_path: файл с сопоставленными парами Habr и Telegram постов.
        - unmatched_habr_path: файл с Habr-постами, которым не нашлось пары.
        - unmatched_telegram_path: файл с Telegram-постами, которым не нашлось пары.

        Также очищает тексты от символа '#' и автоматически подбирает ширину колонок в таблицах.

        :param matched: Список словарей с совпавшими постами.
        :param unmatched_habr: Список Habr-постов без пары.
        :param unmatched_telegram: Список Telegram-постов без пары.
        :param unmatched_pikabu: Список Pikabu-постов без пары.
        :param matched_path: Имя файла для сохранения совпавших пар.
        :param unmatched_habr_path: Имя файла для несопоставленных Habr-постов.
        :param unmatched_telegram_path: Имя файла для несопоставленных Telegram-постов.
        :param unmatched_pikabu_path: Имя файла для несопоставленных Pikabu-постов.

        :return None
        """


        DATA_DIR.mkdir(exist_ok=True, parents=True)

        matched_path            = DATA_DIR / matched_path
        unmatched_habr_path     = DATA_DIR / unmatched_habr_path
        unmatched_telegram_path = DATA_DIR / unmatched_telegram_path
        unmatched_pikabu_path   = DATA_DIR / unmatched_pikabu_path

        matched_df = pd.DataFrame(matched)
        unmatched_df = pd.DataFrame(unmatched_habr)
        unmatched_telegram_df = pd.DataFrame(unmatched_telegram)
        unmatched_pikabu_df = pd.DataFrame(unmatched_pikabu)

        # matched_df['telegram_content'] = matched_df['telegram_content'].str.replace('#', '', regex=False)
        # matched_df['habr_content'] = matched_df['habr_content'].str.replace('#', '', regex=False)
        # unmatched_telegram_df['content'] = unmatched_telegram_df['content'].str.replace('#', '', regex=False)

        with pd.ExcelWriter(matched_path, engine='openpyxl') as writer:
            matched_df.to_excel(writer, index=False, sheet_name='Matched')
            DataStorage.auto_adjust_column_width(writer.sheets['Matched'], matched_df)

        with pd.ExcelWriter(unmatched_habr_path, engine='openpyxl') as writer:
            unmatched_df.to_excel(writer, index=False, sheet_name='Unmatched_habr')
            DataStorage.auto_adjust_column_width(writer.sheets['Unmatched_habr'], unmatched_df)

        with pd.ExcelWriter(unmatched_telegram_path, engine='openpyxl') as writer:
            unmatched_telegram_df.to_excel(writer, index=False, sheet_name='Unmatched_telegram')
            DataStorage.auto_adjust_column_width(writer.sheets['Unmatched_telegram'], unmatched_telegram_df)

        with pd.ExcelWriter(unmatched_pikabu_path, engine='openpyxl') as writer:
            unmatched_pikabu_df.to_excel(writer, index=False, sheet_name='Unmatched_pikabu')
            DataStorage.auto_adjust_column_width(writer.sheets['Unmatched_pikabu'], unmatched_pikabu_df)

        logger.info(f"✅ Сопоставленные пары записаны в {matched_path}")
        logger.info(f"📄 Несопоставленные habr-посты записаны в {unmatched_habr_path}")
        logger.info(f"📄 Несопоставленные telegram-посты записаны в {unmatched_telegram_path}")
        logger.info(f"📄 Несопоставленные pikabu-посты записаны в {unmatched_pikabu_path}")
