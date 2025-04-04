import os
from logging import Logger
from dotenv import load_dotenv
from loggers import setup_logger, DEFAULT_TELEGRAM_LOG_FILE
from storage import DataStorage

from telethon.sync import TelegramClient                      # Основной клиент для работы(синхронный)
from telethon.tl.functions.messages import GetHistoryRequest  # Получение истории сообщений из чата
from telethon.tl.types import Channel                         # Тип, представляющий тг-канал

logger: Logger = setup_logger('telegram_logger', log_file=DEFAULT_TELEGRAM_LOG_FILE)


class TelegramChannelParser:
    def __init__(self, channel_name: str):
        """
        Инициализирует парсер канала Telegram.
        Args: channel_username (str): Название канала
        """

        self._load_env_vars()         # Загрузка переменных окружения
        self._validate_credentials()  # Проверка обязательных переменных

        self.channel_name = channel_name

        self.client = TelegramClient('session',
                                     api_id=int(self.api_id),
                                     api_hash=self.api_hash
                                     )
        self.channel = None  # Будет содержать объект канала после подключения
        self.posts = []      # Здесь будут храниться полученные посты

    def _load_env_vars(self):
        """Загрузка переменных окружения из .env файла"""
        if not load_dotenv():
            logger.error("Файл .env не найден, пытаюсь использовать системные переменные окружения")

        self.api_id = os.getenv('TELEGRAM_API_ID')
        self.api_hash = os.getenv('TELEGRAM_API_HASH')

    def _validate_credentials(self):
        """Проверка наличия обязательных переменных"""
        required_vars = {
            'TELEGRAM_API_ID': self.api_id,
            'TELEGRAM_API_HASH': self.api_hash,
        }

        missing_vars = [name for name, value in required_vars.items() if not value]

        if missing_vars:
            raise ValueError(
                f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}\n"
                "Пожалуйста, создайте .env файл со следующими переменными:\n"
                "TELEGRAM_API_ID=ваш_api_id\n"
                "TELEGRAM_API_HASH=ваш_api_hash\n"
            )

    async def connect_to_channel(self):
        """Подключение к каналу и получение информации о нем"""
        self.channel = await self.client.get_entity(self.channel_name)

        if not isinstance(self.channel, Channel):
            logger.error('Нет канала с таким именем')
            raise TypeError('Channel must be Channel')

    async def get_posts(self, limit: int = 100, total_limit: int = 500):
        """
        Получение постов из канала
        :param limit: Количество постов за один запрос (max 100)
        :param total_limit: Общее ограничение количества постов (0 - без ограничений)
        """

        if not self.channel:
            await self.connect_to_channel()

        offset_id: int = 0
        total_count_of_messages: int = 0

        while True:
            history = await self.client(GetHistoryRequest(
                peer=self.channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))

            if not history.messages:
                logger.warning("Список сообщений пуст")
                break

            self._process_messages(history.messages)

            total_count_of_messages += len(history.messages)
            if total_limit != 0 and total_count_of_messages >= total_limit:  # достигли/превысили лимит
                break

            offset_id += history.history[-1].id  # ID для следующего запроса

    def _process_messages(self, messages):
        """Обработка и сохранение сообщений"""
        for message in messages:
            post_data = {
                'id': message.id,
                'date': message.date.isoformat(),
                'text': message.message,
                'views': getattr(message, 'views', None),
                'media': bool(message.media),
                'is_forward': bool(message.fwd_from)
            }
            self.posts.append(post_data)

    def save_to_json(self):
        """Сохранение в json"""
        DataStorage.save_as_json(self.posts, 'telegram')

    async def run(self, post_limit: int = 0):
        """Основной метод для запуска парсера"""
        async with self.client:
            await self.connect_to_channel()
            await self.get_posts(total_limit=post_limit)
            self.save_to_json()


# Пример использования
if __name__ == "__main__":
    parser = TelegramChannelParser('DevFM')

    # Запуск парсера с ограничением в 100 постов
    parser.client.loop.run_until_complete(parser.run(post_limit=100))
