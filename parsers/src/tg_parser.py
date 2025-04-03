import os
from datetime import datetime
from logging import Logger
from dotenv import load_dotenv
from loggers import setup_logger

logger: Logger = setup_logger('telegram_logger')

from telethon.sync import TelegramClient                     # Основной клиент для работы(синхронный)
from telethon.tl.functions.messages import GetHistoryRequest # Получение истории сообщений из чата
from telethon.tl.types import Channel                        # Тип, представляющий тг-канал

class TelegramChannelParser:
    def __init__(self, channel_name: str):
        load_dotenv()

        self.api_id = int(os.getenv("TELEGRAM_API_ID"))
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.channel_name = channel_name

        self.client = TelegramClient('session',
                                     api_id=self.api_id,
                                     api_hash=self.api_hash
                                     )
        self.channel = None    # Будет содержать объект канала после подключения
        self.posts = None      # Здесь будут храниться полученные посты

    async def connect_to_channel(self):
        self.channel = await self.client.get_entity(self.channel_name)

        if not isinstance(self.channel, Channel):
            raise TypeError('Channel must be Channel')
            logger.error('Нет канала с таким именем')


