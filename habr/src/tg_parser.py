import os
from datetime import datetime
from dotenv import load_dotenv

from telethon.sync import TelegramClient                     # Основной клиент для работы(синхронный)
from telethon.tl.functions.messages import GetHistoryRequest # Получение истории сообщений из чата
from telethon.tl.types import Channel                        # Тип, представляющий тг-канал

