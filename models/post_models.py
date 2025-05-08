from dataclasses import dataclass
from typing import Optional


@dataclass
class TelegramPost:
    """
    Класс, представляющий пост из Telegram-канала.
    """
    id: int                 # Уникальный идентификатор поста.
    date: str               # Дата публикации
    text: str               # Текст поста
    views: Optional[int]    # Количество просмотров сообщения
    media: bool             # Признак наличия вложенных медиафайлов
    is_forward: bool        # Признак, является ли сообщение пересланным
    post_url: str           # Прямая ссылка на пост в Telegram


@dataclass
class HabrPost:
    """
    Класс, представляющий статью на платформе Habr.
    """
    title: str              # Заголовок статьи
    date: str               # Дата публикации статьи
    content: str            # Основной текст статьи
