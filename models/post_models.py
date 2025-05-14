from dataclasses import dataclass
from typing import Optional


@dataclass
class TelegramPostModel:
    """
    Класс, представляющий пост из Telegram-канала.
    """
    id: int                 # Уникальный идентификатор поста.
    date: str               # Дата публикации
    content: str            # Текст поста
    views: Optional[int]    # Количество просмотров сообщения
    media: bool             # Признак наличия вложенных медиафайлов
    is_forward: bool        # Признак, является ли сообщение пересланным
    post_url: str           # Прямая ссылка на пост в Telegram


@dataclass
class HabrPostModel:
    """
    Класс, представляющий статью на платформе Habr.
    """
    title: str              # Заголовок статьи
    date: str               # Дата публикации статьи
    content: str            # Основной текст статьи
    post_url: str           # Прямая ссылка на пост в Habr


@dataclass
class PikabuPostModel:
    """
    Класс, представляющий статью на платформе Pikabu.
    """
    id: int                 # Уникальный идентификатор поста.
    title: str              # Заголовок статьи
    date: str               # Дата публикации
    content: str            # Текст поста
    rating: str             # Рейтинг статьи
    post_url: str           # Прямая ссылка на пост в Pikabu
    url_profile: str        # Ссылка на автора статьи
