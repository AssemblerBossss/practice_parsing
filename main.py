import asyncio
from parsers.src import *
from parsers.content_comporator_bert import start


async def main():
    # Значения по умолчанию
    DEFAULT_HABR_USER = "DevFM"
    DEFAULT_TG_CHANNEL = "DevFM"

    # Ввод данных с подсказками и значениями по умолчанию
    habr_name = input(f"Введите имя пользователя на Habr [{DEFAULT_HABR_USER}]: ") or DEFAULT_HABR_USER
    telegram_name = input(f"Введите название канала в Telegram [{DEFAULT_TG_CHANNEL}]: ") or DEFAULT_TG_CHANNEL

    # Запуск парсеров

    parser_habr = HabrParser(habr_name)
    habr_posts = await parser_habr.start()


    parser_tg = TelegramChannelParser(telegram_name)
    await parser_tg.run(post_limit=100)
    telegram_posts = parser_tg.get_posts()


    # Запуск сравнения
    start(telegram_posts, habr_posts)


if __name__ == "__main__":
    asyncio.run(main())
