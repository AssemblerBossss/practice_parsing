import asyncio

from models import PikabuPostModel
from parsers.src import *
from parsers.content_comporator_bert import start
from parsers.src import parse_user_profile


async def main():
    # Значения по умолчанию
    DEFAULT_HABR_USER = "DevFM"
    DEFAULT_TG_CHANNEL = "DevFM"

    # Ввод данных с подсказками и значениями по умолчанию
    habr_name = input(f"Введите имя пользователя на Habr [{DEFAULT_HABR_USER}]: ") or DEFAULT_HABR_USER
    telegram_name = input(f"Введите название канала в Telegram [{DEFAULT_TG_CHANNEL}]: ") or DEFAULT_TG_CHANNEL

    # Запуск парсеров
    parser_tg = TelegramChannelParser(telegram_name)
    tg_task = asyncio.create_task(parser_tg.run())

    parser_habr = HabrParser(habr_name)
    habr_task = asyncio.create_task(parser_habr.start())

    await asyncio.gather(tg_task, habr_task)

    pikabu_posts: list[PikabuPostModel] = parse_user_profile()
    # Запуск сравнения
    start(parser_habr.get_posts(), parser_tg.get_posts(), pikabu_posts)


if __name__ == "__main__":
    asyncio.run(main())
