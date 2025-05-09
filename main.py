import asyncio
from parsers.src import *
from parsers.content_comporator_bert import start
from storage import DataStorage


async def main():
    # Значения по умолчанию
    DEFAULT_HABR_USER = "DevFM"
    DEFAULT_TG_CHANNEL = "DevFM"

    # Ввод данных с подсказками и значениями по умолчанию
    habr_name = input(f"Введите имя пользователя на Habr [{DEFAULT_HABR_USER}]: ") or DEFAULT_HABR_USER
    telegram_name = input(f"Введите название канала в Telegram [{DEFAULT_TG_CHANNEL}]: ") or DEFAULT_TG_CHANNEL

    # Запуск парсеров
    parser_habr = HabrParser(habr_name)
    await parser_habr.start()


    parser_tg = TelegramChannelParser(telegram_name)
    await parser_tg.run()

    # Запуск сравнения
    start(parser_tg.get_posts(), parser_habr.get_posts())

    DataStorage.save_as_json(parser_tg.get_posts(), filename='telegram', channel_url=parser_tg.channel_url)
    DataStorage.save_as_json(parser_habr.get_posts(), filename='habr', channel_url=parser_habr.url)


if __name__ == "__main__":
    asyncio.run(main())
