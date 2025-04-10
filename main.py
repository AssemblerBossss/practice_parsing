import asyncio
from parsers.src import *
from parsers.content_comparator import comporator_start

async def main():
    habr_name: str = input("Введите имя пользователя на Habr: ")
    telegram_name: str = input("Введите название канала в Telegram: ") or 'DevFM'
    telegram_name: str = 'DevFM'

    # Запуск парсера с ограничением в 100 постов

    await start_habr(habr_name)
    parser = TelegramChannelParser(telegram_name)
    await parser.run(post_limit=600)
    comporator_start()

if __name__ == "__main__":
    asyncio.run(main())