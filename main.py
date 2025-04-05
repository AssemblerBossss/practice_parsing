import asyncio
from parsers.src import *

async def main():
    habr_name: str = input("Введите имя пользователя на Habr: ")
    telegram_name: str = input("Введите название канала в Telegram: ")

    parser = TelegramChannelParser(telegram_name)

    # Запуск парсера с ограничением в 100 постов
    await parser.run(post_limit=100)
    await start_habr(habr_name)


if __name__ == "__main__":
    asyncio.run(main())