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
    await start_habr(habr_name)
    parser = TelegramChannelParser(telegram_name)
    await parser.run(post_limit=600)

    # Запуск сравнения
    start()


if __name__ == "__main__":
    asyncio.run(main())
