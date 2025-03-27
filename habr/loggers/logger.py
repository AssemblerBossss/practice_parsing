import logging
import os


def setup_logger(logger_name: str, log_file: str = 'habr_parser.log') -> logging.Logger:
    """
    Настройка логгера с записью в файл

    :param logger_name: Имя логгера
    :param log_file: Имя файла для логов
    :return: Настроенный логгер
    """

    # Создаем папку для логов, если ее нет
    os.makedirs('logs', exist_ok=True)
    log_path = os.path.join('logs', log_file)

    # Настройка формата
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Настройка обработчика для файла
    file_handler = logging.FileHandler(
        log_path,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    # Настройка обработчика для файла
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Создаем и настраиваем логгер
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger




