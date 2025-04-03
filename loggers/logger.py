import logging
from .logging_config import DEFAULT_HABR_LOG_FILE, LOG_FORMAT, LOG_LEVEL_INFO, LOGS_DIR, LOG_DATE_FORMAT


def setup_logger(logger_name: str,
                 log_level: str = LOG_LEVEL_INFO,
                 log_file: str = DEFAULT_HABR_LOG_FILE,
                 console_output: bool = True
                 ) -> logging.Logger:
    """
    Настраивает и возвращает логгер с файловым и консольным выводом.

    Args:
        logger_name: Имя логгера (обычно __name__)
        log_file: Имя файла лога (если None, используется DEFAULT_LOG_FILE)
        log_level: Уровень логирования (если None, используется LOG_LEVEL из конфига)
        console_output: Включить вывод в консоль

    Returns:
        Настроенный логгер
    """

    # Создаем директорию для логов, если не существует
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Определяем конечный путь к файлу лога
    log_path = LOGS_DIR / log_file

    # Настройка formatter
    formatter = logging.Formatter(
        fmt=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT
    )

    # Создаем и настраиваем логгер
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Настройка обработчика для файла
    file_handler = logging.FileHandler(
        filename=log_path,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Настройка обработчика для файла
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
