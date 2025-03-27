import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Вывод в консоль
        logging.FileHandler('habr_parser.log')  # Запись в файл
    ]
)

logger = logging.getLogger("habr_logger")