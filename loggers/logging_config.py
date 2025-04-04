from pathlib import Path

# Базовые пути
BASE_DIR = Path(__file__).parent.parent
LOGS_DIR = BASE_DIR / "logs"

# Имена файлов логов
DEFAULT_HABR_LOG_FILE = "parsers.log"
DEFAULT_TELEGRAM_LOG_FILE = "telegram.log"
DEFAULT_PIKABU_LOG_FILE = "pikabu.log"

# Форматы
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Уровни логирования
LOG_LEVEL_INFO = "INFO"
