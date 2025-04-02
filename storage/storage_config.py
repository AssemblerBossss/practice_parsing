from pathlib import Path

# Разрешенные файлы для чтения
ALLOWED_FILES = {
    'habr': 'habr.json',
    'pikabu': 'pikabu.json',
    'telegram': 'telegram.json'
}

# Путь до директории с файлами json
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"