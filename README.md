# 📰 Content Aggregator & Comparator


### ! Необходимо добавить в корень проекта .env файл, в котором указать ваш API_ID и API_HASH
Пример .env файла

```dotenv
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=1a1b1c1d2a2b2c2d3a3b3c3d4a4b4c4d
```


## Проект предназначен для **парсинга контента** из различных источников — **Pikabu**, **Habr** и **Telegram**, — и **сравнения статей** между собой с использованием модели BERT. Результаты сохраняются в виде Excel-файлов с совпавшими и несовпавшими постами.

- 🧩 Парсеры для разных источников
- 🤖 Сравнение контента с использованием BERT
- 📦 Хранение данных в JSON/Excel
- 📜 Логирование работы
- 🐳 Возможность запуска через Docker

---

## 📂 Структура проекта

```text
├── main.py # Точка входа
├── parsers/ # Логика парсинга и сравнения
│ ├── content_comporator_bert.py
│ └── src/
│   ├── pikabu_parser.py
│   ├── habr_parser.py
│   └── tg_parser.py
├── models/ # Модели данных
├── storage/ # Хранение и сериализация данных
├── loggers/ # Конфигурация логов
├── logs/ # Файлы логов
├── pyproject.toml      # Зависимости
├── poetry.lock
├── requirements.txt    # Зависимости
├── Dockerfile
├── Makefile # Команды сборки и запуска
└── README.md
```

---

## 🚀 Быстрый старт

### 1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/yourusername/content-comparator.git
   ```
   
### 2. 🐳 Запуск через Docker
   ``` bash
   make build
   make run
   ```

Программное средство выполнит:

- Парсинг контента из источников (Pikabu, Habr, Telegram)
- Сравнение текстов статей
- Сохранение результатов в storage/data/

📊 Результаты
- matched_posts.xlsx — совпавшие статьи
- unmatched_pikabu.xlsx, unmatched_habr.xlsx, unmatched_telegram.xlsx — уникальные посты
- JSON-файлы с сырыми данными из источников
