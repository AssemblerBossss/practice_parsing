import json
import openpyxl
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
from typing import Literal
from datetime import datetime
from loggers import setup_logger
from storage.storage_config import (DATA_DIR, ALLOWED_FILES)

logger = setup_logger("saving_logger")


class DataStorage:
    @staticmethod
    def save_as_json(posts, filename: Literal['habr', 'pikabu', 'telegram']) -> bool:
        """
        Сохраняет посты в JSON файл.
        Args:
            posts: Данные для сохранения
            filename: Имя файла (без расширения)
        Raises:
            ValueError: При недопустимом имени файла
        """
        DATA_DIR.mkdir(exist_ok=True, parents=True)

        if filename not in ALLOWED_FILES:
            logger.error("Указано неверное имя для сохранения в json: %s. Допустимые: %s",
                         filename, list(ALLOWED_FILES.keys()))
            raise ValueError(f"Invalid filename. Allowed names: {list(ALLOWED_FILES.keys())}")
        filename = filename + '.json'
        file_path = DATA_DIR / filename

        output_data = {
            'metadata': {
                'generated_at' : datetime.now().isoformat(),
                "posts_count": len(posts)
            },
            'posts': posts
        }

        try:
            with open(file_path , 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=4)
            logger.info("Saved %d posts to %s", len(posts), file_path)
            return True
        except Exception as e:
            logger.error("Failed to save posts: %s", str(e))
            return False

    @staticmethod
    def read_json(source: Literal['habr', 'pikabu', 'telegram']) -> list[dict[str, str]]:
        """
        Чтение посты из JSON файла.
        Args:
            source: Источник данных для чтения
        Returns:
            list[dict]: Список постов или None при ошибке
        Raises:
            ValueError: При недопустимом имени источника
        """

        if source not in ALLOWED_FILES:
            logger.error("Недопустимый источник: %s. Допустимые: %s",
                         source, list(ALLOWED_FILES.keys()))
            raise ValueError(f"Invalid source. Allowed: {list(ALLOWED_FILES.keys())}")

        file_name = ALLOWED_FILES[source]
        file_path = DATA_DIR / file_name

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f).get('posts', [])
            logger.info("Successfully read data from %s", file_path)
            return data
        except FileNotFoundError:
            logger.error("File not found: %s", file_path)
            return None
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format in %s: %s", file_path, str(e))
            return None
        except Exception as e:
            logger.error("Error reading file %s: %s", file_path, str(e))
            return None

    @staticmethod
    def save_to_excel(similar_posts: list, filename: str = 'similar_posts.xlsx') -> bool:
        """
        Сохраняет посты в XLSX файл.
        Args:
            similar_posts: Данные для сохранения
            filename: Имя файла (без расширения)
        """

        DATA_DIR.mkdir(exist_ok=True, parents=True)
        file_path = DATA_DIR / filename

        try:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = 'Схожие посты'

            # Заголовки столбцов
            headers = [
                "№",
                "Платформа",
                "Заголовок (Habr)",
                "Дата (Habr)",
                "N-грамм (Habr)",
                "ID (Telegram)",
                "Дата (Telegram)",
                "N-грамм (Telegram)",
                "Оценка схожести"
            ]

            # Форматирование заголовков
            header_font = Font(bold=True)
            header_alignment = Alignment(horizontal='center', vertical='center')

            for col_num, header in enumerate(headers, 1):
                cell = ws.cell(row=1, column=col_num, value=header)  # Создает ячейку в Excel-листе
                cell.font = header_font                              # Устанавливает жирный шрифт
                cell.alignment = header_alignment                    # Выравнивание по центру

            for row_num, post in enumerate(similar_posts, 2):
                source, h_title, h_date, t_id, t_date, score, t_len, h_len = post

                ws.cell(row=row_num, column=1, value=row_num - 1)  # №
                ws.cell(row=row_num, column=2, value=source)  # Платформа
                ws.cell(row=row_num, column=3, value=h_title) # Заголовок Habr
                ws.cell(row=row_num, column=4, value=h_date)  # Дата Habr
                ws.cell(row=row_num, column=5, value=h_len)   # N-грамм Habr
                ws.cell(row=row_num, column=6, value=t_id)    # ID Telegram
                ws.cell(row=row_num, column=7, value=t_date)  # Дата Telegram
                ws.cell(row=row_num, column=8, value=t_len)   # N-грамм Telegram
                ws.cell(row=row_num, column=9, value=score)  # Оценка схожести

            # Настраиваем ширину столбцов
            column_widths = {
                'A': 5,  # №
                'B': 10,  # Платформа
                'C': 50,  # Заголовок Habr
                'D': 20,  # Дата Habr
                'E': 10,  # N-грамм Habr
                'F': 15,  # ID Telegram
                'H': 20,  # Дата Telegram
                'J': 10,  # N-грамм Telegram
                'I': 15  # Оценка схожести
            }

            for col, width in column_widths.items():
                ws.column_dimensions[col].width = width

            # Добавляем дату генерации отчета
            ws.cell(row=ws.max_row + 2, column=1,
                    value=f"Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # Сохраняем файл
            wb.save(file_path)
            logger.info("Результаты сохранены в файл: %s", filename)

        except Exception as e:
            logger.error("Ошибка при сохранении в Excel: %s", str(e))
            raise

    @staticmethod
    def save_telegram_to_excel(posts: list[dict], filename: str = "unmatched_telegram.xlsx"):
        DATA_DIR.mkdir(exist_ok=True, parents=True)
        file_path = DATA_DIR / filename

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Unmatched Telegram Posts"

        if not posts:
            ws.append(["Нет данных"])
            wb.save(file_path)
            return

        headers = list(posts[0].keys())
        ws.append(headers)

        header_font = Font(bold=True)
        header_alignment = Alignment(horizontal='center')

        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=1, column=col_idx)
            cell.font = header_font
            cell.alignment = header_alignment

        for post in posts:
            row = []
            for key in headers:
                value = post.get(key, '')
                if key == "text" and isinstance(value, str):
                    value = "'" + value
                    cell = WriteOnlyCell(ws, value=value)
                    cell.number_format = '@'  # Принудительно форматировать как текст
                    cell.alignment = Alignment(wrap_text=True)
                    row.append(cell)
                else:
                    row.append(value)
            ws.append(row)

        for column_cells in ws.columns:
            length = max(len(str(cell.value)) if cell.value else 0 for cell in column_cells)
            adjusted_width = min(length + 2, 50)
            ws.column_dimensions[column_cells[0].column_letter].width = adjusted_width

        wb.save(file_path)
