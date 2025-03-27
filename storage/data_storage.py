import json
from datetime import datetime
from pathlib import Path
from loggers import setup_logger


logger = setup_logger("saving_logger")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

class DataStorage:
    @staticmethod
    def save_as_json(posts, file_name: str) -> bool:
        DATA_DIR.mkdir(exist_ok=True, parents=True)
        file_path = DATA_DIR / file_name

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
            logger.info(f"Saved {len(output_data)} posts to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save posts: {str(e)}")
            return False
    #
    # def read_json(filename, subfolder="json"):
    #     """Чтение данных из JSON файла"""
    #     try:
    #         with open(os.path.join(subfolder, filename), 'r', encoding='utf-8') as f:
    #             return json.load(f)
    #     except Exception as e:
    #         logger.error(f"File read error: {str(e)}")
    #         return None