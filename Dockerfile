FROM python:3.10-slim

# Системные зависимости
RUN apt-get update && apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Точка монтирования (код не копируется при сборке)
VOLUME /app

CMD ["python", "main.py"]