FROM python:3.10-slim


RUN apt-get update && apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ВЕСЬ проект (включая main.py, parsers/, storage/ и т.д.)
COPY . .

CMD ["python", "main.py"]