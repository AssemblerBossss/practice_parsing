# practice_parsing

```bash
docker build -t post-comporator
```

```bash
docker run -it --rm -v $(pwd):/app -e LOGS_DIR=/app/logs -e STORAGE_DIR=/app/storage/data -e SESSION_FILE=/app/session.session post-comparator
```