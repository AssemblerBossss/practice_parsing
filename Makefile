build:
	docker build -t post-comparator .


run:
	docker run -it --rm \
	  -e LOGS_DIR=/app/logs \
	  -e STORAGE_DIR=/app/storage/data \
	  -e SESSION_FILE=/app/session.session \
	  post-comparator