build:
	docker build -t post-comparator .


run:
	docker run -it --rm \
	  -v $(PWD)/logs:/app/logs \
	  -v $(PWD)/storage/data:/app/storage/data \
	  -v $(PWD)/session.session:/app/session.session \
	  post-comparator