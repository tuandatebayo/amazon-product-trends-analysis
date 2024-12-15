start-docker:
	docker compose up -d

shutdown-docker:
	docker compose down	

start-all:
	bash start-all.sh