setup:
	./scripts/setup-env.sh

full-refresh:
	./scripts/setup-env.sh --full-refresh
	@echo "🧹 Data lake reset complete"

up:
	docker compose up -d --build
	docker exec -it analytics_platform bash

down:
	docker compose down

clean:
	docker compose down -v
	rm data-lake
	rm logs/* 
	rm control/*

reset-all:
	make full-refresh
	make down
	make up

rebuild:
	make down
	make up