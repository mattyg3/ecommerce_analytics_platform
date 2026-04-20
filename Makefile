setup:
	./scripts/setup-env.sh

full-refresh:
	./scripts/setup-env.sh --full-refresh
	@echo "🧹 Data lake reset complete"

up:
	docker compose up --build

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf data-lake
	rm -rf logs control

reset-all:
	make full-refresh
	make down
	make up