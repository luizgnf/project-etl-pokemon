help:
	@echo ""
	@echo "Makefile for managing the local Airflow environment"
	@echo ""
	@echo "Available commands:"
	@echo "  make compose-build     - Build the Docker containers with docker compose"
	@echo "  make compose-up        - Start all services in detached mode"
	@echo "  make compose-down      - Stop and remove all containers"
	@echo "  make compose-clean     - Remove all volumes and orphan services"
	@echo "  make airflow-logs      - Tail logs from the Airflow webserver"
	@echo "  make airflow-bash      - Open a bash shell inside the Airflow webserver container"
	@echo "  make duckdb-open       - Access your DuckDB CLI"
	@echo "  make metabase-start    - Start the current Metabase service"
	@echo "  make metabase-stop     - Stop the current Metabase service"
	@echo ""

compose-build:
	@docker compose build

compose-up:
	@docker compose up -d

compose-down:
	@docker compose down

compose-clean:
	@docker compose down --volumes --remove-orphans
	@docker volume prune -f

airflow-logs:
	@docker compose logs -f airflow-webserver

airflow-bash:
	@docker compose exec airflow-webserver bash

metabase-start:
	@docker compose start metabase

metabase-stop:
	@docker compose stop metabase