SHELL := /bin/bash

COMPOSE_FILE := docker/docker-compose.yml
COMPOSE := docker compose -f $(COMPOSE_FILE)

.PHONY: help pipeline-up pipeline-down pipeline-restart pipeline-logs pipeline-ps pipeline-build pipeline-pull pipeline-clean run-model run-dashboard

help:
	@echo "Available targets:"
	@echo "  pipeline-up       Build and start the full pipeline"
	@echo "  pipeline-down     Stop pipeline containers"
	@echo "  pipeline-restart  Restart pipeline containers"
	@echo "  pipeline-logs     Follow logs from all services"
	@echo "  pipeline-ps       Show service status"
	@echo "  pipeline-build    Build service images"
	@echo "  pipeline-pull     Pull latest service images"
	@echo "  pipeline-clean    Stop and remove containers, volumes, orphans"
	@echo "  run-model         Run FastAPI model server locally"
	@echo "  run-dashboard     Run Streamlit dashboard locally"

pipeline-up:
	$(COMPOSE) up -d --build

pipeline-down:
	@if output="$$( $(COMPOSE) down 2>&1 )"; then \
		:; \
	else \
		status=$$?; \
		if [ "$$output" = "EOF" ]; then \
			echo "Docker daemon is not running; skipping compose teardown."; \
		else \
			printf '%s\n' "$$output"; \
			exit $$status; \
		fi; \
	fi

pipeline-restart: pipeline-down pipeline-up

pipeline-logs:
	$(COMPOSE) logs -f --tail=150

pipeline-ps:
	$(COMPOSE) ps

pipeline-build:
	$(COMPOSE) build

pipeline-pull:
	$(COMPOSE) pull

pipeline-clean:
	@if output="$$( $(COMPOSE) down -v --remove-orphans 2>&1 )"; then \
		:; \
	else \
		status=$$?; \
		if [ "$$output" = "EOF" ]; then \
			echo "Docker daemon is not running; skipping compose teardown."; \
		else \
			printf '%s\n' "$$output"; \
			exit $$status; \
		fi; \
	fi

run-model:
	uvicorn model_server.app:app --host 0.0.0.0 --port 8000 --reload

run-dashboard:
	streamlit run dashboards/streamlit.py --server.port=8501 --server.address=0.0.0.0
