# Переменные для настройки
include .env
COMPOSE_FILE = docker-compose.yml
DATAGEN_CONTAINER = datagen
PROJECT_COMPOSE_NAME=etl_module3
NETWORK=$(PROJECT_COMPOSE_NAME)_app-network
MIGRATIONS_DIR=$(CURDIR)/postgres/migrations
CONFIG_FILE=$(CURDIR)/postgres/flyway.conf

# Флаг для помощи
.PHONY: help
help: ## Показать доступные команды
	@echo "Доступные команды:"
	@echo "  make build      - Запустить билд компоуз"
	@echo "  make rebuild    - Запустить компоуз с пересборкой и очисткой данных"
	@echo "  make up         - Запустить компоуз (просто поднять контейнеры)"
	@echo "  make gen-data   - Запустить генератор данных"
	@echo "  make down       - Остановить компоуз"

# 0. Запуск сборки
.PHONY: build
build: ## Полная пересборка с очисткой
	docker-compose -f $(COMPOSE_FILE) build
	docker-compose -f $(COMPOSE_FILE) up -d

# 1. Запуск с пересборкой и очисткой данных
.PHONY: rebuild
rebuild: ## Полная пересборка с очисткой
	docker-compose -f $(COMPOSE_FILE) down -v
	docker-compose -f $(COMPOSE_FILE) build --no-cache
	docker-compose -f $(COMPOSE_FILE) up -d

# 2. Запуск контейнеров
.PHONY: up
up: ## Запуск контейнеров
	docker-compose -f $(COMPOSE_FILE) up -d

# 3. Запуск генератора данных
.PHONY: gen-data
gen-data: ## Запуск генератора данных
	@echo "Check container status"
	@docker ps | findstr datagen > NUL || (echo "Container not stated. Trying to start..." && docker start datagen)
	docker exec $(DATAGEN_CONTAINER) python /code/datagen.py


# 4. Остановка compose
.PHONY: down
down: ## Остановка контейнеров
	docker-compose -f $(COMPOSE_FILE) down

# 5. Запуск миграций в postgres
.PHONY:
migrate:
	docker run --rm --network=$(NETWORK) \
	-v $(MIGRATIONS_DIR):/flyway/sql \
	-v $(CONFIG_FILE):/flyway/conf/flyway.conf \
	-e POSTGRES_DB=${POSTGRES_DB} \
	-e POSTGRES_ROOT_USER=${POSTGRES_ROOT_USER} \
	-e POSTGRES_ROOT_PASSWORD=${POSTGRES_ROOT_PASSWORD} \
	-e POSTGRES_SCHEMA=${POSTGRES_SCHEMA} \
	-e POSTGRES_HOST=${POSTGRES_HOST} \
	-e POSTGRES_PORT=${POSTGRES_PORT} \
	-e FLYWAY_PLACEHOLDERS_app_username=${POSTGRES_USER} \
	-e FLYWAY_PLACEHOLDERS_app_password=${POSTGRES_PASSWORD} \
	-e FLYWAY_PLACEHOLDERS_app_database=${POSTGRES_DB} \
	-e FLYWAY_PLACEHOLDERS_app_schema=${POSTGRES_SCHEMA} \
	flyway/flyway:latest migrate

# # 6. Запуск миграций в postgres
# .PHONY:
# migrate:
