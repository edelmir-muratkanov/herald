# Herald: команды для локальной разработки.
# Список целей: make help
#
# Строки рецептов в этом файле начинаются с табуляции (требование make).

SHELL := /bin/bash
.DEFAULT_GOAL := help

GO           ?= go
COMPOSE_FILE ?= deployments/docker-compose.yml
COMPOSE      := docker compose -f $(COMPOSE_FILE)
ALL_PROFILES := --profile rabbitmq --profile providers --profile tools
ENV_FILE     := deployments/.env

# Адрес запущенного API для контрактных тестов.
BASE_URL ?= http://localhost:8081
# Сервис для логов: make logs SVC=kafka
SVC ?=

.PHONY: help
help: ## Показать список команд
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---- Инфраструктура (Docker Compose) ----

# Создаёт .env из шаблона, только если его ещё нет (order-only: существующий файл не перезаписывается).
$(ENV_FILE): | deployments/.env.example
	cp deployments/.env.example $(ENV_FILE)
	@echo "Создан $(ENV_FILE). Проверьте значения."

.PHONY: env
env: $(ENV_FILE) ## Создать deployments/.env из шаблона, если его нет

.PHONY: up
up: $(ENV_FILE) ## Запустить PostgreSQL и Kafka (создаёт топики)
	$(COMPOSE) up -d

.PHONY: up-rabbitmq
up-rabbitmq: $(ENV_FILE) ## Запустить базовые сервисы и RabbitMQ
	$(COMPOSE) --profile rabbitmq up -d

.PHONY: up-providers
up-providers: $(ENV_FILE) ## Запустить базовые сервисы и заглушки провайдеров (Mailpit, providersim)
	$(COMPOSE) --profile providers up -d --build

.PHONY: up-all
up-all: $(ENV_FILE) ## Запустить всё: базовые сервисы, RabbitMQ, заглушки, Kafka UI
	$(COMPOSE) $(ALL_PROFILES) up -d --build

.PHONY: down
down: ## Остановить все сервисы (данные сохраняются)
	$(COMPOSE) $(ALL_PROFILES) down

.PHONY: reset
reset: ## Остановить все сервисы и УДАЛИТЬ данные (тома PostgreSQL, Kafka, RabbitMQ)
	@read -r -p "Все данные будут удалены. Продолжить? [y/N] " ans && [ "$$ans" = "y" ]
	$(COMPOSE) $(ALL_PROFILES) down -v

.PHONY: ps
ps: ## Состояние сервисов
	$(COMPOSE) $(ALL_PROFILES) ps -a

.PHONY: logs
logs: ## Логи сервисов (make logs SVC=kafka — только один)
	$(COMPOSE) $(ALL_PROFILES) logs -f --tail=100 $(SVC)

.PHONY: psql
psql: ## Консоль psql в контейнере PostgreSQL
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

.PHONY: kafka-topics
kafka-topics: ## Список топиков Kafka
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# ---- Go ----

.PHONY: fmt
fmt: ## Отформатировать код
	$(GO) fmt ./...

.PHONY: vet
vet: ## Статический анализ (go vet)
	$(GO) vet ./...

.PHONY: test
test: ## Юнит-тесты
	$(GO) test ./...

.PHONY: test-race
test-race: ## Юнит-тесты с детектором гонок
	$(GO) test -race ./...

.PHONY: test-contract
test-contract: ## Контрактные тесты HTTP API (сервер и PostgreSQL должны быть запущены)
	HERALD_BASE_URL=$(BASE_URL) $(GO) test -tags=contract -count=1 ./test/contract/...

.PHONY: check
check: fmt vet test-race ## Проверки перед коммитом: fmt, vet, тесты с -race

.PHONY: generate
generate: ## Сгенерировать код (go generate)
	$(GO) generate ./...

.PHONY: generate-check
generate-check: generate ## Убедиться, что сгенерированный код закоммичен
	@git diff --exit-code -- . ':(exclude)go.sum' || (echo "Сгенерированный код устарел: выполните make generate и закоммитьте"; exit 1)

.PHONY: tidy
tidy: ## Привести go.mod и go.sum в порядок
	$(GO) mod tidy

.PHONY: build
build: ## Собрать все команды из cmd/ в каталог bin/
	$(GO) build -o bin/ ./cmd/...

.PHONY: run-api
run-api: ## Запустить API локально (нужны PostgreSQL и переменные окружения приложения)
	$(GO) run ./cmd/api

.PHONY: providersim
providersim: ## Запустить providersim локально, без Docker (http://localhost:8090)
	cd deployments/providersim && $(GO) run .