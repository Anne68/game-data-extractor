.PHONY: build up down logs test clean setup help

# Variables
COMPOSE_FILE = docker-compose.yml
PROJECT_NAME = game-data-extractor

help: ## Afficher cette aide
	@echo "Commandes disponibles:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $1, $2}'

build: ## Construire l'image Docker
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) build

up: ## Démarrer tous les services
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) up -d

down: ## Arrêter tous les services
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) down

logs: ## Voir les logs
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) logs -f

test: ## Exécuter les tests
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec app python3 scripts/quick_test.py

extract-games: ## Extraire des jeux (test)
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec app python3 scripts/run_extraction.py --games-only --limit 10

extract-prices: ## Scraper des prix (test)
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec app python3 scripts/run_extraction.py --prices-only --limit 5

status: ## Voir le statut du système
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec app python3 scripts/run_extraction.py --status

shell: ## Accéder au shell du conteneur app
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec app bash

mysql: ## Accéder au shell MySQL
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec mysql mysql -u games_user -pgames_password games_db

clean: ## Nettoyer les containers et volumes
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) down -v
	docker system prune -f

setup: build up ## Setup complet (build + up)
	@echo "🚀 Attente du démarrage des services..."
	@sleep 10
	@make test

dev: ## Mode développement avec logs
	make up && make logs
