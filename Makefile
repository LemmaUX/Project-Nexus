PYTHON ?= python
COMPOSE ?= docker compose

.PHONY: test up down validate

test:
	$(PYTHON) -m unittest discover -s tests -v

validate:
	$(PYTHON) -m unittest discover -s tests -v

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v
