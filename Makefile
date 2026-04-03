PYTHON ?= python
VENV ?= .venv
APP ?= src/main.py
INGESTION_DATE ?= 2024-01-01
DATA_SIZE ?= small

ifeq ($(OS),Windows_NT)
	PYTHON_BIN=$(VENV)/Scripts/python.exe
	PIP_BIN=$(VENV)/Scripts/pip.exe
else
	PYTHON_BIN=$(VENV)/bin/python
	PIP_BIN=$(VENV)/bin/pip
endif

.PHONY: install data pipeline validate gold monitor test docker-build docker-run clean

install:
	$(PYTHON) -m venv $(VENV)
	"$(PIP_BIN)" install -r requirements.txt

data:
	"$(PYTHON_BIN)" $(APP) generate-data --size $(DATA_SIZE) --ingestion-date $(INGESTION_DATE)

pipeline:
	"$(PYTHON_BIN)" $(APP) run-pipeline --ingestion-date $(INGESTION_DATE)

validate:
	"$(PYTHON_BIN)" $(APP) validate-only --ingestion-date $(INGESTION_DATE)

gold:
	"$(PYTHON_BIN)" $(APP) build-gold --ingestion-date $(INGESTION_DATE)

monitor:
	"$(PYTHON_BIN)" $(APP) monitoring-summary --limit 5

test:
	"$(PYTHON_BIN)" -m pytest -q

docker-build:
	docker build -t ddpp:latest .

docker-run:
	docker-compose up --build

clean:
	@if exist $(VENV) rmdir /s /q $(VENV) && echo "Removed venv" || true
	powershell -Command "Get-ChildItem data -Recurse -Include *.csv,*.json,*.parquet | Remove-Item -Force" 2>$null || true
