.PHONY: help install pipeline warehouse api dashboard test clean

help:
	@echo "DataFlow - Available Commands"
	@echo "-----------------------------"
	@echo "make install      Install dependencies"
	@echo "make pipeline     Run full ETL pipeline (default: 2024-01-01 hour 0)"
	@echo "make warehouse    Build DuckDB warehouse from gold layer"
	@echo "make api          Start FastAPI analytics API"
	@echo "make dashboard    Start Streamlit dashboard"
	@echo "make test         Run test suite"
	@echo "make clean        Remove cached files"

install:
	pip install -r requirements.txt
	pip install -e .

pipeline:
	python flows/etl_flow.py

warehouse:
	python -m src.warehouse.db

api:
	uvicorn src.api.main:app --reload --port 8000

dashboard:
	streamlit run src/dashboard/app.py

test:
	pytest tests/ -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete