
.PHONY: setup dataset etl train clean bump-patch bump-minor bump-major

setup:
	pip install poetry
	poetry install

dataset:
	poetry run python src/datagen/main.py --rows 1000000

etl:
	poetry run python src/jobs/ingestion_bronze.py
	poetry run python src/jobs/refine_silver.py
	poetry run python src/jobs/agg_gold.py

train:
	poetry run python src/ml/train_churn.py

clean:
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -rf data/gold/*
	rm -rf spark-warehouse
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -rf {} +

bump-patch:
	poetry run bump2version patch --verbose

bump-minor:
	poetry run bump2version minor --verbose

bump-major:
	poetry run bump2version major --verbose