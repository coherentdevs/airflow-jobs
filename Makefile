.PHONY: install test lint format

install:
	pip install -r requirements.txt

lint:
	pylint dags scripts

format:
	black dags scripts