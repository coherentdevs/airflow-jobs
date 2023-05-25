.PHONY: install test lint format

install:
	pip install -r requirements.txt

lint:
	pylint dags scripts

format:
	black dags scripts

upload-postgres-dag:
	gsutil cp -r dags/common gs://us-east1-contract-composer-2b295c93-bucket/dags
	gsutil cp -r dags/postgres gs://us-east1-contract-composer-2b295c93-bucket/dags
	gsutil cp requirements.txt gs://us-east1-contract-composer-2b295c93-bucket/dags