# Airflow Jobs

This repository contains the Apache Airflow DAGs and related scripts for building incremental decoded tables in Snowflake and BigQuery. The goal is to keep the decoded and enriched data up to date as we're indexing the raw chain tip data live.

## Overview

The service consists of three main components:

1. **Airflow**: Manages and orchestrates the Directed Acyclic Graph (DAG) that runs periodically every minute, executing incremental DBTs.
2. **Snowflake and BigQuery**: Store the raw and decoded data and perform necessary joins and transformations.
3. **DBT**: Performs data transformations in Snowflake and BigQuery using incremental DBT models.

## Requirements

- Python 3.x
- Apache Airflow 2.2.3
- Apache Airflow Providers Google 6.0.0
- Apache Airflow Providers Snowflake 1.3.1
- Google Cloud Storage 1.43.0
- DBT Cloud Operator 0.1.0

## Getting Started

1. Clone this repository.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Set up the necessary environment variables, connections, and configurations for your Apache Airflow instance, Snowflake, BigQuery, and DBT Cloud.
4. Follow the deployment steps mentioned in the previous sections of this document to sync the DAGs to a Google Cloud Storage bucket and configure the Airflow instance to use the GCS bucket as its DAG folder.
5. Monitor the performance of your Airflow DAGs, Snowflake, BigQuery, and DBT tasks using Datadog and the data consistency job.

## Contributing 

If you wish to contribute to this project, please follow the standard GitHub workflow:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Make your changes and commit them to your branch.
4. Create a pull request, describing the changes you've made and the problem they solve.
5. The maintainers will review your pull request and, if approved, merge it into the main branch.