from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime.now() - timedelta(days=1),
}

# Instantiate the DAG
dag = DAG(
    "backfill_decoded_tables",
    default_args=default_args,
    description="DAG to backfill logs, traces, and transactions in parallel",
    schedule_interval=None,
    catchup=False,
    tags=['backfill']
)


backfill_transactions_dbt_model = BashOperator(
    task_id='backfill_transactions_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_transactions '
                 '--target production --vars \"{"raw_database": "ethereum_managed", "decoded_database": '
                 '"ethereum_managed", "decoded_schema": "decoded",'
                 '"contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

backfill_traces_dbt_model = BashOperator(
    task_id='backfill_traces_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_traces '
                 '--target production --vars \"{"decoded_database": "ethereum_managed", "decoded_schema": "decoded", '
                 '"raw_database": "ethereum_managed", "contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

backfill_logs_dbt_model = BashOperator(
    task_id='backfill_logs_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_logs '
                 '--target production --vars \"{"raw_database": '
                 '"ethereum_managed", "decoded_database": "ethereum_managed", "decoded_schema": "decoded", '
                 '"contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

[backfill_logs_dbt_model, backfill_traces_dbt_model, backfill_transactions_dbt_model]