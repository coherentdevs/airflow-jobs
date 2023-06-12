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
    "backfill_decoded_polygon_tables",
    default_args=default_args,
    description="DAG to backfill polygon logs, traces, and transactions in parallel",
    schedule_interval=None,
    catchup=False,
    tags=['backfill']
)


backfill_polygon_transactions_dbt_model = BashOperator(
    task_id='backfill_polygon_transactions_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_polygon_transactions '
                 '--target polygon-production --vars \"{"polygon_raw_database": "polygon_managed", "polygon_decoded_database": '
                 '"polygon_managed", "polygon_decoded_schema": "decoded",'
                 '"contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

backfill_polygon_traces_dbt_model = BashOperator(
    task_id='backfill_polygon_traces_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_polygon_traces '
                 '--target polygon-production --vars \"{"polygon_raw_database": "polygon_managed", "polygon_decoded_database": '
                 '"polygon_managed", "polygon_decoded_schema": "decoded",'
                 '"contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

backfill_polygon_logs_dbt_model = BashOperator(
    task_id='backfill_polygon_logs_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_polygon_logs '
                 '--target polygon-production --vars \"{"polygon_raw_database": "polygon_managed", "polygon_decoded_database": '
                 '"polygon_managed", "polygon_decoded_schema": "decoded",'
                 '"contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

[backfill_polygon_logs_dbt_model, backfill_polygon_traces_dbt_model, backfill_polygon_transactions_dbt_model]