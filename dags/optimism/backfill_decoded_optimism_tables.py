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
    "backfill_decoded_optimism_tables",
    default_args=default_args,
    description="DAG to backfill optimism logs, traces, and transactions in parallel",
    schedule_interval=None,
    catchup=False,
    tags=['backfill']
)


backfill_optimism_transactions_dbt_model = BashOperator(
    task_id='backfill_optimism_transactions_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_optimism_transactions '
                 '--target optimism-production --vars \"{"optimism_raw_database": "optimism_managed", "optimism_decoded_database": '
                 '"optimism_managed", "optimism_decoded_schema": "decoded",'
                 '"contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

backfill_optimism_traces_dbt_model = BashOperator(
    task_id='backfill_optimism_traces_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_optimism_traces '
                 '--target optimism-production --vars \"{"optimism_raw_database": "optimism_managed", "optimism_decoded_database": '
                 '"optimism_managed", "optimism_decoded_schema": "decoded",'
                 '"contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

backfill_optimism_logs_dbt_model = BashOperator(
    task_id='backfill_optimism_logs_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_optimism_logs '
                 '--target optimism-production --vars \"{"optimism_raw_database": "optimism_managed", "optimism_decoded_database": '
                 '"optimism_managed", "optimism_decoded_schema": "decoded",'
                 '"contracts_database": "contracts", "backfill": True}\" ',
    dag=dag
)

[backfill_optimism_logs_dbt_model, backfill_optimism_traces_dbt_model, backfill_optimism_transactions_dbt_model]