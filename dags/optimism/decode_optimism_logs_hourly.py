from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime.now() - timedelta(days=1),
}

# Instantiate the DAG
dag = DAG(
    "decode_optimism_logs_hourly",
    default_args=default_args,
    description="DAG to decode optimism logs hourly",
    schedule_interval='0 * * * *',
    catchup=False,
)


run_incremental_optimism_logs_dbt_model = BashOperator(
    task_id='run_incremental_optimism_logs_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_optimism_logs '
                 '--target optimism-production --vars \"{"optimism_raw_database": '
                 '"optimism_managed", "contracts_database": "contracts"}\" ',
    dag=dag
)

run_incremental_optimism_logs_dbt_model
