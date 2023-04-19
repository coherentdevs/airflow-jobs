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
    "decode_blocks_hourly",
    default_args=default_args,
    description="DAG to decode blocks hourly",
    schedule_interval=timedelta(hours=1),
    catchup=False,
)


run_incremental_model = BashOperator(
    task_id='run_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --models decoded_blocks '
                 '--target production --vars \"{"raw_database": '
                 '"ethereum_managed", "contracts_database": "contracts"}\" ',
    dag=dag
)

run_incremental_model