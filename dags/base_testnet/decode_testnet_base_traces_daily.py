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
    "decode_testnet_base_traces_daily",
    default_args=default_args,
    description="DAG to decode testnet_base traces every day",
    schedule_interval='0 0 * * *',
    catchup=False,
)


run_incremental_testnet_base_traces_dbt_model = BashOperator(
    task_id='run_incremental_testnet_base_traces_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --select decoded_testnet_base_traces '
                 '--target testnet-base-production --vars \"{"testnet_base_raw_database": '
                 '"base_managed", "contracts_database": "contracts"}\" ',
    dag=dag
)

run_incremental_testnet_base_traces_dbt_model
