from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import logging
import os

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
    "decode_dag",
    default_args=default_args,
    description="DAG to load parquet files from GCS to Snowflake and run incremental dbt models",
    schedule_interval=None,
    catchup=False,
)


def extract_gcs_path(**kwargs):
    gcs_path = kwargs['dag_run'].conf['name']
    gcs_path_split = gcs_path.split("/")
    type = gcs_path_split[0]
    temp_table_name = gcs_path.replace(".parquet", "").replace("/", "_").replace("-", "_")
    kwargs["ti"].xcom_push("type", type)
    kwargs["ti"].xcom_push("gcs_path", gcs_path)
    kwargs["ti"].xcom_push("temp_table_name", temp_table_name)

    logging.info(f"Extracted gcs_path: {gcs_path}")

extract_gcs_path_task = PythonOperator(
    task_id="extract_gcs_path",
    python_callable=extract_gcs_path,
    provide_context=True,
    dag=dag,
)

def log_success(context):
    logging.info("Successfully loaded parquet file into temp table")


def log_failure(context):
    logging.error("Failed to load parquet file into temp table")


load_parquet_to_temp_table = SnowflakeOperator(
    task_id="load_parquet_to_temp_table",
    sql="""
        CREATE OR REPLACE TABLE temporary_incremental.{{ ti.xcom_pull(key='temp_table_name') }} LIKE ETHEREUM_MANAGED.RAW.{{ ti.xcom_pull(key='type') }};
        COPY INTO temporary_incremental.{{ ti.xcom_pull(key='temp_table_name') }}
            FROM @ETHEREUM_MANAGED.RAW.ETH_RAW_STAGE
            file_format = parquet_format
            files = ('{{ ti.xcom_pull(key='gcs_path') }}')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """,
    snowflake_conn_id="snowflake_temporary_incremental",
    on_success_callback=log_success,
    on_failure_callback=log_failure,
    dag=dag
)

run_incremental_model = BashOperator(
    task_id='run_dbt_model',
    bash_command='cd /home/airflow/gcs/dags/evm-models/ && dbt run --models decoded_{{ ti.xcom_pull(key="type") }} '
                 '--target production --vars \"{"raw_schema": "temporary_incremental", "raw_database": '
                 '"ethereum_managed", "source_table_{{ ti.xcom_pull(key=\"type\") }}": "{{'
                 'ti.xcom_pull(key=\"temp_table_name\") }}", "contracts_database": "contracts", "contracts_schema": "evm" }\" ',
    timeout=600,
    dag=dag
)

drop_temp_table = SnowflakeOperator(
    task_id="drop_temp_table",
    sql="DROP TABLE IF EXISTS temporary_incremental.{{ ti.xcom_pull(key='temp_table_name') }};",
    snowflake_conn_id="snowflake_temporary_incremental",
    dag=dag
)

extract_gcs_path_task >> load_parquet_to_temp_table >> run_incremental_model >> drop_temp_table
