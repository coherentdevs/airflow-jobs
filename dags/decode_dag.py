from datetime import timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from dbt_cloud_operator import DbtCloudOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    "decode_dag",
    default_args=default_args,
    description="DAG for decoding data using two SQL scripts",
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2023, 4, 11),
    catchup=False,
)

# Task 1: Join raw data with method or event fragments
join_raw_data = SnowflakeOperator(
    task_id="join_raw_data",
    sql="path/to/join_raw_data.sql", # TODO
    snowflake_conn_id="your_snowflake_connection", # TODO
    autocommit=True,
    dag=dag,
)

# Task 2: Run decoding DBT job
run_decoding_dbt_job = SnowflakeOperator(
    task_id="run_decoding_dbt_job",
    sql="path/to/run_decoding_dbt_job.sql", # TODO
    snowflake_conn_id="your_snowflake_connection", # TODO
    autocommit=True,
    dag=dag,
)

# Set task dependencies
join_raw_data >> run_decoding_dbt_job