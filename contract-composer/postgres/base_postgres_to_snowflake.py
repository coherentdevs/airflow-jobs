import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from common import config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "start_date": datetime.now() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'base_testnet_backfill_contracts',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

SNOWFLAKE_CONN_ID = 'snowflake_contracts_xsmall'
POSTGRES_CONN_ID = 'postgres_connection'
BLOCKCHAIN = 'base'
SNOWFLAKE_CONTRACTS_TABLE = "base_testnet"

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL


def get_snowflake_engine():
    engine = create_engine(URL(
        account=config.SNOWFLAKE_ACCOUNT,
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        role=config.SNOWFLAKE_ROLE,
    ))
    return engine


def get_snowflake_address_list():
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    query = f"SELECT address FROM {SNOWFLAKE_CONTRACTS_TABLE}"
    result = snowflake_hook.get_pandas_df(sql=query)
    return result['ADDRESS']


def query_postgres_for_new_rows(ti):
    address_list = ti.xcom_pull(task_ids='get_snowflake_address_list')
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    address_list_str = ', '.join(["'" + str(address) + "'" for address in address_list])

    query = f"""
        SELECT 'address', 'name', 'symbol', 'official_name', 'standard', 'abi', 'decimals' 
        FROM {config.CONTRACTS_TABLE_NAME} 
        WHERE 'address' NOT IN ({address_list_str}) AND BLOCKCHAIN = '{BLOCKCHAIN}'"""
    result = postgres_hook.get_pandas_df(sql=query)

    export_new_rows_to_snowflake(result)
    logging.info("successful!")


import pandas as pd

def export_new_rows_to_snowflake(df):
    engine = get_snowflake_engine()
    column_names = ['address', 'name', 'symbol', 'official_name', 'standard', 'abi', 'decimals']
    df.columns = column_names  # Set the column names of the DataFrame

    # Convert 'decimals' column to nullable integer type
    df['decimals'] = pd.to_numeric(df['decimals'], errors='coerce').astype('Int64')

    try:
        rows_affected = df.to_sql(SNOWFLAKE_CONTRACTS_TABLE, engine, method='multi', if_exists='append', index=False, chunksize=int(config.BATCH_SIZE))
    except Exception as e:
        raise e
    logging.info(f"inserted {rows_affected} rows into snowflake")




t1 = PythonOperator(
    task_id='get_snowflake_address_list',
    python_callable=get_snowflake_address_list,
    dag=dag,
)

t2 = PythonOperator(
    task_id='query_and_insert_to_postgres',
    python_callable=query_postgres_for_new_rows,
    provide_context=True,
    dag=dag,
)

t1 >> t2
