from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import config, optimism_queries

SNOWFLAKE_CONN_ID="snowflake_ethereum_small"
MAX_COUNT=100

with DAG(
        "create-new-optimism-tables",
        description="""
        raw optimism tables
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        schedule_interval=None,
        default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
) as dag:
    """
    #### Optimism Block table creation
    """
    init_optimism_blocks_table = SnowflakeOperator(
        task_id="init_optimism_blocks_table",
        sql=optimism_queries.SQL_CREATE_OPTIMISM_RAW_BLOCKS_TABLE,
        params={"table_name": config.BLOCKS_TABLE_NAME},
    )
    init_optimism_transactions_table = SnowflakeOperator(
        task_id="init_optimism_transactions_table",
        sql=optimism_queries.SQL_CREATE_OPTIMISM_RAW_TRANSACTIONS_TABLE,
        params={"table_name": config.TRANSACTIONS_TABLE_NAME},
    )
    init_optimism_traces_table = SnowflakeOperator(
        task_id="init_optimism_traces_table",
        sql=optimism_queries.SQL_CREATE_OPTIMISM_RAW_TRACES_TABLE,
        params={"table_name": config.TRACES_TABLE_NAME},
    )
    init_optimism_logs_table = SnowflakeOperator(
        task_id="init_optimism_logs_table",
        sql=optimism_queries.SQL_CREATE_OPTIMISM_RAW_LOGS_TABLE,
        params={"table_name": config.LOGS_TABLE_NAME},
    )


    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> [init_optimism_blocks_table, init_optimism_transactions_table, init_optimism_traces_table, init_optimism_logs_table] >> end