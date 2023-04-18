from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import queries, config

SNOWFLAKE_CONN_ID="snowflake_ethereum_small"
MAX_COUNT=100

with DAG(
        "create-new-eth-tables",
        description="""
        raw ethereum tables
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
) as dag:
    """
    #### Ethereum Block table creation
    """
    init_blocks_table = SnowflakeOperator(
        task_id="init_blocks_table",
        sql=queries.SQL_CREATE_ETH_RAW_BLOCKS_TABLE,
        params={"table_name": config.BLOCKS_TABLE_NAME},
    )
    init_transactions_table = SnowflakeOperator(
        task_id="init_transactions_table",
        sql=queries.SQL_CREATE_ETH_RAW_TRANSACTIONS_TABLE,
        params={"table_name": config.TRANSACTIONS_TABLE_NAME},
    )
    init_traces_table = SnowflakeOperator(
        task_id="init_traces_table",
        sql=queries.SQL_CREATE_ETH_RAW_TRACES_TABLE,
        params={"table_name": config.TRACES_TABLE_NAME},
    )
    init_logs_table = SnowflakeOperator(
        task_id="init_logs_table",
        sql=queries.SQL_CREATE_ETH_RAW_LOGS_TABLE,
        params={"table_name": config.LOGS_TABLE_NAME},
    )


    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> [init_blocks_table, init_transactions_table, init_traces_table, init_logs_table] >> end