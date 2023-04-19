from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import queries, config

SNOWFLAKE_CONN_ID="snowflake_ethereum_small"
MAX_COUNT=100

with DAG(
        "create-tip-pipes",
        description="""
        raw ethereum tip pipes
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
) as dag:

    """
    #### Ethereum pipes creation
    """
    init_blocks_pipe = SnowflakeOperator(
        task_id="init_eth_blocks_pipe",
        sql=queries.SQL_CREATE_ETH_RAW_BLOCKS_PIPE,
        params={"pipe_name": config.BLOCKS_TABLE_NAME},
    )
    init_transactions_pipe = SnowflakeOperator(
        task_id="init_eth_transactions_pipe",
        sql=queries.SQL_CREATE_ETH_RAW_TRANSACTIONS_PIPE,
        params={"pipe_name": config.TRANSACTIONS_TABLE_NAME},
    )
    init_traces_pipe = SnowflakeOperator(
        task_id="init_eth_traces_pipe",
        sql=queries.SQL_CREATE_ETH_RAW_TRACES_PIPE,
        params={"pipe_name": config.TRACES_TABLE_NAME},
    )
    init_logs_pipe = SnowflakeOperator(
        task_id="init_eth_logs_pipe",
        sql=queries.SQL_CREATE_ETH_RAW_LOGS_PIPE,
        params={"pipe_name": config.LOGS_TABLE_NAME},
    )



    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> [init_blocks_pipe, init_transactions_pipe, init_traces_pipe, init_logs_pipe] >> end