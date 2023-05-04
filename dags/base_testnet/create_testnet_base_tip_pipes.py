from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import config, base_queries

SNOWFLAKE_CONN_ID="snowflake_base_small"

with DAG(
        "create-testnet-base-tip-pipes",
        description="""
        raw base tip pipes
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        schedule_interval=None,
        default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
) as dag:

    """
    #### Testnet base pipes creation
    """
    init_testnet_base_blocks_pipe = SnowflakeOperator(
        task_id="init_testnet_base_blocks_pipe",
        sql=base_queries.SQL_CREATE_TESTNET_BASE_RAW_BLOCKS_PIPE,
        params={"pipe_name": config.BLOCKS},
    )
    init_testnet_base_transactions_pipe = SnowflakeOperator(
        task_id="init_testnet_base_transactions_pipe",
        sql=base_queries.SQL_CREATE_TESTNET_BASE_RAW_TRANSACTIONS_PIPE,
        params={"pipe_name": config.TRANSACTIONS},
    )
    init_testnet_base_traces_pipe = SnowflakeOperator(
        task_id="init_testnet_base_traces_pipe",
        sql=base_queries.SQL_CREATE_TESTNET_BASE_RAW_TRACES_PIPE,
        params={"pipe_name": config.TRACES},
    )
    init_testnet_base_logs_pipe = SnowflakeOperator(
        task_id="init_testnet_base_logs_pipe",
        sql=base_queries.SQL_CREATE_TESTNET_BASE_RAW_LOGS_PIPE,
        params={"pipe_name": config.LOGS},
    )



    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> [init_testnet_base_blocks_pipe, init_testnet_base_transactions_pipe, init_testnet_base_traces_pipe, init_testnet_base_logs_pipe] >> end