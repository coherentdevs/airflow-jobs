from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import config, optimism_queries

SNOWFLAKE_CONN_ID="snowflake_optimism_small"

with DAG(
        "create-optimism-tip-pipes",
        description="""
        raw optimism tip pipes
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        schedule_interval=None,
        default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
) as dag:

    """
    #### Optimism pipes creation
    """
    init_optimism_blocks_pipe = SnowflakeOperator(
        task_id="init_optimism_blocks_pipe",
        sql=optimism_queries.SQL_CREATE_OPTIMISM_RAW_BLOCKS_PIPE,
        params={"pipe_name": config.BLOCKS},
    )
    init_optimism_transactions_pipe = SnowflakeOperator(
        task_id="init_optimism_transactions_pipe",
        sql=optimism_queries.SQL_CREATE_OPTIMISM_RAW_TRANSACTIONS_PIPE,
        params={"pipe_name": config.TRANSACTIONS},
    )
    init_optimism_traces_pipe = SnowflakeOperator(
        task_id="init_optimism_traces_pipe",
        sql=optimism_queries.SQL_CREATE_OPTIMISM_RAW_TRACES_PIPE,
        params={"pipe_name": config.TRACES},
    )
    init_optimism_logs_pipe = SnowflakeOperator(
        task_id="init_optimism_logs_pipe",
        sql=optimism_queries.SQL_CREATE_OPTIMISM_RAW_LOGS_PIPE,
        params={"pipe_name": config.LOGS},
    )



    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> [init_optimism_blocks_pipe, init_optimism_transactions_pipe, init_optimism_traces_pipe, init_optimism_logs_pipe] >> end