from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import queries, config

SNOWFLAKE_CONN_ID="snowflake_ethereum"
MAX_COUNT=100

with DAG(
        "fill-ethereum",
        description="""
        rebackfill raw ethereum
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
) as dag:
    """
    #### Ethereum Block table creation
    """
    min = 16000000
    step = 10000
    iterations = 90
    block_loaders = []
    tx_loaders = []
    trace_loaders = []
    log_loaders = []
    tables = [config.BLOCKS_TABLE_NAME, config.TRANSACTIONS_TABLE_NAME, config.TRACES_TABLE_NAME, config.LOGS_TABLE_NAME]
    objects = [config.BLOCKS, config.TRANSACTIONS, config.TRACES, config.LOGS]
    loaders = [block_loaders, tx_loaders, trace_loaders, log_loaders]

    for y in range(4):
        for n in range(0,iterations):
            start = min + (n * step)
            stop = start + step - 1
            stmt = queries.ETH_COPY_FMT.format(table_name=tables[y], object_type=objects[y], start=start, end=stop)
            task = SnowflakeOperator(
                task_id=f"fill_%s_%s_%s" % (n, step, objects[y]),
                sql=stmt,
            )
            loaders[y].append(task)


    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    # init_tables = [
    #     init_blocks_table,
    #     init_transactions_table,
    #     init_traces_table,
    #     init_logs_table,
    # ]

    # begin >> init_tables

    # init_blocks_table >> block_loaders
    # init_transactions_table >> tx_loaders
    # init_traces_table >> trace_loaders
    # init_logs_table >> log_loaders

    begin >> block_loaders + tx_loaders + trace_loaders + log_loaders >> end