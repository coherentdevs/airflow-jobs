from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import optimism_queries, config

SNOWFLAKE_CONN_ID="snowflake_optimism"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime.now() - timedelta(days=1),
    "snowflake_conn_id": SNOWFLAKE_CONN_ID
}

with DAG(
        "fill-optimism-v2",
        description="""
        rebackfill raw optimism
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        schedule_interval=None,
        default_args=default_args,
) as dag:
    """
    #### Optimism Block table creation
    """
    min = 0
    step = 10000
    iterations = 9000
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
            stmt = optimism_queries.OPTIMISM_COPY_FMT.format(table_name=tables[y], object_type=objects[y], start=start, end=stop)
            task = SnowflakeOperator(
                task_id=f"fill_%s_%s_%s" % (start, stop, objects[y]),
                sql=stmt,
            )
            loaders[y].append(task)


    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> block_loaders + tx_loaders + trace_loaders + log_loaders >> end