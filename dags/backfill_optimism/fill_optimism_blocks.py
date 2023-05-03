from airflow import DAG
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime.now() - timedelta(days=1),
    "snowflake_conn_id": SNOWFLAKE_CONN_ID
}

with DAG(
        "fill-optimism-blocks-v1",
        description="""
        rebackfill raw optimism blocks
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        schedule_interval=None,
        default_args=default_args,
) as dag:
    """
    #### backfill optimism blocks to 90,000,000 block
    """
    min = 0
    step = 10000
    iterations = 9000
    block_loaders = []
    tables = config.BLOCKS_TABLE_NAME
    objects = config.BLOCKS
    for batch in range(0, 5):
        current_batch = []

        for n in range(0,1800):
            start = min + (n * step)
            stop = start + step - 1
            stmt = optimism_queries.OPTIMISM_COPY_FMT.format(table_name=tables, object_type=objects, start=start, end=stop)
            task = SnowflakeOperator(
                task_id=f"fill_%s_%s_%s" % (start, stop, objects),
                sql=stmt,
            )
            current_batch.append(task)
        block_loaders.append(current_batch)
        min += 1800 * step


    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> block_loaders[0]
    for i in range(len(block_loaders) - 1):
        block_loaders[i][-1] >> block_loaders[i + 1]
    block_loaders[-1][-1] >> end