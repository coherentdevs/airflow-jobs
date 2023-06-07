from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import polygon_queries, config

SNOWFLAKE_CONN_ID="snowflake_polygon"

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
        "fill-polygon-blocks",
        description="""
        rebackfill raw polygon blocks
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        schedule_interval=None,
        default_args=default_args,
) as dag:
    """
    #### backfill polygon blocks to 43650000 block
    """
    min = 0
    step = 10000
    block_loaders = []
    tables = config.BLOCKS_TABLE_NAME
    objects = config.BLOCKS
    for batch in range(0, 5):
        current_batch = []

        for n in range(0,873):
            start = min + (n * step)
            stop = start + step - 1
            stmt = polygon_queries.POLYGON_COPY_FMT.format(table_name=tables, object_type=objects, start=start, end=stop)
            task = SnowflakeOperator(
                task_id=f"fill_%s_%s_%s" % (start, stop, objects),
                sql=stmt,
            )
            current_batch.append(task)
        block_loaders.append(current_batch)
        min += 873 * step


    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> block_loaders[0]
    for i in range(len(block_loaders) - 1):
        block_loaders[i][-1] >> block_loaders[i + 1]
    block_loaders[-1][-1] >> end