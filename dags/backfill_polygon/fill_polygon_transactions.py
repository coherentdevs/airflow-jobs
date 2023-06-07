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
        "fill-polygon-transactions",
        description="""
        rebackfill raw polygon transactions
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        schedule_interval=None,
        default_args=default_args,
) as dag:
    """
    #### backfill polygon transactions to 43650000 block
    """
    min = 0
    step = 10000
    transaction_loaders = []
    tables = config.TRANSACTIONS_TABLE_NAME
    objects = config.TRANSACTIONS
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
        transaction_loaders.append(current_batch)
        min += 873 * step


    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    begin >> transaction_loaders[0]
    for i in range(len(transaction_loaders) - 1):
        transaction_loaders[i][-1] >> transaction_loaders[i + 1]
    transaction_loaders[-1][-1] >> end