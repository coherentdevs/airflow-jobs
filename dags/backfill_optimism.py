from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from common import optimism_queries, config

def execute_snowflake_tasks(min, iterations, **kwargs):
    step = 10000
    tables = [config.BLOCKS_TABLE_NAME, config.TRANSACTIONS_TABLE_NAME, config.TRACES_TABLE_NAME,
              config.LOGS_TABLE_NAME]
    objects = [config.BLOCKS, config.TRANSACTIONS, config.TRACES, config.LOGS]
    loaders = []

    for y in range(4):
        for n in range(0, iterations):
            start = min + (n * step)
            stop = start + step - 1
            stmt = optimism_queries.OPTIMISM_COPY_FMT.format(table_name=tables[y], object_type=objects[y], start=start, end=stop)
            task = SnowflakeOperator(
                task_id=f"fill_%s_%s_%s" % (n, step, objects[y]),
                sql=stmt,
            )
            loaders.append(task)

    return loaders

SNOWFLAKE_CONN_ID = "snowflake_optimism"

with DAG(
        "fill-optimism",
        description="""
        rebackfill raw optimism data
    """,
        doc_md=__doc__,
        start_date=datetime(2022, 12, 1),
        schedule=None,
        schedule_interval=None,
        default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
) as dag:

    def wrapper_func(*args, **kwargs):
        dag_run = kwargs['dag_run']
        min = dag_run.conf['min']
        iterations = dag_run.conf['iterations']
        return execute_snowflake_tasks(min, iterations, **kwargs)

    snowflake_tasks = PythonOperator(
        task_id="execute_snowflake_tasks",
        python_callable=wrapper_func,
        provide_context=True,
        dag=dag,
)

begin = EmptyOperator(task_id="begin")
end = EmptyOperator(task_id="end")

begin >> snowflake_tasks >> end
