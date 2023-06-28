from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

start = 10500
end = 12600
SNOWFLAKE_CONN_ID = 'snowflake_pinecone_upsert'  # Snowflake connection ID

default_args = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_eth_addresses_from_snowflake(partition_id):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = f"SELECT from_address FROM unique_eth_addresses_batch_10000 WHERE partition_id = {partition_id}"
    records = hook.get_records(sql)
    return records


def upload_to_gcs(bucket_name, directory, file_name, addresses):
    hook = GCSHook()
    os.chdir('/home/airflow/gcs/dags')
    # Make sure the directory exists
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(f'{directory}/{file_name}', 'w') as txtfile:
        for row in addresses:
            txtfile.write(row[0] + '\n')

    hook.upload(bucket_name, f'{directory}/{file_name}', f'{directory}/{file_name}')
    os.remove(f'{directory}/{file_name}')  # remove local file after upload


with DAG(
    'upload_unique_eth_addresses_gcs_p6',
    default_args=default_args,
    description='DAG to get ETH addresses from Snowflake and store them in GCS',
    schedule_interval=None,
) as dag:

    for i in range(start, end):
        fetch_addresses = PythonOperator(
            task_id=f'fetch_addresses_{i}',
            python_callable=get_eth_addresses_from_snowflake,
            op_kwargs={'partition_id': i}
        )

        upload_addresses = PythonOperator(
            task_id=f'upload_addresses_{i}',
            python_callable=upload_to_gcs,
            op_kwargs={
                'bucket_name': 'us-central1-composer-dev-c335d299-bucket',
                'directory': 'dags/eth_addresses',
                'file_name': f'addresses_{i}.txt',
                'addresses': fetch_addresses.output
            }
        )

        fetch_addresses >> upload_addresses
