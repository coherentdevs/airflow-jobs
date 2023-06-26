from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
from datetime import datetime, timedelta

import pinecone
import itertools
import logging
import os

SNOWFLAKE_CONN_ID = "snowflake_pinecone_upsert"
VECTORS_ENDPOINT = "/internal/vectors"
PINECONE_NAMESPACE = "user_vectors"
PINECODE_API_KEY = Variable.get("pinecone_api_key")

SEMANTIC_FETCH_BATCH_SIZE = 10000
PINECONE_UPSERT_BATCH_SIZE = 100
N = 1680  # the number of parallel tasks

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.now() - timedelta(days=1),
}


def chunks(iterable, batch_size=SEMANTIC_FETCH_BATCH_SIZE):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))

def get_vectors_and_upsert_fn(file_name):
    # Read addresses from file
    with open(file_name, 'r') as f:
        addresses = f.read().splitlines()

    pinecone.init("abfba852-761f-4099-8255-3fa563185154", environment='us-west4-gcp')
    upsert_tuples = []

    http = HttpHook(method='POST', http_conn_id='http_semantic_api')
    for i in range(0, len(addresses), SEMANTIC_FETCH_BATCH_SIZE):
        batch_addresses = addresses[i: i + SEMANTIC_FETCH_BATCH_SIZE]
        data = {
            "addresses": [address for address in batch_addresses]
        }
        endpoint = f"{VECTORS_ENDPOINT}"
        # pull vectors from semantic api
        logging.info("Pulling vectors from semantic api")
        get_vectors_response = http.run(endpoint=endpoint,
                                        headers={"Content-Type": "application/json", "x-api-key": "demo"},
                                        json=data)
        if get_vectors_response.status_code != 200:
            logging.error(f"Received status code {get_vectors_response.status_code} from Semantic API")
            logging.error(f"Error message: {get_vectors_response.text}")
            return

        vectors = get_vectors_response.json()
        for address, vector in vectors.items():
            upsert_tuples.append((address, vector))

    # upsert vectors to pinecone
    logging.info("upserting vectors to pinecone")

    # Upsert data with 100 vectors per upsert request asynchronously
    # - Create pinecone.Index with pool_threads=30 (limits to 30 simultaneous requests)
    # - Pass async_req=True to index.upsert()
    with pinecone.Index('addresses', pool_threads=30) as index:
        # Send requests in parallel
        async_results = [
            index.upsert(vectors=ids_vectors_chunk, namespace="testing_parallelization_v9", async_req=True)
            for ids_vectors_chunk in chunks(upsert_tuples, batch_size=PINECONE_UPSERT_BATCH_SIZE)
        ]
        # Wait for and retrieve responses (this raises in case of error)
        [async_result.get() for async_result in async_results]

    logging.info("Pinecone upsert finished")

with DAG(
        'eth_addresses_to_pinecone_v1',
        default_args=default_args,
        description='DAG to get ETH addresses from GCS and upsert them to Pinecone',
        schedule_interval=None,
) as dag:
    for i in range(N):
        upsert_vectors = PythonOperator(
            task_id=f'upsert_vectors_{i}',
            python_callable=get_vectors_and_upsert_fn,
            op_kwargs={
                'file_name': f'/home/airflow/gcs/dags/eth_addresses/addresses_{i}.txt',
            }
        )
