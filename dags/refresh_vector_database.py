from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta

import pinecone
import itertools
import logging
from common import config

SNOWFLAKE_CONN_ID = "snowflake_ethereum_decoded"
VECTORS_ENDPOINT = "/internal/vectors"
PINECONE_NAMESPACE = "user_vectors"
PINECODE_API_KEY = Variable.get("pinecone_api_key")
SEMANTIC_FETCH_BATCH_SIZE = 50
PINECONE_UPSERT_BATCH_SIZE = 100

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime.now() - timedelta(days=1),
}


def fetch_addresses_fn():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = "SELECT DISTINCT from_address FROM DECODED_TRANSACTIONS LIMIT 10000"
    logging.info("getting Ethereum addresses from snowflake")
    records = hook.get_records(sql)
    return records


def chunks(iterable, batch_size=SEMANTIC_FETCH_BATCH_SIZE):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def get_vectors_and_upsert_fn(addresses):
    # initialize pinecone index
    pinecone.init("abfba852-761f-4099-8255-3fa563185154", environment='us-west4-gcp')
    upsert_tuples = []

    http = HttpHook(method='GET', http_conn_id='http_semantic_api')
    for i in range(0, len(addresses), SEMANTIC_FETCH_BATCH_SIZE):
        batch_addresses = addresses[i: i + SEMANTIC_FETCH_BATCH_SIZE]
        query_string = "&".join([f"addresses={address[0]}" for address in batch_addresses])
        endpoint = f"{VECTORS_ENDPOINT}?{query_string}"
        # pull vectors from semantic api
        logging.info("Pulling vectors from semantic api")
        get_vectors_response = http.run(endpoint=endpoint,
                                        headers={"Content-Type": "application/json", "x-api-key": "demo"})
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
            index.upsert(vectors=ids_vectors_chunk, async_req=True)
            for ids_vectors_chunk in chunks(upsert_tuples, batch_size=SEMANTIC_FETCH_BATCH_SIZE)
        ]
        # Wait for and retrieve responses (this raises in case of error)
        [async_result.get() for async_result in async_results]

    logging.info("Pinecone upsert finished")


with DAG(
        "refresh-vector-database",
        description="""
        Fetch Word2Vec embeddings for Ethereum addresses and upsert to Pinecone.
    """,
        start_date=datetime(2022, 12, 1),
        schedule_interval=None,
        default_args=default_args,
) as dag:
    fetch_addresses = PythonOperator(
        task_id="fetch_addresses",
        python_callable=fetch_addresses_fn,
    )

    get_vectors_and_upsert = PythonOperator(
        task_id='get_vectors_and_upsert',
        python_callable=get_vectors_and_upsert_fn,
        op_kwargs={'addresses': fetch_addresses.output},
    )

    fetch_addresses >> get_vectors_and_upsert