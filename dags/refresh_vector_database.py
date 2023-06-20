from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
from common.pinecone import PineconeClient
import logging
from common import config

SNOWFLAKE_CONN_ID="snowflake_ethereum_decoded"
VECTORS_ENDPOINT="/internal/vectors"
PINECONE_NAMESPACE="user_vectors"
PINECODE_API_KEY=Variable.get("pinecone_api_key")
BATCH_SIZE = 50

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
    sql="SELECT DISTINCT from_address FROM DECODED_TRANSACTIONS LIMIT 10000"
    logging.info("getting Ethereum addresses from snowflake")
    records = hook.get_records(sql)
    return records

def get_vectors_and_upsert_fn(addresses, pinecone):
    http = HttpHook(method='GET', http_conn_id='http_semantic_api')
    for i in range(0, len(addresses), BATCH_SIZE):
        batch_addresses = addresses[i: i + BATCH_SIZE]
        query_string = "&".join([f"addresses={address[0]}" for address in batch_addresses])
        endpoint = f"{VECTORS_ENDPOINT}?{query_string}"
        # pull vectors from semantic api
        logging.info("Pulling vectors from semantic api")
        get_vectors_response = http.run(endpoint=endpoint, headers={"Content-Type": "application/json", "x-api-key": "demo"})
        if get_vectors_response.status_code != 200:
            logging.error(f"Received status code {get_vectors_response.status_code} from Semantic API")
            logging.error(f"Error message: {get_vectors_response.text}")
            return

        vectors = get_vectors_response.json()
        # upsert vectors to pinecone
        logging.info("upserting vectors to pinecone")
        upsert_vectors_response = pinecone.upsert_vectors(vectors, PINECONE_NAMESPACE)
        if upsert_vectors_response.status_code != 200:
            logging.error(f"Received status code {upsert_vectors_response.status_code} from server")
            logging.error(f"Error message: {upsert_vectors_response.text}")





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

    pinecone = PineconeClient(PINECODE_API_KEY)

    get_vectors_and_upsert = PythonOperator(
        task_id='get_vectors_and_upsert',
        python_callable=get_vectors_and_upsert_fn,
        op_kwargs={'addresses': fetch_addresses.output, 'pinecone': pinecone},
    )

    fetch_addresses >> get_vectors_and_upsert