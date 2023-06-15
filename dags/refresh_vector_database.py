from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from common.pinecone import PineconeClient
from common import config

SNOWFLAKE_CONN_ID="snowflake_ethereum"
VECTORS_ENDPOINT="http://api.semantic-beta.coherent.xyz/vectors"
PINECONE_NAMESPACE="user_vectors"
PINECODE_API_KEY=Variable.get("pinecode_api_key")
BATCH_SIZE = 1000

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

def upsert_to_pinecone(vectors, pinecone):
    pinecone.upsert_vectors(vectors, PINECONE_NAMESPACE)

with DAG(
        "fetch-ethereum-address-vectors-v1",
        description="""
        Fetch Word2Vec embeddings for Ethereum addresses and upsert to Pinecone.
    """,
        start_date=datetime(2022, 12, 1),
        schedule_interval=None,
        default_args=default_args,
) as dag:

    fetch_addresses = SnowflakeOperator(
        task_id="fetch_addresses",
        sql="SELECT DISTINCT from_address, to_address FROM ETHEREUM.DECODED.DECODED_TRANSACTIONS",
    )

    addresses = fetch_addresses.execute(context=None)
    pinecone = PineconeClient(PINECODE_API_KEY)
    for i in range(0, len(addresses), BATCH_SIZE):
        batch_addresses = addresses[i: i + BATCH_SIZE]

        get_vectors = SimpleHttpOperator(
            task_id=f'get_vectors_{i}',
            method='GET',
            endpoint=VECTORS_ENDPOINT,
            data=batch_addresses,
            headers={"Content-Type": "application/json"},
            response_filter=lambda response: response.json(),
        )

        upsert_to_pinecone = PythonOperator(
            task_id=f'upsert_to_pinecone_{i}',
            python_callable=upsert_to_pinecone,
            op_kwargs={'vectors': get_vectors.execute(context=None), 'pinecone': pinecone},
        )

        fetch_addresses >> get_vectors >> upsert_to_pinecone
