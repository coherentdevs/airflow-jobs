from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from common.pinecone import PineconeClient
from common import config

SNOWFLAKE_CONN_ID="snowflake_ethereum"
VECTORS_ENDPOINT="http://api.semantic-beta.coherent.xyz/vectors"
PINECONE_NAMESPACE="your_pinecone_namespace"
PINECODE_API_KEY=""

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

def upsert_to_pinecone(**context):
    pinecone = PineconeClient(PINECODE_API_KEY)
    vectors = context['task_instance'].xcom_pull(task_ids='get_vectors')
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

    get_vectors = SimpleHttpOperator(
        task_id='get_vectors',
        method='GET',
        endpoint=VECTORS_ENDPOINT,
        data=lambda ti: ti.xcom_pull(task_ids='fetch_addresses'),
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: response.json(),
    )

    upsert_to_pinecone = PythonOperator(
        task_id='upsert_to_pinecone',
        python_callable=upsert_to_pinecone,
        provide_context=True,
    )

    fetch_addresses >> get_vectors >> upsert_to_pinecone
