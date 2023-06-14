import json
import requests
from . import config

class PineconeClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'Api-Key': self.api_key,
        }
        self.base_url = 'https://addresses-656d21f.svc.us-west4-gcp.pinecone.io'

    def upsert_vectors(self, vectors, namespace):
        url = f"{self.base_url}/vectors/upsert"
        data = {
            'vectors': vectors,
            'namespace': namespace,
        }
        response = requests.post(url, headers=self.headers, data=json.dumps(data))
        return response.json()

    def query_vectors(self, vector, topK, includeMetadata, includeValues, namespace):
        url = f"{self.base_url}/query"
        data = {
            'vector': vector,
            'topK': topK,
            'includeMetadata': includeMetadata,
            'includeValues': includeValues,
            'namespace': namespace,
        }
        response = requests.post(url, headers=self.headers, data=json.dumps(data))
        return response.json()


pinecone = PineconeClient(config.config['production'].PINECONE_API_KEY)
