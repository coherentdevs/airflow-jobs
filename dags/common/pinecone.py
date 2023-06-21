import json
import requests
import logging
from . import config

class PineconeClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'Api-Key': 'abfba852-761f-4099-8255-3fa563185154',
        }
        self.base_url = 'https://addresses-656d21f.svc.us-west4-gcp.pinecone.io'

    def upsert_vectors(self, vectors, namespace):
        url = f"{self.base_url}/vectors/upsert"
        data = {
            'vectors': vectors,
            'namespace': namespace,
        }

        response = requests.post(url, headers=self.headers, data=json.dumps(data))
        return response

