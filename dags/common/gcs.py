from google.cloud import storage

class GCSBucket:
    def __init__(self, bucket_name) -> None:
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(bucket_name)

    def list(self):
        results = self.bucket.list_blobs(max_results=10)
        return results