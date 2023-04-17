import os
from google.cloud import storage

# Set your bucket name and local dags folder path
bucket_name = 'us-central1-composer-dev-c335d299-bucket'
local_dags_folder = '../dags'

def sync_dags():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    for root, _, files in os.walk(local_dags_folder):
        for file in files:
            if file.endswith('.py'):
                local_file_path = os.path.join(root, file)
                remote_file_path = os.path.relpath(local_file_path, local_dags_folder)
                blob = bucket.blob(remote_file_path)
                blob.upload_from_filename(local_file_path)

if __name__ == '__main__':
    sync_dags()