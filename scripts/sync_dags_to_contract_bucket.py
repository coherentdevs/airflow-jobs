import os
from google.cloud import storage

# Set your bucket name and local dags folder path
bucket_name = 'us-east1-contract-composer-2b295c93-bucket'
local_dags_folder = './dags'

storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

def sync(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            local_file_path = os.path.join(root, file)
            remote_file_path = 'contract-composer/' + os.path.relpath(local_file_path, local_dags_folder)
            blob = bucket.blob(remote_file_path)
            blob.upload_from_filename(local_file_path)

        for directory in dirs:
            sync(directory)

sync(local_dags_folder)