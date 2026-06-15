from google.cloud import storage
from datetime import datetime
import dlt
import json

BUCKET_NAME = "rewe_products_bucket"
FOLDER_PREFIX = "rewe_products/"


def list_directories_and_find_latest():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=FOLDER_PREFIX)

    latest_blob = None
    latest_time = datetime.min.replace(tzinfo=None)

    for blob in blobs:
        if blob.time_created.replace(tzinfo=None) > latest_time:
            latest_time = blob.time_created.replace(tzinfo=None)
            latest_blob = blob

    if latest_blob:
        print(f"Latest created object: {latest_blob.name} (Created at: {latest_time})")
    else:
        print("No objects found in the folder.")

    return latest_blob


def read_files_in_folder(folder_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=folder_path)

    files_content = {}
    for blob in blobs:
        if not blob.name.endswith("/"):
            file_content = blob.download_as_text()
            files_content[blob.name] = file_content
            print(f"Read file: {blob.name}")

    return files_content


@dlt.resource(name="rewe_products")
def rewe_products():
    latest_blob = list_directories_and_find_latest()

    if latest_blob:
        folder_path = "/".join(latest_blob.name.split("/")[:-1]) + "/"
        print(f"Folder path: {folder_path}")
        files_content = read_files_in_folder(folder_path)
    else:
        print("No latest folder found.")

    for file_name, content in files_content.items():
        print(f"\nFile: {file_name}")
        data = json.loads(content)
        products = data.get("_embedded").get("products")
        yield products


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="rewe_products",
        destination="bigquery",
        dataset_name="rewe_products_data",
    )
    info = pipeline.run(rewe_products, table_name="rewe_products", write_disposition="append")
    print(info)
