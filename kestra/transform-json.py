from google.cloud import storage
from datetime import datetime
import dlt
import json

BUCKET_NAME = "rewe_products"
FOLDER_PREFIX = "rewe_products/"  # Ensure the prefix ends with a slash

def list_directories_and_find_latest():
    """
    List directories (or objects) in a GCS bucket folder and find the latest created one.

    Returns:
        latest_blob (Blob): The latest created object in the folder.
    """
    # Initialize a GCS client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(BUCKET_NAME)

    # List blobs (objects) in the folder
    blobs = bucket.list_blobs(prefix=FOLDER_PREFIX)

    # Find the latest created blob
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
    """
    Read all files within a specific folder in a GCS bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        folder_path (str): Path to the folder.

    Returns:
        files_content (dict): A dictionary with file names as keys and their content as values.
    """
    # Initialize a GCS client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(BUCKET_NAME)

    # List blobs (objects) in the folder
    blobs = bucket.list_blobs(prefix=folder_path)

    # Read the contents of each file
    files_content = {}
    for blob in blobs:
        if not blob.name.endswith("/"):  # Skip subfolders
            file_content = blob.download_as_text()
            files_content[blob.name] = file_content
            print(f"Read file: {blob.name}")

    return files_content

# Add a timestamp to the raw data
def add_timestamp(data):
    current_timestamp = datetime.utcnow().isoformat()  # Get current UTC timestamp
    for row in data:
        row["load_timestamp"] = current_timestamp  # Add timestamp to each row
    return data

# Define the resource for rewe products data
@dlt.resource(name="rewe_products")   # <--- The name of the resource (will be used as the table name)
def rewe_products():
    # Step 1: Find the latest created blob
    latest_blob = list_directories_and_find_latest()

    if latest_blob:
        # Extract the folder path from the latest blob's name
        folder_path = "/".join(latest_blob.name.split("/")[:-1]) + "/"
        print(f"Folder path: {folder_path}")

        # Step 2: Read all files within the latest folder
        files_content = read_files_in_folder(folder_path)
    else:
        print("No latest folder found.")

    for file_name, content in files_content.items():
        print(f"\nFile: {file_name}")
        data = json.loads(content)
        products = data.get("_embedded").get("products")
        yield products # <--- yield data to manage memory


if __name__ == "__main__":
    # Define a dlt pipeline with automatic normalization
    pipeline = dlt.pipeline(
        pipeline_name="rewe_products",
        destination="bigquery",
        dataset_name="rewe_products_data",
    )

    # Run the pipeline with raw nested data
    info = pipeline.run(rewe_products, table_name="rewe_products", write_disposition="append")

    # Print the load summary
    print(info)