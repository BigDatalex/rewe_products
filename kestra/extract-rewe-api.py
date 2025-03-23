import time

from playwright.sync_api import sync_playwright
from google.cloud import storage
import datetime
import os
import json

# Constants
BASE_URL_TEMPLATE = "https://shop.rewe.de/api/products?objectsPerPage=250&page={page}&search=%2A&sorting=RELEVANCE_DESC&serviceTypes=DELIVERY&market=240557&debug=false&autocorrect=true"
BUCKET_NAME = "rewe_products"
TIMESTAMP = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
FOLDER_NAME = f"rewe_products/{TIMESTAMP}"
FILE_NAME_TEMPLATE = "rewe_api_response_page_{page}.json"


def create_folder_if_not_exists(folder_path):
    """Create a folder if it doesn't already exist."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created.")


def write_json(data, file_path):
    """Write JSON data to a file."""
    with open(file_path, "w") as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Data written to {file_path}.")


def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """Uploads a file to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name} in bucket {bucket_name}.")


def fetch_data():
    """Fetch data from the REWE API and save it to local files and GCS."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        # Fetch the first page to determine total pages
        response = page.goto(BASE_URL_TEMPLATE.format(page=1))
        if not response.ok:
            raise Exception(f"Failed to fetch data: {response.status}")

        data = json.loads(response.text())
        total_pages = data.get("pagination", {}).get("totalPages", 1)

        # Save and upload the first page
        file_name = FILE_NAME_TEMPLATE.format(page=1)
        file_path = os.path.join(FOLDER_NAME, file_name)
        write_json(data, file_path)
        upload_to_gcs(BUCKET_NAME, file_path, file_path)  # Reuse file_path for GCS destination

        # Fetch and process remaining pages
        for page_num in range(2, total_pages + 1):
            time.sleep(1)
            response = page.goto(BASE_URL_TEMPLATE.format(page=page_num))
            if not response.ok:
                print(f"Failed to fetch page {page_num}: {response.status}")
                continue

            data = json.loads(response.text())
            file_name = FILE_NAME_TEMPLATE.format(page=page_num)
            file_path = os.path.join(FOLDER_NAME, file_name)
            write_json(data, file_path)
            upload_to_gcs(BUCKET_NAME, file_path, file_path)  # Reuse file_path for GCS destination

        browser.close()


if __name__ == "__main__":
    create_folder_if_not_exists(FOLDER_NAME)
    fetch_data()