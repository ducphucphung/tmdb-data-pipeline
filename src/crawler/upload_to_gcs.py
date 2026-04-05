import os
import json
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from tmdb_client import TMDBClient

from google.cloud import storage

load_dotenv()

import os
from pathlib import Path
from google.cloud import storage


def upload_folder_to_gcs(bucket_name, local_root, gcs_root="raw", overwrite=False):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    local_root = Path(local_root)

    if not local_root.exists():
        raise FileNotFoundError(f"Local root folder does not exist: {local_root}")

    uploaded_count = 0

    for file_path in local_root.rglob("*.json"):
        if file_path.is_file():
            # Get relative path from local_root
            relative_path = file_path.relative_to(local_root)

            # Build GCS object path
            destination_blob_name = str(Path("raw") / relative_path).replace("\\", "/")

            blob = bucket.blob(destination_blob_name)

            try:
                if overwrite:
                    blob.upload_from_filename(str(file_path))
                else:
                    # Upload only if object does not already exist
                    blob.upload_from_filename(str(file_path), if_generation_match=0)

                print(f"Uploaded: {file_path} -> gs://{bucket_name}/{destination_blob_name}")
                uploaded_count += 1

            except Exception as e:
                print(f"Skipped/Failed: {file_path} -> {destination_blob_name}")
                print(f"Reason: {e}")   
                

    print(f"\nDone. Uploaded {uploaded_count} file(s).")

bucket_name = os.getenv("GCS_BUCKET_NAME")

upload_folder_to_gcs(
    bucket_name=bucket_name,
    local_root="data/raw",
    gcs_root="raw",
    overwrite=False
)



