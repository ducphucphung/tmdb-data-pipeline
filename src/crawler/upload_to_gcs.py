import os
from pathlib import Path
from datetime import date

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()


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
            destination_blob_name = str(Path(gcs_root) / relative_path).replace("\\", "/")

            blob = bucket.blob(destination_blob_name)

            try:
                if overwrite:
                    blob.upload_from_filename(str(file_path))
                else:
                    # Upload only if object does not already exist
                    blob.upload_from_filename(str(file_path), if_generation_match=0)

                print(f"Uploaded: {file_path} -> gs://{bucket_name}/{destination_blob_name}")
                uploaded_count += 1

            except Exception:
                pass
                

    print(f"\nDone. Uploaded {uploaded_count} file(s).")

bucket_name = os.getenv("GCS_BUCKET_NAME")
snapshot_date = os.getenv("SNAPSHOT_DATE", date.today().isoformat())

upload_folder_to_gcs(
    bucket_name=bucket_name,
    local_root=f"data/raw/movie_details/snapshot_date={snapshot_date}",
    gcs_root=f"raw/movie_details/snapshot_date={snapshot_date}",
    overwrite=False
)


