import os
from google.cloud import storage_control_v2
from dotenv import load_dotenv

load_dotenv()

def create_folder(bucket_name: str, folder_name: str) -> None:
    storage_control_client = storage_control_v2.StorageControlClient()
    project_path = storage_control_client.common_project_path("_")
    bucket_path = f"{project_path}/buckets/{bucket_name}"

    request = storage_control_v2.CreateFolderRequest(
        parent=bucket_path,
        folder_id=folder_name,
    )
    response = storage_control_client.create_folder(request=request)

    print(f"Created folder: {response.name}")

bucket_name = os.getenv("GCS_BUCKET_NAME")
create_folder(bucket_name=bucket_name, folder_name="raw")
