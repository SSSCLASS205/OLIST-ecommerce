import boto3
import os
from botocore.exceptions import ClientError


LOCAL_FOLDER_PATH = "/home/sssclass/Desktop/project/olist_project/dbt_project/dags"
BUCKET_NAME = 'REPLACE_ME'
S3_PREFIX = 'dags'


def upload(local_folder, files_list):
    client = boto3.client('s3')

    for file_name in files_list:
        local_file_path = os.path.join(local_folder, file_name)
        
        s3_key = f"{S3_PREFIX}/{file_name}"

        try:
            client.upload_file(local_file_path, BUCKET_NAME, s3_key)
            print(f"Successfully uploaded {file_name} to s3://{BUCKET_NAME}/{s3_key}")

        except ClientError as e:
            print(f"Failed to upload {file_name} to S3: {e}")


def main():
    print(f"Scanning {LOCAL_FOLDER_PATH} for DAGs...")
    
    # Safety check: ensure the folder actually exists
    if not os.path.exists(LOCAL_FOLDER_PATH):
        print(f"Error: Folder not found at {LOCAL_FOLDER_PATH}")
        return

    all_entries = os.listdir(LOCAL_FOLDER_PATH)
    
    # Filter to only grab files (ignores sub-folders like __pycache__)
    files = [f for f in all_entries if os.path.isfile(os.path.join(LOCAL_FOLDER_PATH, f))]

    if not files:
        print("⚠️ No files found in the DAGs folder.")
        return

    print(f"Found {len(files)} files. Starting upload...")
    upload(LOCAL_FOLDER_PATH, files)
    print("Upload complete.")


if __name__ == "__main__":
    main()