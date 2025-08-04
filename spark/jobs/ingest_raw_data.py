import os
import time
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession

# Configuration for MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "sample-bucket")
SOURCE_DIR = os.getenv("SOURCE_DIR", "data/data/raw/")
DEST_PREFIX = os.getenv("DEST_PREFIX", "raw/")

MAX_RETRIES = 3
RETRY_DELAY = 5


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' already exists.")
    except ClientError:
        print(f"Bucket '{BUCKET_NAME}' not found. Attempting to create...")
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"✅ Created bucket '{BUCKET_NAME}'.")
        except Exception as ex:
            print(f"❌ Failed to create bucket '{BUCKET_NAME}': {ex}")
            raise


def upload_file(s3, local_path, object_key):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            print(f"📤 Uploading '{local_path}' to '{BUCKET_NAME}/{object_key}' (attempt {attempt + 1})...")
            s3.upload_file(local_path, BUCKET_NAME, object_key)
            print(f"✅ Successfully uploaded '{local_path}'")
            return
        except ClientError as e:
            print(f"⚠️ Upload failed: {e}")
            time.sleep(RETRY_DELAY)
            attempt += 1
    raise RuntimeError(f"❌ Failed to upload '{local_path}' after {MAX_RETRIES} attempts.")


def upload_all_files():
    s3 = get_s3_client()
    ensure_bucket(s3)

    if not os.path.isdir(SOURCE_DIR):
        raise FileNotFoundError(f"❌ Source directory '{SOURCE_DIR}' does not exist.")

    files = [
        os.path.join(SOURCE_DIR, f)
        for f in os.listdir(SOURCE_DIR)
        if os.path.isfile(os.path.join(SOURCE_DIR, f))
    ]

    if not files:
        print(f"📂 No files to upload in '{SOURCE_DIR}'.")
        return

    for file_path in files:
        filename = os.path.basename(file_path)
        s3_key = os.path.join(DEST_PREFIX.strip("/"), filename)
        upload_file(s3, file_path, s3_key)


if __name__ == "__main__":
    # Initialize SparkSession (preferred over SparkContext)
    spark = SparkSession.builder.appName("IngestRawData").getOrCreate()

    try:
        print("🚀 Starting ingestion of raw files to MinIO...")
        upload_all_files()
        print("✅ Ingestion complete.")
    except Exception as e:
        print("❌ Ingestion failed:", str(e))
        raise
    finally:
        spark.stop()
