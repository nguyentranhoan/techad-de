import os
import tempfile
import pytest
from unittest import mock
from botocore.exceptions import ClientError
from moto import mock_s3
import boto3

import spark.jobs.ingest_raw_data as  ingest_raw_data # Replace with actual script name


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "testkey")
    monkeypatch.setenv("MINIO_SECRET_KEY", "testsecret")
    monkeypatch.setenv("MINIO_BUCKET", "test-bucket")
    monkeypatch.setenv("SOURCE_DIR", "/tmp/fake-dir")
    monkeypatch.setenv("DEST_PREFIX", "raw/")


@mock_s3
def test_get_s3_client(mock_env_vars):
    client = ingest_raw_data.get_s3_client()
    assert isinstance(client, boto3.client("s3").__class__)


@mock_s3
def test_ensure_bucket_creates_if_missing(mock_env_vars):
    client = ingest_raw_data.get_s3_client()
    # Ensure it creates the bucket if missing
    ingest_raw_data.ensure_bucket(client)
    response = client.list_buckets()
    assert any(b['Name'] == "test-bucket" for b in response["Buckets"])


@mock_s3
def test_upload_file_success(mock_env_vars):
    client = ingest_raw_data.get_s3_client()
    client.create_bucket(Bucket="test-bucket")

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(b"sample-data")
        tmp_path = tmp.name

    ingest_raw_data.upload_file(client, tmp_path, "raw/test.csv")

    response = client.list_objects_v2(Bucket="test-bucket", Prefix="raw/")
    assert any(obj["Key"] == "raw/test.csv" for obj in response.get("Contents", []))


@mock_s3
def test_upload_all_files(mock_env_vars, monkeypatch):
    client = ingest_raw_data.get_s3_client()
    client.create_bucket(Bucket="test-bucket")

    # Create temp dir with files
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "file1.csv")
        with open(file_path, "w") as f:
            f.write("a,b,c")

        monkeypatch.setenv("SOURCE_DIR", tmpdir)
        ingest_raw_data.upload_all_files()

        result = client.list_objects_v2(Bucket="test-bucket")
        keys = [obj["Key"] for obj in result.get("Contents", [])]
        assert "raw/file1.csv" in keys


@mock_s3
def test_upload_file_retries_on_failure(mock_env_vars):
    client = ingest_raw_data.get_s3_client()
    client.create_bucket(Bucket="test-bucket")

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(b"sample-data")
        tmp_path = tmp.name

    # Patch upload_file to fail first two times
    with mock.patch("boto3.client") as mock_boto:
        instance = mock_boto.return_value
        instance.upload_file.side_effect = [
            ClientError({"Error": {"Code": "500"}}, "Upload"),
            ClientError({"Error": {"Code": "500"}}, "Upload"),
            None,
        ]

        ingest_raw_data.upload_file(instance, tmp_path, "raw/failure.csv")
        assert instance.upload_file.call_count == 3
