"""Idempotently initialize MinIO S3 buckets for CareFlow Atlas."""

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

BUCKETS = [
    "careflow-bronze",
    "careflow-silver",
    "careflow-gold",
    "careflow-quarantine",
]

def init_minio_buckets(
    endpoint_url: str = "http://localhost:9000",
    aws_access_key_id: str = "minioadmin",
    aws_secret_access_key: str = "miniopassword",
):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )

    existing_buckets = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for bucket in BUCKETS:
        if bucket not in existing_buckets:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f"[MinIO] Created bucket: {bucket}")
            except ClientError as e:
                print(f"[MinIO] Error creating {bucket}: {e}")
        else:
            print(f"[MinIO] Bucket exists: {bucket}")

if __name__ == "__main__":
    init_minio_buckets()
