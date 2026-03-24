# dags/healthcare/get_latest_csv_key.py

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from healthcare.config import (
    HEALTHCARE_S3_BUCKET,
    HEALTHCARE_CSV_PREFIX,
    HEALTHCARE_EXCLUDE_PREFIXES,
    AWS_CONN_ID,
)

def get_latest_csv_key(**context):
    ti = context["task_instance"]

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    session = hook.get_session(region_name="us-east-2")  # change if your bucket region differs
    s3 = session.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=HEALTHCARE_S3_BUCKET, Prefix=HEALTHCARE_CSV_PREFIX)

    latest_obj = None

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Must be a CSV file
            if not key.lower().endswith(".csv"):
                continue

            # Skip excluded prefixes
            # IMPORTANT: exclusions must match the FULL key prefix
            if any(key.startswith(p) for p in HEALTHCARE_EXCLUDE_PREFIXES):
                continue

            if (latest_obj is None) or (obj["LastModified"] > latest_obj["LastModified"]):
                latest_obj = obj

    if not latest_obj:
        raise ValueError(f"No CSV files found under s3://{HEALTHCARE_S3_BUCKET}/{HEALTHCARE_CSV_PREFIX}")

    latest_key = latest_obj["Key"]
    last_modified = latest_obj["LastModified"]

    print(f"Latest CSV found: s3://{HEALTHCARE_S3_BUCKET}/{latest_key} (LastModified={last_modified})")

    # ✅ Push BOTH naming styles (so downstream works no matter what)
    ti.xcom_push(key="s3_bucket", value=HEALTHCARE_S3_BUCKET)
    ti.xcom_push(key="s3_key", value=latest_key)

    ti.xcom_push(key="latest_csv_bucket", value=HEALTHCARE_S3_BUCKET)
    ti.xcom_push(key="latest_csv_key", value=latest_key)

    return latest_key
