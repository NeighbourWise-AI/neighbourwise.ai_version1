# dags/healthcare/copy_into_sf.py

import os
import snowflake.connector
from airflow.sdk.bases.hook import BaseHook
from healthcare.config import SNOWFLAKE_TABLE_FQN, AWS_CONN_ID

def copy_latest_csv_into_snowflake(**context):
    ti = context["task_instance"]

    s3_bucket = ti.xcom_pull(task_ids="get_latest_csv_key", key="s3_bucket")
    s3_key = ti.xcom_pull(task_ids="get_latest_csv_key", key="s3_key")

    s3_path = f"s3://{s3_bucket}/{s3_key}"
    print(f"Copying from: {s3_path}")

    aws_conn = BaseHook.get_connection(AWS_CONN_ID)
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"],
        insecure_mode=True,
    )

    # 🔥 Debug mode: return parsing/load errors instead of hiding them
    validate_sql = f"""
    COPY INTO {SNOWFLAKE_TABLE_FQN}
    FROM '{s3_path}'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_access_key}'
        AWS_SECRET_KEY = '{aws_secret_key}'
    )
    FILE_FORMAT = (
        TYPE = CSV
        FIELD_DELIMITER = ','
        SKIP_HEADER = 2
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        TRIM_SPACE = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        NULL_IF = ('NULL','null','')
        EMPTY_FIELD_AS_NULL = TRUE
    )
    VALIDATION_MODE = 'RETURN_ERRORS';
    """

    copy_sql = validate_sql.replace("VALIDATION_MODE = 'RETURN_ERRORS';", "ON_ERROR = 'ABORT_STATEMENT';")

    cur = conn.cursor()
    try:
        # 1) Show errors (this is the real fix)
        cur.execute(validate_sql)
        errors = cur.fetchall()
        if errors:
            print("❌ COPY VALIDATION ERRORS (first 20):")
            for row in errors[:20]:
                print(row)
            raise RuntimeError("COPY validation failed. Fix the CSV/file_format based on errors above.")

        # 2) If no validation errors, do real load
        cur.execute(copy_sql)
        copy_result = cur.fetchall()   # ✅ COPY INTO returns per-file results
        print("✅ COPY RESULT:")
        for r in copy_result:
            print(r)

        # 3) Confirm rows in table
        cur.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_TABLE_FQN}")
        row_count = cur.fetchone()[0]
        print(f"Row count after load: {row_count}")

        if row_count == 0:
            raise RuntimeError("COPY ran but loaded 0 rows. Check COPY RESULT printed above.")

        return {"loaded_from": s3_path, "row_count": row_count}

    finally:
        cur.close()
        conn.close()
