from __future__ import annotations

import os
import json
import csv
from datetime import datetime
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
"""Create Snowflake table using direct connector."""
import snowflake.connector

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
}


def fetch_and_convert_to_csv(**context):
    """Fetch JSON from API, save locally, and convert to CSV.
    
    Expects Airflow Variables:
      - boston_api_crime_url: full API URL to fetch
    
    Returns the paths of saved files via XCom.
    """
    api_url = Variable.get("boston_api_crime_url")

    # Use task execution timestamp for unique filename
    ts = context.get("ts_nodash") or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    
    # Define local paths in airflow-project directory
    base_dir = "/opt/airflow"  # In Docker, this maps to your airflow-project
    json_path = f"{base_dir}/boston_crime_{ts}.json"
    csv_path = f"{base_dir}/boston_crime_{ts}.csv"

    # Fetch ALL data with pagination
    print(f"Fetching ALL data from {api_url} with pagination")
    
    all_records = []
    offset = 0
    limit = 10000
    
    while True:
        paginated_url = f"{api_url}&limit={limit}&offset={offset}"
        print(f"Fetching records {offset} to {offset + limit}...")
        
        resp = requests.get(paginated_url, timeout=120)
        resp.raise_for_status()
        
        data = resp.json()
        records = data.get('result', {}).get('records', [])
        
        if not records:
            print("No more records found")
            break
        
        all_records.extend(records)
        print(f"Fetched {len(records)} records. Total so far: {len(all_records)}")
        
        if len(records) < limit:
            print("Reached last page")
            break
        
        offset += limit
    
    print(f"Finished fetching. Total records: {len(all_records)}")
    
    if not all_records:
        raise ValueError("No records found in API response")
    
    # Save complete JSON
    print(f"Saving JSON to {json_path}")
    with open(json_path, 'w', encoding='utf-8') as fh:
        json.dump({"records": all_records}, fh)
    
    # Convert to CSV
    print(f"Converting {len(all_records)} records to CSV")
    fieldnames = all_records[0].keys()

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)
    
    print(f"CSV saved to {csv_path}")
    print(f"JSON and CSV files saved in airflow-project directory")

    # Push file paths to XCom for the next task
    context['task_instance'].xcom_push(key='csv_path', value=csv_path)
    context['task_instance'].xcom_push(key='json_path', value=json_path)
    context['task_instance'].xcom_push(key='timestamp', value=ts)

    return {"csv_path": csv_path, "json_path": json_path, "record_count": len(all_records)}

def upload_to_s3(**context):
    """Upload the CSV file to S3.
    
    Expects Airflow Variables:
      - boston_s3_bucket: target S3 bucket name
    Optional Airflow Variables:
      - boston_s3_key_prefix: key prefix in bucket (default: "crime-safety/")
      - boston_aws_conn_id: Airflow AWS connection id (default: "aws_default")
    """

    s3_bucket = Variable.get("boston_s3_bucket")
    s3_prefix = Variable.get("boston_s3_key_prefix", default_var="crime-safety/")
    aws_conn_id = Variable.get("boston_aws_conn_id", default_var="aws_default")

    # Get file path from previous task
    ti = context['task_instance']
    csv_path = ti.xcom_pull(task_ids='fetch_and_convert', key='csv_path')
    ts = ti.xcom_pull(task_ids='fetch_and_convert', key='timestamp')

    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at {csv_path}")
    
    # Upload to S3
    s3_key = f"{s3_prefix}boston_crime_{ts}.csv"
    print(f"Uploading {csv_path} to s3://{s3_bucket}/{s3_key}")

    hook = S3Hook(aws_conn_id=aws_conn_id)
    hook.load_file(
        filename=csv_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )

    print(f"Successfully uploaded to S3: s3://{s3_bucket}/{s3_key}")
    # Push S3 location to XCom for Snowflake task
    context['task_instance'].xcom_push(key='s3_key', value=s3_key)
    context['task_instance'].xcom_push(key='s3_bucket', value=s3_bucket)

    return s3_key

def create_snowflake_table(**context):

    # Get credentials from environment variables
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        role=os.environ['SNOWFLAKE_ROLE'],
        insecure_mode=True  # Bypass SSL verification
    )

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS NEIGHBOURWISE_DOMAINS.STAGE.STG_BOSTON_CRIME (
        _ID NUMBER,
        INCIDENT_NUMBER VARCHAR(50),
        OFFENSE_CODE VARCHAR(10),
        OFFENSE_CODE_GROUP VARCHAR(100),
        OFFENSE_DESCRIPTION VARCHAR(500),
        DISTRICT VARCHAR(10),
        REPORTING_AREA VARCHAR(10),
        SHOOTING VARCHAR(5),
        OCCURRED_ON_DATE TIMESTAMP,
        YEAR NUMBER,
        MONTH NUMBER,
        DAY_OF_WEEK VARCHAR(20),
        HOUR NUMBER,
        UCR_PART VARCHAR(50),
        STREET VARCHAR(200),
        LAT FLOAT,
        LONG FLOAT,
        LOCATION VARCHAR(500),
        LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """

    try:
        cursor = conn.cursor()
        print("Creating Snowflake table STG_BOSTON_CRIME")
        cursor.execute(create_table_sql)
        print("Table STG_BOSTON_CRIME created or already exists")
        return "Table ready"
    finally:
        cursor.close()
        conn.close()
    
def load_s3_to_snowflake(**context):
    """Load data from S3 to Snowflake using COPY INTO command."""
    
    ti = context['task_instance']
    s3_bucket = ti.xcom_pull(task_ids='upload_to_s3', key='s3_bucket')
    s3_key = ti.xcom_pull(task_ids='upload_to_s3', key='s3_key')

    # Get AWS credentials
    from airflow.hooks.base import BaseHook
    aws_conn = BaseHook.get_connection('aws_default')
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password

    # Construct S3 path
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        role=os.environ['SNOWFLAKE_ROLE'],
        insecure_mode=True
    )

    # Create Snowflake COPY INTO command
    copy_sql = f"""
    COPY INTO NEIGHBOURWISE_DOMAINS.STAGE.STG_BOSTON_CRIME
    FROM '{s3_path}'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_access_key}'
        AWS_SECRET_KEY = '{aws_secret_key}'
    )
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        TRIM_SPACE = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        NULL_IF = ('NULL', 'null', '')
    )
    ON_ERROR = 'CONTINUE'
    PURGE = FALSE;
    """

    try:
        cursor = conn.cursor()
        print(f"Loading data from {s3_path} to Snowflake")
        cursor.execute(copy_sql)
        
        # Get count
        cursor.execute("SELECT COUNT(*) FROM STG_BOSTON_CRIME")
        count = cursor.fetchone()[0]
        print(f"Total records in table: {count}")
        
        return {"status": "success", "records_loaded": count}
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id="boston_api_to_s3_to_snowflake",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 2, 19),
    schedule_interval='0 0 * * 0',
    catchup=False,
    tags=["boston", "s3", "api"],
) as dag:

    fetch_and_convert_task = PythonOperator(
        task_id="fetch_and_convert",
        python_callable=fetch_and_convert_to_csv,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    create_table_task = PythonOperator(
        task_id="create_snowflake_table",
        python_callable=create_snowflake_table,
        provide_context=True,
    )

    load_to_snowflake_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_s3_to_snowflake,
        provide_context=True,
    )

    fetch_and_convert_task >> upload_task >> create_table_task >> load_to_snowflake_task
