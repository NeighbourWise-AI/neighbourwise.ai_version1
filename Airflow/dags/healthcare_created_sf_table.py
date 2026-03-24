# dags/healthcare/create_sf_table.py

import os
import snowflake.connector
from healthcare.config import SNOWFLAKE_TABLE_FQN

def create_snowflake_healthcare_table(**context):
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

    create_table_sql = f"""
    CREATE OR REPLACE TABLE {SNOWFLAKE_TABLE_FQN} (
        FACILITY_TYPE VARCHAR(200),
        DPH_FACILITY_ID VARCHAR(20),
        FACILITY_NAME VARCHAR(500),
        STREET VARCHAR(500),
        CITY_TOWN VARCHAR(200),
        ZIP_CODE VARCHAR(20),
        TELEPHONE VARCHAR(50),
        BED_COUNT VARCHAR(50),
        ADULT_DAY_HEALTH_CAPACITY VARCHAR(50),
        LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """

    cur = conn.cursor()
    try:
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
        print("Running in:", cur.fetchone())

        print("Executing DDL for:", SNOWFLAKE_TABLE_FQN)
        cur.execute(create_table_sql)

        return "Table ready"
    finally:
        cur.close()
        conn.close()
