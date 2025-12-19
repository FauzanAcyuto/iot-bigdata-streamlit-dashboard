import json
import logging
from contextlib import contextmanager
from pathlib import Path

import boto3
import duckdb
import plotly.express as px
import streamlit as st

CREDENTIALS_PATH = "creds/creds.json"

with open(Path(CREDENTIALS_PATH), "r") as file:
    creds = json.load(file)
    aws_creds = creds["AWS"]
aws_creds
aws_creds.pop("aws_bucket")


@contextmanager
def init_duckdb_connection(aws_credentials: dict, ram_limit: str):
    logger = logging.getLogger(__name__)

    existing_keys = list(aws_credentials.keys())
    required_keys = ["aws_secret_access_key", "aws_access_key_id", "aws_region"]
    if not set(required_keys) <= set(existing_keys):
        logger.exception(
            f"AWS Credentials doesn't contain required keys {required_keys}"
        )
        raise

    try:
        logger.info("Initializing duckdb connection to S3")
        conn = duckdb.connect()
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute(f"SET memory_limit = '{ram_limit}'")
        conn.execute(f"SET s3_region = '{aws_credentials['aws_region']}';")
        conn.execute(
            f"SET s3_access_key_id = '{aws_credentials['aws_access_key_id']}';"
        )
        conn.execute(
            f"SET s3_secret_access_key = '{aws_credentials['aws_secret_access_key']}';"
        )

        yield conn
    finally:
        conn.close()


s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_creds["aws_access_key_id"],
    aws_secret_access_key=aws_creds["aws_secret_access_key"],
    region_name=aws_creds["aws_region"],
)
bucket = "smartdbucket"
obj_key = "datalog/cis_smartd_tbl_iot_scania"

response = s3.list_objects_v2(
    Bucket=bucket,
    Prefix=obj_key,
)

if "Contents" in response:
    for obj in response["Contents"]:
        print(obj["Key"])
else:
    print(f"No objects found in '{obj_key}'")

with init_duckdb_connection(aws_creds, "4GB") as conn:
    df = conn.sql("""
        SELECT *
        FROM read_parquet('s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania/**/*.parquet',hive_partitioning=true)
        WHERE hiveperiod BETWEEN '2025-12-11 00:00:00' AND '2025-12-20 00:00:00'
        """).pl()
df
