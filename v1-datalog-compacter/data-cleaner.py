import json
from pathlib import Path
import logging

from contextlib import contextmanager
import duckdb

# ======= CONFIGURATION =======
CREDENTIALS_PATH = "creds/creds.json"
with open(Path(CREDENTIALS_PATH), "r") as file:
    creds = json.load(file)
    cred = creds["AWS"]

keys_to_keep = ["aws_secret_access_key", "aws_access_key_id", "aws_region"]
aws_creds = {key: value for key, value in cred.items() if key in keys_to_keep}

print(aws_creds)


# ======= FUNCTION DECLARATION ======
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
        conn.execute("SET TimeZone = 'UTC';")
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


def get_s3_datalog():
    with init_duckdb_connection(aws_creds, "4GB") as conn:
        data = conn.sql(
            """
            SELECT dstrct_code, hiveperiod,heartbeat, CAST(
            CASE
                WHEN heartbeat < 10000000000 THEN make_timestamp(CAST(heartbeat * 1000000 as BIGINT) )
                WHEN heartbeat < 10000000000000 THEN make_timestamp(CAST(heartbeat * 1000 as BIGINT))
                WHEN heartbeat < 10000000000000000 THEN make_timestamp(CAST(heartbeat as BIGINT))
                ELSE make_timestamp(CAST(heartbeat / 1000as BIGINT))
            END + INTERVAL 8 HOURS
        AS DATE) as accurate_wita_date
            FROM read_parquet('s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania/**/*.parquet',hive_partitioning=true)
            """
        )

        df = conn.sql(
            """
            SELECT DISTINCT hiveperiod,dstrct_code,accurate_wita_date
            FROM data
            WHERE hiveperiod != accurate_wita_date
        """
        )

        df.pl().write_csv(Path("output.csv"))
        return None


def main():
    # 1. Make a list of keys to fix
    # 1.1 Iterate list_objects from boto3 on output.csv and get keys list
    # 2. Read data from those keys
    # 3. Repartition the data
    # 4. Delete the keys
    # 5. Validate the output
    get_s3_datalog()
    return None


if __name__ == "__main__":
    main()
