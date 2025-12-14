import json
import logging
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path

import boto3
import duckdb

# ====== CONFIG ======
CREDENTIALS_PATH = "creds/creds.json"
with open(Path(CREDENTIALS_PATH), "r") as file:
    creds = json.load(file)
    aws_creds = creds["AWS"]

TARGET_BUCKET_PATH = "s3://smartdbucket/datalogparquet"
BUCKET_NAME = "smartdbucket"
SOURCE_KEY_GLOB = "s3://smartdbucket/datalog"

# logging parameters
LOG_LEVEL = logging.INFO
LOG_FILE_PATH = "logs/gzip_to_parquet.log"
LOG_SIZE_MB = 1
LOG_FILES_TO_KEEP = 5


# ====== FUNCTION DECLARATION ======
def setup_logger(loglevel, log_file, logsize, files_to_keep):
    """
    loglevel(obj) = log level object (logging.INFO)
    log_file(str) = path to log file (../log/etl.log)
    logsize(int) = size of log files before rotated (in MB)
    files_to_keep = number of rotated files to keep
    """
    # Create log directory
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Setup logging
    logging.basicConfig(
        handlers=[
            RotatingFileHandler(
                log_file,
                maxBytes=logsize * 1024 * 1024,
                backupCount=files_to_keep,
            ),
            logging.StreamHandler(),
        ],
        level=loglevel,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,  # Override any existing config
    )

    logger = logging.getLogger()
    logger.info(f"Logging initialized → {log_file}")
    return logger


@contextmanager
def init_duckdb_connection(aws_credentials):
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


def get_all_keys_in_district(aws_credentials, bucket, distrik, date):
    logger = logging.getLogger(__name__)

    existing_keys = list(aws_credentials.keys())
    required_keys = ["aws_secret_access_key", "aws_access_key_id", "aws_region"]
    if not set(required_keys) <= set(existing_keys):
        logger.exception(
            f"AWS Credentials doesn't contain required keys {required_keys}"
        )
        raise

    logger.info("Getting deviceids")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials["aws_access_key_id"],
        aws_secret_access_key=aws_credentials["aws_secret_access_key"],
        region_name=aws_credentials["aws_region"],
    )
    paginator = s3.get_paginator("list_objects_v2")

    deviceids = []
    for page in paginator.paginate(
        Bucket=bucket, Prefix=f"datalog/{distrik}/", Delimiter="/"
    ):
        for prefix in page.get("CommonPrefixes", []):
            deviceids.append(prefix["Prefix"])

    logger.info(f"{len(deviceids)} deviceids obtained for district {distrik}")

    keys = []
    # r = list(range(0, 24))
    # datelist = [f"{date}{i:02d}" for i in r]

    for device_prefix in deviceids:
        print("device_prefix: ", device_prefix)
        date_prefix = f"{device_prefix}{date}"  # e.g., "jobsite1/device1/20240101"
        print("date_prefix: ", date_prefix)
        for page in paginator.paginate(Bucket=bucket, Prefix=date_prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

    logger.info(f"{len(keys)} keys obtained for {distrik} on day {date}")

    return keys, deviceids


def get_datalog_from_s3_per_hiveperiod(conn, s3key_list: list, targetpath: str):
    logger = logging.getLogger(__name__)

    logger.info(f"Grabbing datalog for device {s3key_list} from s3")

    s3key_list_string = "['" + "', '".join(s3key_list) + "']"
    data = conn.sql(f"""
        SELECT 
            *,
            filename AS source_file
        FROM read_json_auto('{s3key_list_string}', filename=true)
    """)

    logger.info("Got the main data from s3")

    row_count = data.count("*").fetchone()[0]

    if row_count == 0:
        logger.warning(f"No data found for {s3key_list_string}")

        return None

    logger.info("Writing file to disk as parquet")

    conn.execute(f"""
        COPY (SELECT *,CAST(to_timestamp(heartbeat) + INTERVAL 8 HOURS AS DATE) as hiveperiod FROM data)
        TO '{targetpath}/datalog' 
        (
            FORMAT parquet,
            COMPRESSION snappy
        )
    """)

    logger.info("Writing metadata to conversion log")

    conn.execute(f"""
        COPY (SELECT 
            filename,
            'SUCCESS' as conversion_status
        FROM data
        )
        TO '{targetpath}/metadata' 
        (
            FORMAT parquet,
            COMPRESSION snappy,
            APPEND
        )
    """)

    logger.info("All done!")

    return None


def datalog_compacter(conn, targetpath: str):
    logger = logging.getLogger(__name__)

    logger.info("Initializing data compacter")

    logger.info("Compacted data ")


def main():
    logger = logging.getLogger()
    # List files that need to be processed
    keys, devices = get_all_keys_in_district(aws_creds, BUCKET_NAME, "BRCG", "20251212")

    with open(Path("data/keys.csv"), "a") as file:
        for row in keys:
            row = f"{row}\n"
            file.write(row)

    with init_duckdb_connection(aws_creds) as conn:
        get_datalog_from_s3_per_hiveperiod(
            conn,
            keys,
            "data",
            # TARGET_BUCKET_PATH,
        )

    logger.info("All Done!")
    # get data and write into into partitions in one go

    ## Partition will be /distrik/hiveperiod/deviceid/someparquetfiles


if __name__ == "__main__":
    setup_logger(LOG_LEVEL, LOG_FILE_PATH, LOG_SIZE_MB, LOG_FILES_TO_KEEP)
    main()
