import json
import logging
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path

import duckdb

# ====== CONFIG ======
CREDENTIALS_PATH = "creds/creds.json"
with open(Path(CREDENTIALS_PATH), "r") as file:
    creds = json.load(file)
    aws_creds = creds["AWS"]

TARGET_BUCKET_PATH = "s3://smartdbucket/datalogparquet"
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


# def get_file_keys(bucket,prefix):
#     s3 = boto3.client('s3')
#     paginator = s3.get_paginator('list_objects_v2')

#     keys = []
#     for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
#         for obj in page.get('Contents', []):
#             keys.append(obj['Key'])

# return keys


def get_datalog_from_s3_per_hiveperiod(conn, s3key: str, targetpath: str):
    logger = logging.getLogger(__name__)

    logger.info(f"Grabbing datalog for device {s3key} from s3")

    data = conn.sql(f"""
        SELECT 
            *,
            filename AS source_file
        FROM read_json_auto('{s3key}', filename=true)
    """)

    logger.info("Got the main data from s3")

    row_count = data.count("*").fetchone()[0]

    if row_count == 0:
        logger.warning(f"No data found for {s3key}")
        return None

    logger.info("Writing file to disk as parquet")

    conn.execute(f"""
        COPY (SELECT *,CAST(to_timestamp(heartbeat) + INTERVAL 8 HOURS AS DATE) as hiveperiod FROM data)
        TO '{targetpath}/datalog' 
        (
            FORMAT parquet,
            COMPRESSION snappy,
            PARTITION_BY (hiveperiod, deviceid),
            APPEND
        )
    """)

    logger.info("Writing metadata to conversion log")

    conn.execute(f"""
        COPY (SELECT 
            deviceid,
            CAST(to_timestamp(heartbeat) AS DATE) AS utc_date,
            HOUR(to_timestamp(heartbeat)) AS utc_hour,
            COUNT(*) AS row_ct
        FROM data
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3)
        TO '{targetpath}/metadata' 
        (
            FORMAT parquet,
            COMPRESSION snappy,
            APPEND
        )
    """)

    logger.info("All done!")

    return None


# def update_conversion_log(conn,bucketname,prefix):
#     logger = logging.getLogger(__name__)
#     logger.info("test")


#     result = conn.sql("""
#     SELECT
#     FROM
#     """)


def main():
    logger = logging.getLogger()
    # List files that need to be processed
    with init_duckdb_connection(aws_creds) as conn:
        get_datalog_from_s3_per_hiveperiod(
            conn,
            "s3://smartdbucket/datalog/BRCG/SLS30I009/20251212*/20251212*.txt.gz",
            TARGET_BUCKET_PATH,
        )

    # get data and write into into partitions in one go

    ## Partition will be /distrik/hiveperiod/deviceid/someparquetfiles


if __name__ == "__main__":
    setup_logger(LOG_LEVEL, LOG_FILE_PATH, LOG_SIZE_MB, LOG_FILES_TO_KEEP)
    main()
