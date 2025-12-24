import json
import logging
from contextlib import contextmanager
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import sleep

import boto3
import duckdb
from sqlalchemy import URL, create_engine, text

# ====== USER INPUT ======
DISTRIK = input("What district should we work on? ").upper()
if DISTRIK not in ("BRCB", "BRCG"):
    raise Exception("Distrik not in BRCB or BRCG!")

run_type = input("STEADY or BOOST mode? ").upper()
if run_type == 'BOOST':
    SLEEP_DURATION = 0
else:
    SLEEP_DURATION = 3600


# ====== CONFIG ======
CREDENTIALS_PATH = "creds/creds.json"
with open(Path(CREDENTIALS_PATH), "r") as file:
    creds = json.load(file)
    aws_creds = creds["AWS"]

TARGET_BUCKET_PATH = "s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania"
# TARGET_BUCKET_PATH = "data"
BUCKET_NAME = "smartdbucket"
SOURCE_KEY_GLOB = "s3://smartdbucket/datalog"
RAM_LIMIT = "10GB"
KEY_LIMIT_PER_RUN = 2000

# logging parameters
LOG_LEVEL = logging.INFO
LOG_FILE_PATH = f"logs/gzip_to_parquet_{DISTRIK}.log"
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


def generate_sql_engine(driver, creds_path, creds_name, **additional_params):
    """
    Generate SQLAlchemy engine from credentials file.

    Args:
        driver: SQLAlchemy driver name (e.g., 'mssql+pyodbc')
        creds_path: Path to credentials JSON file
        creds_name: Key name in credentials JSON
        **additional_params: Additional URL parameters

    Returns:
        SQLAlchemy engine

    Raises:
        FileNotFoundError: If credentials file not found
        KeyError: If credentials key not found in JSON
    """
    # get db creds
    logger = logging.getLogger(__name__)

    creds_path = Path(creds_path)
    # Validate credentials file
    if not creds_path.exists():
        logger.error(f"Credentials file not found: {creds_path}")
        raise FileNotFoundError(f"Credentials file not found: {creds_path}")

    try:
        # Load credentials
        with open(creds_path, "r") as file:
            all_creds = json.load(file)

        if creds_name not in all_creds:
            available = ", ".join(all_creds.keys())
            logger.error(f"Database '{creds_name}' not found in credentials")
            raise KeyError(f"Database '{creds_name}' not found. Available: {available}")
        creds = all_creds[creds_name]
        logger.info(f"Loaded credentials for: {creds_name}")

        # Create engine
        url_obj = URL.create(drivername=driver, **creds, **additional_params)
        engine = create_engine(
            url_obj,
            pool_pre_ping=True,  # Test connections before using
            # pool_size=5,
            # max_overflow=10,
        )

        # Test connection
        logger.info("Testing database connection...")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successful")

        return engine

    except Exception as e:
        logger.exception(f"Failed to create SQL engine {e}")
        raise


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


def get_all_keys_in_district(aws_credentials, bucket, distrik, date):
    logger = logging.getLogger(__name__)

    existing_keys = list(aws_credentials.keys())
    required_keys = ["aws_secret_access_key", "aws_access_key_id", "aws_region"]
    if not set(required_keys) <= set(existing_keys):
        logger.exception(
            f"AWS Credentials doesn't contain required keys {required_keys}"
        )
        raise Exception

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
        date_prefix = f"{device_prefix}{date}"  # e.g., "jobsite1/device1/20240101"
        for page in paginator.paginate(Bucket=bucket, Prefix=date_prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

    logger.info(f"{len(keys)} keys obtained for {distrik} on day {date}")

    return keys, deviceids


def get_pending_keys_sql(engine, distrik, file_limit=1000):
    logger = logging.getLogger(__name__)
    logger.info(f"Getting log files S3 keys to compress with limit: {file_limit}")

    if distrik == "BRCB":
        query = text(f"""SELECT TOP {file_limit} file_path_s3 
                     FROM tbl_t_upload_datalog 
                     WHERE is_upload_s3 = 'true'
                        AND distrik = 'BRCB'
                        AND file_path_lokal != 'Minio'
                        AND (compression_status != 'SUCCESS'  OR compression_status IS NULL)
                        AND upload_s3_date >= '2025-12-01 00:00'
                     ORDER BY upload_s3_date DESC
                     """)
    elif distrik == "BRCG":
        query = text(f"""SELECT TOP {file_limit} file_name
                        FROM tbl_t_upload_s3_log
                        WHERE distrik = 'BRCG'
                            AND (compression_status IS NULL OR compression_status != 'SUCCESS')
                            AND status = 'OK'
                            AND upload_date >= '2025-12-01 00:00'
                        ORDER BY upload_date DESC
                    """)
    else:
        logger.exception("District variable not in 'BRCB' OR 'BRCG'")
        raise Exception

    with engine.connect() as conn:
        result = conn.execute(query)
        list_of_keys = [row[0] for row in result]

    row_count = len(list_of_keys)

    if row_count == 0:
        logger.info("No more pending data to process!")
        return []

    logger.info(f"Got {row_count} of keys to work on.")
    return list_of_keys


def get_datalog_from_s3_per_hiveperiod(
    conn, bucket_name: str, s3key_list: list, targetpath: str, distrik: str
):
    logger = logging.getLogger(__name__)

    logger.info("Grabbing datalog for device all from s3")

    s3key_list_string = (
        f"['s3://{bucket_name}/" + f"', 's3://{bucket_name}/".join(s3key_list) + "']"
    )
    print(s3key_list_string[:100])
    data = conn.sql(f"""
        SELECT
            *,
            '{distrik}' AS dstrct_code,
            CAST(to_timestamp(heartbeat) + INTERVAL 8 HOURS AS DATE) as hiveperiod,
            filename AS source_file
        FROM read_json_auto({s3key_list_string}, filename=true, sample_size=-1, union_by_name=true)
    """)

    logger.info("Got the main data from s3")

    row_count = data.count("*").fetchone()[0]

    if row_count == 0:
        logger.warning(f"No data found for {s3key_list_string}")

        return None

    logger.info(f"Writing parquet file to target with {row_count} rows")

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
    main_query = f"""
        COPY (SELECT * FROM data)
        TO '{targetpath}/datalog' 
        (
            FORMAT parquet,
            COMPRESSION snappy,
            PARTITION_BY (hiveperiod,dstrct_code),
            APPEND
        )
    """
    try:
        conn.execute(main_query)
    except Exception:
        logger.exception("Main compacter query failed!")
        raise

    logger.info("Writing metadata to conversion log")

    # metadata_query = f"""
    #     COPY (SELECT 
    #         filename,
    #         COUNT(1),
    #         'SUCCESS' as conversion_status
    #     FROM data
    #     GROUP BY filename
    #     )
    #     TO '{targetpath}/metadata' 
    #     (
    #         FORMAT parquet,
    #         COMPRESSION snappy,
    #         APPEND
    #     )
    # """
    # try:
    #     conn.execute(metadata_query)
    # except Exception:
    #     logger.exception("Metadata query failed!")

    logger.info("All done!")

    return None


def update_compression_status_in_db(engine, keys: list, distrik: str):
    logger = logging.getLogger(__name__)
    row_num = len(keys)
    logger.info(f"Updating success status for {row_num} keys")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    key_list_string = "','".join(keys)

    if distrik == "BRCB":
        query = text(f"""UPDATE tbl_t_upload_datalog
                        SET compression_status = 'SUCCESS', compression_timestamp = '{now}'
                        WHERE file_path_s3 IN ('{key_list_string}')
                     """)

    elif distrik == "BRCG":
        query = text(f"""UPDATE tbl_t_upload_s3_log
                        SET compression_status = 'SUCCESS', compression_timestamp = '{now}'
                        WHERE file_name IN ('{key_list_string}') and status = 'OK'
                     """)

    else:
        logger.exception("District variable not in 'BRCB' OR 'BRCG'")
        raise Exception

    with engine.connect() as conn:
        result = conn.execute(query)
        conn.commit()

    return result


def datalog_compacter(conn, targetpath: str):
    logger = logging.getLogger(__name__)

    logger.info("Initializing data compacter")

    logger.info("Compacted data ")


def main():
    logger = logging.getLogger()

    logger.info(f'RAM LIMIT: {RAM_LIMIT}')
    # List files that need to be processed
    # keys, devices = get_all_keys_in_district(
    # aws_creds, BUCKET_NAME, DISTRIK, HIVEPERIOD
    # )
    engine = generate_sql_engine("mssql+pyodbc", CREDENTIALS_PATH, "pama-jiepsqco403")
    keys = get_pending_keys_sql(engine, DISTRIK, KEY_LIMIT_PER_RUN)

    if len(keys)==0:
        return None

    with init_duckdb_connection(aws_creds, RAM_LIMIT) as conn:
        get_datalog_from_s3_per_hiveperiod(
            conn, BUCKET_NAME, keys, TARGET_BUCKET_PATH, DISTRIK
        )
    result = update_compression_status_in_db(engine, keys, DISTRIK)

    print(result)
    logger.info("All Done!")
    # get data and write into into partitions in one go

    ## Partition will be /distrik/hiveperiod/deviceid/someparquetfiles


if __name__ == "__main__":
    setup_logger(LOG_LEVEL, LOG_FILE_PATH, LOG_SIZE_MB, LOG_FILES_TO_KEEP)
    while True:
        main()
        sleep(SLEEP_DURATION)

