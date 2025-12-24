import json
import logging
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import streamlit as st
import polars as pl


CREDENTIALS_PATH = "creds/creds.json"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIMEZONE = "Asia/Singapore"  # gmt + 8

with open(Path(CREDENTIALS_PATH), "r") as file:
    creds = json.load(file)
    aws_creds = creds["AWS"]
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


@st.cache_data
def get_unit_list(hiveperiod: str, district: str):
    with init_duckdb_connection(aws_creds, "4GB") as conn:
        query = f"""
            SELECT DISTINCT dstrct_code,unitno,deviceid
            FROM read_parquet('s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania/**/*.parquet',hive_partitioning=true)
            WHERE hiveperiod = '{hiveperiod}'
                AND dstrct_code = '{district}'
            """
        # st.text(query)

        df = conn.sql(query).pl()
        st.text(f"found {len(df)} of unitno")

    return df


@st.cache_data
def get_s3_datalog(hiveperiod: str, district: str, unitno: list, hour: tuple):
    if isinstance(unitno, list):
        unitno = "', '".join(unitno)

    st.text(unitno)

    with init_duckdb_connection(aws_creds, "4GB") as conn:
        df = (
            conn.sql(
                f"""
            SELECT to_timestamp(heartbeat) as datetime,heartbeat,dstrct_code,hiveperiod,unitno,camcabinstatus,camfrontstatus,gpsspeed,gpsnumsat,VehicleSpeed,speedsource,gpslat,gpslong
            FROM read_parquet('s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania/**/*.parquet',hive_partitioning=true)
            WHERE hiveperiod = '{hiveperiod}'
                AND dstrct_code = '{district}'
                AND unitno IN ('{unitno}')
                AND DATE_PART('hour', to_timestamp(heartbeat)+ INTERVAL 8 HOUR) BETWEEN {hour[0]} AND {hour[1]}
            """
            )
            .pl()
            .with_columns(
                pl.col("datetime").dt.replace_time_zone("UTC"),
            )
            .with_columns(
                (pl.col("datetime") + pl.duration(hours=8)).alias("datetime_wita"),
            )
        )

    return df


# ====== LAYOUT ======
st.title("Smartd MH02 Business Intelligence")
tab_deviation, tab_speed = st.tabs(["Deviation Analysis", "Speed Analysis"])

# ====== INIT SESSION STATE ======
if "filter_button_pressed" not in st.session_state:
    st.session_state.filter_button_pressed = False

if "data_successfully_loaded" not in st.session_state:
    st.session_state.data_successfully_loaded = False

# ====== MAIN ======

# Default values
wita_today = datetime.now(ZoneInfo(TIMEZONE))
districts = ["BRCB", "BRCG"]
unit_list = ["LD772", "LD782", "LD781", "PM1582", "PM1598", "PM189"]

# Filter definition
hiveperiod = st.sidebar.date_input("Hiveperiod: ", None)  # single date
district = st.sidebar.selectbox("District: ", districts)  # single value
unitno = st.sidebar.selectbox("Select Unitno: ", unit_list)
hour = st.sidebar.slider("Hour (WITA): ", 1, 24, (1, 24))  # tuple

st.session_state.filter_button_pressed = st.sidebar.button("Apply Filter!")

if st.session_state.filter_button_pressed:
    if not st.session_state.data_successfully_loaded:
        data_load_state = st.text(f"Loading data for {unitno} on {hiveperiod}")

    try:
        dataframe = None
        dataframe = get_s3_datalog(hiveperiod, district, unitno, hour)
    except Exception as e:
        st.text(f"Exception occured {e}")

    if dataframe is not None and len(dataframe) > 0:
        st.session_state.data_successfully_loaded = False
        base_data = (
            dataframe.with_columns(
                pl.col("gpsspeed").replace(-9999, -1),
                pl.col("gpsnumsat").replace(-9999, -1),
                pl.col("VehicleSpeed").replace(-9999, -1),
                pl.when(pl.col("gpslat") < -8880)
                .then(pl.lit("false"))
                .otherwise(pl.lit("true"))
                .alias("gpsstatus"),
                pl.lit(1).alias("constant"),
            )
            .with_columns(
                (pl.col("gpsspeed") - pl.col("VehicleSpeed")).abs().alias("error_rate")
            )
            .sort("datetime_wita")
            .group_by_dynamic(
                "datetime_wita", by=["unitno", "dstrct_code", "hiveperiod"], every="1m"
            )
            .agg(
                pl.col("gpsspeed").mean(),
                pl.col("VehicleSpeed").mean(),
                pl.col("error_rate").mean(),
                pl.col("gpsnumsat").mean(),
                pl.col("gpsstatus").min(),
                pl.col("camfrontstatus").min(),
                pl.col("camcabinstatus").min(),
                pl.col("constant").mean(),
                pl.col("speedsource").min(),
            )
        )

        with tab_deviation:
            st.dataframe(dataframe)
            st.text(f"Obtained data with {len(dataframe)} rows")
            df_data = (
                base_data.select(
                    "datetime_wita",
                    "dstrct_code",
                    "unitno",
                    "gpsstatus",
                    "camcabinstatus",
                    "camfrontstatus",
                ).with_columns(
                    pl.col("gpsstatus").cast(pl.String),
                    pl.col("camcabinstatus").cast(pl.String),
                    pl.col("camfrontstatus").cast(pl.String),
                )
                # .with_columns(pl.col("datetime_wita").dt.replace_time_zone(None)),
            )

            st.dataframe(df_data)

            st.bar_chart(base_data, x="datetime_wita", y="constant", color="gpsstatus")

            st.bar_chart(
                base_data, x="datetime_wita", y="constant", color="camfrontstatus"
            )
            st.bar_chart(
                base_data, x="datetime_wita", y="constant", color="camcabinstatus"
            )

        with tab_speed:
            st.text(f"Obtained data with {len(dataframe)} rows")
            speed_df_data = base_data.select(
                "hiveperiod",
                "datetime_wita",
                "dstrct_code",
                "unitno",
                "speedsource",
                "VehicleSpeed",
                "gpsspeed",
                "gpsnumsat",
                "error_rate",
                "constant",
            )  # .with_columns(pl.col("datetime_wita").dt.replace_time_zone(None))

            st.dataframe(speed_df_data)
            st.bar_chart(
                speed_df_data, x="datetime_wita", y="constant", color="speedsource"
            )
            st.line_chart(
                speed_df_data,
                x="datetime_wita",
                y=["VehicleSpeed", "gpsspeed", "gpsnumsat"],
            )
            st.line_chart(
                speed_df_data,
                x="datetime_wita",
                y=["error_rate", "gpsnumsat"],
            )
