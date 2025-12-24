# Datalog Compacter System - Version 1

![banner](https://github.com/FauzanAcyuto/iot-bigdata-streamlit-dashboard/blob/master/v1-datalog-compacter/media/data%20compacter%20project.png)

## Why?

In Mata Hati 02 (My Smart Mining IoT Project) we had 600+ devices installed across two job sites, these devices each generate 1 row per second containing telemetry from various sensors (179 fields) which comes out to around 2.16 Million rows per hour.

This kind of throughput needed to happen in the middle of the Borneo jungle with frequent network outages, areas that has poor coverage, and other environmental challenges.

The whole pipeline will be covered in my version 2 of this project documentation. In this readme we will get going after the fact that the data has already been stored in AWS S3. And the challenge has become data optimization one instead of data delivery.

The existing data is stored in this manner:

```
smartdbucket
|-BRCB # jobsite code
|   |-SLS30Ixxx # deviceid
|   |     |-2025121201 # date hour
|   |     |      |-2025121201.txt.gz # actual data file in txt.gz format
|   |     |-...
|   |-SLS30Ixxy
|   |-SLS30Ixyy
|   |-...
|-BRCG
    |-...
```

There are many issues with this current format:

1. The data is stored as a newline delimited JSON compressed to GZIP - Not the most performative format
2. The data is stored in many small files (one file per-device per-hour) - Also a big hit on performance
3. The data is partitioned in a way that makes sense for data production, but not for querying
4. Querying one weeks worth of data with DuckDB using 4GB's of memory takes a whopping 15 minutes (for a single job site)

**The goal:**
Turn S3 into a performant datalake where operations team can query 1 days worth of data in seconds to make quick decisions in the field (Speed, safety, location, device health information)

## How?

We will be implementing the following solutions to achieve that goal:

1. Turn the data into parquet format
2. Partition the data properly according to query patterns
3. Enrich the data with the necessary columns
4. Process the data using larger-than-memory, Zero-copy architecture for maximum efficiency (cost, time, resources)

## The results?

Immense performance improvement:

1. Querying data from the old bucket to the news cut down query times from 15+ minutes to just 2 seconds.

Before:
![before](https://github.com/FauzanAcyuto/iot-bigdata-streamlit-dashboard/blob/master/v1-datalog-compacter/media/before.jpeg)
After:
![after](https://github.com/FauzanAcyuto/iot-bigdata-streamlit-dashboard/blob/master/v1-datalog-compacter/media/after.jpeg)
2. Storage space decreased by 10%
3. A server with 20 GB's of ram can process 3 Million rows of data in <3minst

## Technical Decisions & Techiniques

### Turn the data into parquet format

Now there are a few tools and methods that we can pick here. We know that the data is stored in new line delimited json, schema drift happens very often as updates get rolled out to the devices, and the files are stored in many-many sub folders.

We need a solution that :

1. Allows us to 'glob' files out of S3
2. Allows larger-than-memory Zero-copy data processing
3. Can handle type errors gracefully (since actual real life data is always dirty)

A solution that uses pyarrow as its base, supports lazy loading, and parquet transformation. This narrows the candidate to two options, DuckDB and Polars. And I decided to choose duckDB because read_json_auto() handles type errors very well.

### Partition the data properly according to query patterns

The most common query that happens to this data is:

```sql
SELECT *
FROM table
WHERE 1=1
  AND hiveperiod = '2025-12-12'
  AND distrik = 'BRCB'
  AND unitno = 'DTxxxx'

```

With date filtering happening much more often than district, in this case the partition should look like this:

```
smartdbucket
|-hiveperiod=2025-12-12 # by date, not date hour
|           |-district=BRCB
|           |        |-data_{uid}.parquet
|           |-district=BRCG
|           |-...
|-hiveperiod=2025-12-13
```

**but wait. why not partition by unitno?**
This is because partitioning by unitno comes at a trade off. 600 devices means that each hiveperiod will have 600 files of ~20MB files each, this slows down reads as parquet files are more efficient with large file sizes. So well combine all units into one file to get 150-250MB files for each day.

### Enrich data with necesary columns

The source data didn't have the two most frequently queried columns either, fortunately adding them is very easy with DuckDB since is based on the SQL syntax

```python
import duckdb

data = conn.sql(
    f"""
    SELECT 
        *,
        '{distrik}' AS dstrct_code,
        CAST(to_timestamp(heartbeat) + INTERVAL 8 HOURS AS DATE) as hiveperiod,
        filename AS source_file
    FROM read_json_auto({s3key_list_string}, filename=true, sample_size=-1, union_by_name=true)
"""
)
```

### Process the data using larger than memory zero-copy architecture

This is the strong point of pyarrow based systems such as DuckDB or Polars (and of course pyspark does it best with 100GB+ datasets).

Using DuckDB relations I never load the data into memory in the course of the script, instead I give duckdb a memory limit (4-20GB) to allow it to process data faster or to preserve resources

![zeo-copy](https://github.com/FauzanAcyuto/iot-bigdata-streamlit-dashboard/blob/master/v1-datalog-compacter/media/zero%20copy.png)
