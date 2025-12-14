```python

import polars as pl
import json
from pathlib import Path
import s3fs
import duckdb

```

#%%
# import storage options
with open(Path('creds/creds.json'),'r') as target:
    creds = json.load(target)
    storage_options = creds['AWS']

storage_options
#%%
s3 = s3fs.S3FileSystem(key=storage_options['AccessKey'],secret=storage_options['SecretKey'])
deviceid = 'SLS30I009'
hivehour = '20251212*'


bucketpath = f's3://{storage_options['BucketName']}/datalog*/BRC*/{deviceid}/{hivehour}/{hivehour}.gzip.txt'  

s3paths = s3.glob(bucketpath)
s3paths

#%%
# test pull data from s3
duckdb.sql(f"""
SELECT from_epoch(heartbeat) as tgl_utc,COUNT(1) as jml
FROM read_json_auto({bucketpath})
GROUP BY from_epoch(heartbeat)
ORDER BY from_epoch(heartbeat)
           """)



#%%
