# Datalog Compacter System - Version 1

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
