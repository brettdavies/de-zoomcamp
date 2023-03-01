SELECT 1

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_fhv_trips`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nytaxi-bucket/data/fhv/fhv_tripdata_2019-*.csv.gz']
);


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE nytaxi.fhv_trips_non_partitoned AS
SELECT * FROM nytaxi.external_fhv_trips;

-- SELECT count(1) FROM `nytaxi-378623.nytaxi.external_fhv_trips`


-- Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
SELECT COUNT(DISTINCT affiliated_base_number)
FROM `nytaxi.external_fhv_trips`

SELECT COUNT(DISTINCT affiliated_base_number)
FROM `nytaxi.fhv_trips_non_partitoned`


SELECT COUNT(1)
FROM `nytaxi.fhv_trips_non_partitoned`
WHERE TRUE
  AND PUlocationID IS NULL
  AND DOlocationID IS NULL


 -- Create a partitioned table from external table
CREATE OR REPLACE TABLE nytaxi.fhv_trips_partitoned
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM nytaxi.fhv_trips_non_partitoned


SELECT DISTINCT Affiliated_base_number
FROM `nytaxi.fhv_trips_non_partitoned`
WHERE TRUE
  AND pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'

SELECT DISTINCT Affiliated_base_number
FROM `nytaxi.fhv_trips_partitoned`
WHERE TRUE
  AND pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'



-- PARQUET
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.p_external_fhv_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nytaxi-bucket/data/fhv/fhv_tripdata_2019-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE nytaxi.p_fhv_trips_non_partitoned AS
SELECT * FROM nytaxi.p_external_fhv_trips;

SELECT COUNT(DISTINCT affiliated_base_number)
FROM `nytaxi.p_external_fhv_trips`

SELECT COUNT(DISTINCT affiliated_base_number)
FROM `nytaxi.p_fhv_trips_non_partitoned`

SELECT DISTINCT Affiliated_base_number
FROM `nytaxi.p_fhv_trips_non_partitoned`
WHERE TRUE
  AND pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
