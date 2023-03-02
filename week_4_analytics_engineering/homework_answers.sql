--- QUESTION 1
SELECT count(1)
FROM `nytaxi-378623.production.fact_trips` 
WHERE TRUE
  AND pickup_datetime >= TIMESTAMP '2019-01-01 00:00:00'
  AND pickup_datetime < TIMESTAMP '2021-01-01 00:00:00';


--- QUESTION 2
SELECT service_type, count(1)
FROM `nytaxi-378623.production.fact_trips` 
WHERE TRUE
  AND pickup_datetime >= TIMESTAMP '2019-01-01 00:00:00'
  AND pickup_datetime < TIMESTAMP '2021-01-01 00:00:00'
GROUP BY service_type;


--- QUESTION 3
SELECT count(1)
FROM `nytaxi-378623.dbt_taxi_models.stg_fhv_tripdata`
WHERE TRUE
  AND pickup_datetime >= TIMESTAMP '2019-01-01 00:00:00'
  AND pickup_datetime < TIMESTAMP '2020-01-01 00:00:00';

--- QUESTION 4
SELECT count(1)
FROM `nytaxi-378623.production.fact_ftv_trips` 
WHERE TRUE
  AND pickup_datetime >= TIMESTAMP '2019-01-01 00:00:00'
  AND pickup_datetime < TIMESTAMP '2020-01-01 00:00:00';


--- QUESTION 5
SELECT extract(month from pickup_datetime) as month
  ,COUNT(1) as num_trips
FROM `production.fact_ftv_trips`
WHERE TRUE
  AND extract(year from pickup_datetime) = 2019
GROUP BY extract(month from pickup_datetime);