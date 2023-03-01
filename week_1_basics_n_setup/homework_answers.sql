--QUESTION 3
--ANSWER 20530

-- SELECT *
-- FROM green_taxi_trips
-- WHERE TRUE
-- 	AND EXTRACT (DAY FROM lpep_pickup_datetime) = 15
-- 	AND EXTRACT (DAY FROM lpep_dropoff_datetime) = 15



--QUESTION 4
--ANSWER 2019-01-15

-- SELECT lpep_pickup_datetime
-- FROM green_taxi_trips
-- WHERE TRUE
-- ORDER BY trip_distance DESC
-- LIMIT 1



--QUESTION 5
--ANSWER 2 = 1282 & 3 = 254

-- SELECT passenger_count, count(1)
-- FROM green_taxi_trips
-- WHERE TRUE
-- 	AND date (lpep_pickup_datetime)='2019-01-01'
-- GROUP BY passenger_count
-- HAVING passenger_count = 2 OR passenger_count = 3



--QUESTION 6
--ANSWER Long Island City/Queens Plaza

-- SELECT DOZone."Zone"
-- FROM green_taxi_trips gtrip
-- 	JOIN taxi_zone_lookup PUZone ON gtrip."PULocationID" = PUZone."LocationID"
-- 	JOIN taxi_zone_lookup DOZone ON gtrip."DOLocationID" = DOZone."LocationID"
-- WHERE TRUE
-- 	AND PUZone."Zone" = 'Astoria'
-- ORDER BY gtrip.tip_amount DESC
-- LIMIT 1
