-- Databricks notebook source
-- Lets create a new database to host our tables
DROP DATABASE IF EXISTS phData CASCADE;
CREATE DATABASE phData;
USE phData;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df = spark.read.csv("/FileStore/tables/airlines.csv", header="true", inferSchema="true")
-- MAGIC df.write.format("parquet").saveAsTable("stg_airlines")
-- MAGIC 
-- MAGIC df = spark.read.csv("/FileStore/tables/airports.csv", header="true", inferSchema="true")
-- MAGIC df.write.format("parquet").saveAsTable("stg_airports")
-- MAGIC 
-- MAGIC df = spark.read.csv("/FileStore/tables/partition_01.csv", header="true", inferSchema="true")
-- MAGIC 
-- MAGIC df_partition_02 = spark.read.csv("/FileStore/tables/partition_02.csv", header="true", inferSchema="true")
-- MAGIC df = df.union(df_partition_02)
-- MAGIC 
-- MAGIC df_partition_03 = spark.read.csv("/FileStore/tables/partition_03.csv", header="true", inferSchema="true")
-- MAGIC df = df.union(df_partition_03)
-- MAGIC 
-- MAGIC df_partition_04 = spark.read.csv("/FileStore/tables/partition_04.csv", header="true", inferSchema="true")
-- MAGIC df = df.union(df_partition_04)
-- MAGIC 
-- MAGIC df_partition_05 = spark.read.csv("/FileStore/tables/partition_05.csv", header="true", inferSchema="true")
-- MAGIC df = df.union(df_partition_05)
-- MAGIC 
-- MAGIC df_partition_06 = spark.read.csv("/FileStore/tables/partition_06.csv", header="true", inferSchema="true")
-- MAGIC df = df.union(df_partition_06)
-- MAGIC 
-- MAGIC df_partition_07 = spark.read.csv("/FileStore/tables/partition_07.csv", header="true", inferSchema="true")
-- MAGIC df = df.union(df_partition_07)
-- MAGIC 
-- MAGIC df_partition_08 = spark.read.csv("/FileStore/tables/partition_08.csv", header="true", inferSchema="true")
-- MAGIC df = df.union(df_partition_08)
-- MAGIC 
-- MAGIC df.write.format("parquet").saveAsTable("stg_flights")

-- COMMAND ----------

-- Report 1 - Total number of flights by airline and airport on a monthly basis
SELECT
  b.Airline, c.Airport, a.Year, a.Month, COUNT(*) Flights
FROM
  stg_flights a
INNER JOIN
  stg_airlines b ON a.airline = b.iata_code
INNER JOIN
  stg_airports c ON a.origin_airport = c.iata_code
GROUP BY 
  b.airline, c.airport, a.year, a.month
ORDER BY
  b.airline, c.airport, a.year, a.month


-- COMMAND ----------

-- Report 2 - On time percentage of each airline for the year 2015

-- ALTER TABLE stg_flights MODIFY COLUMN airline varchar(200) NULL;
-- CREATE INDEX stg_flights_airline_idx ON stg_flights (airline);

-- ALTER TABLE stg_flights ADD COLUMN isDelayed INT;
-- CREATE INDEX stg_flights_isDelayed_idx ON stg_flights (isDelayed);

-- UPDATE stg_flights 
-- SET isDelayed = 
-- 	CASE 
-- 		WHEN arrival_delay != 0 THEN 1 	-- if delay is not 0 (mins) then there was a delay
-- 		ELSE 0 END 
-- ;

WITH 
flights AS (
	SELECT	airline
    , CASE 
		WHEN arrival_delay != 0 THEN 1 	        -- if delay is not 0 (mins) then there was a delay
		ELSE 0 END isDelayed
	FROM 	stg_flights a
	WHERE 	1=1
	AND 	YEAR = 2015 						-- asked for year 2015
	AND 	arrival_delay IS NOT NULL 			-- null means we are missing data
	AND 	arrival_delay >= 0 					-- negative numbers mean the flight was early
),
onTime AS (
	SELECT		Airline, COUNT(*) OnTime 
	FROM 		flights 
	WHERE 		isDelayed = 0 
	GROUP BY 	airline
),
total_flights AS (
	SELECT		airline, COUNT(*) Total 
	FROM 		flights 
	GROUP BY 	airline
)
SELECT 
	c.Airline, a.OnTime, b.Total
	, ROUND((a.onTime/b.total) * 100,2) OnTimePercentage
FROM 
	onTime a
INNER JOIN
	total_flights b USING(airline)
INNER JOIN
  stg_airlines c ON a.airline = c.iata_code
ORDER BY 1

-- COMMAND ----------

-- Report 3 - Airlines with the largest number of delays
SELECT
  b.Airline, COUNT(a.departure_delay) Delays
FROM
  stg_flights a
INNER JOIN
  stg_airlines b ON a.airline = b.iata_code
WHERE 
  a.departure_delay > 0
GROUP BY 
  b.Airline
ORDER BY 2 DESC

-- COMMAND ----------

-- Report 4 - Cancellation reasons by airport
SELECT 	
  c.Airport, a.CANCELLATION_REASON cancellationReason, COUNT(*) Cancellations
FROM 
  stg_flights a
INNER JOIN
  stg_airports c ON a.origin_airport = c.iata_code
GROUP BY 
  c.Airport, a.CANCELLATION_REASON
ORDER BY 1,2 

-- COMMAND ----------

-- Report 5 - Delay reasons by airport
SELECT 	
  c.Airport, SUM(AIR_SYSTEM_DELAY) airSystemDelay, SUM(SECURITY_DELAY) securityDelay, SUM(AIRLINE_DELAY) airlineDelay, SUM(LATE_AIRCRAFT_DELAY) aircraftDelay, SUM(WEATHER_DELAY) weatherDelay
FROM
  stg_flights a
INNER JOIN
  stg_airports c ON a.origin_airport = c.iata_code
GROUP BY 
  c.Airport
ORDER BY 1


-- COMMAND ----------

-- Report 6 - Airline with the most unique routes
WITH airport_routes AS (
	SELECT 	a.AIRLINE, CONCAT(a.ORIGIN_AIRPORT, '-',a.DESTINATION_AIRPORT) ROUTE
	FROM 	stg_flights a
),
route_by_airport AS (
	SELECT 	AIRLINE, ROUTE 
	FROM 	airport_routes 
	GROUP BY AIRLINE, ROUTE
)
SELECT 	
  b.Airline, COUNT(*) uniqueRoutes
FROM 	
  route_by_airport a
INNER JOIN
  stg_airlines b ON a.airline = b.iata_code
GROUP BY 
  b.airline
ORDER BY 2 DESC
LIMIT 1
