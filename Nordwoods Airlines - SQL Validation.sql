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

WITH 
flights_ontime AS (
  SELECT a.AIRLINE, a.AIRLINE_DELAY
  FROM stg_flights a 
  WHERE 1=1
  AND a.`YEAR` = 2015
  AND a.AIRLINE_DELAY IS NOT NULL
  AND a.AIRLINE_DELAY = 0
)
, flights_total AS (
  SELECT a.AIRLINE, a.AIRLINE_DELAY
  FROM stg_flights a 
  WHERE 1=1
  AND a.`YEAR` = 2015
  AND a.AIRLINE_DELAY IS NOT NULL
)
, airline_ontime_final AS ( SELECT AIRLINE, COUNT(*) ONTIME FROM flights_ontime GROUP BY AIRLINE )
, airline_total_final AS ( SELECT AIRLINE, COUNT(*) TOTAL FROM flights_total  GROUP BY AIRLINE )
SELECT a.AIRLINE, a.ONTIME, b.TOTAL,  ROUND(a.ONTIME/b.TOTAL*100,2) OnTimePercentage
FROM airline_ontime_final a
INNER JOIN airline_total_final b USING(airline)
ORDER BY 1 
;

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
WITH 
airlines_and_routes AS (
  SELECT 	b.AIRLINE, CONCAT(a.ORIGIN_AIRPORT, '-',a.DESTINATION_AIRPORT) ROUTE
  FROM 	stg_flights a
  INNER JOIN stg_airlines b ON a.airline = b.iata_code
  GROUP BY b.airline, route
)
SELECT airline, COUNT(*) uniqueRoutes FROM airlines_and_routes GROUP BY airline ORDER BY 2 DESC LIMIT 1
;
