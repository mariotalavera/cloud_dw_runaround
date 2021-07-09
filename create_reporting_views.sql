USE phdata.public;

-- df_report_1 | Total Number Of Flights By Airline And Airport, 2015
CREATE OR REPLACE MATERIALIZED VIEW df_report_1_view AS 
SELECT   AIRLINE_NAME "Airline", ORIGIN_AIRPORT_NAME "Airport", MONTH "Month", COUNT(*) Flights
FROM     df_report_1
GROUP BY AIRLINE_NAME, ORIGIN_AIRPORT_NAME, MONTH
;

-- df_report_2 | Airline On Time Percentage, 2015
CREATE OR REPLACE MATERIALIZED VIEW df_report_2_view AS SELECT * FROM df_report_2;

-- df_report_3 | Airlines With Largest Number Of Delays
CREATE OR REPLACE MATERIALIZED VIEW df_report_3_view AS 
SELECT  AIRLINE_NAME, Count("DEPARTURE_DELAY") "Departure Delay" 
FROM    df_report_3 
GROUP BY AIRLINE_NAME
;

-- df_report_4 | Cancellation Reasons By Airport
CREATE OR REPLACE MATERIALIZED VIEW df_report_4_view AS 
SELECT ORIGIN_AIRPORT_NAME "Airport", CANCELLATION_REASON "Cancellation Reason", COUNT(*) "Cancellations" 
FROM  df_report_4
GROUP BY ORIGIN_AIRPORT_NAME, CANCELLATION_REASON
;

-- df_report_5 | Delay Reasons By Airport
CREATE OR REPLACE MATERIALIZED VIEW df_report_5_view AS 
SELECT 
    ORIGIN_AIRPORT_NAME "Airport", 
    SUM(AIR_SYSTEM_DELAY) "Air System Delay", 
    SUM(SECURITY_DELAY) "Security Delay", 
    SUM(AIRLINE_DELAY) "Airline Delay", 
    SUM(LATE_AIRCRAFT_DELAY) "Late Aircraft Delay", 
    SUM(WEATHER_DELAY) "Weather Delay"
FROM 
    df_report_5
GROUP BY 
    ORIGIN_AIRPORT_NAME
;

-- df_report_6 | Airline With The Largest Number Of Unique Routes
CREATE OR REPLACE MATERIALIZED VIEW df_report_6_view AS 
WITH airlines_and_routes AS (
    SELECT   AIRLINE_NAME, route 
    FROM     df_report_6
    )
SELECT AIRLINE_NAME "Airline", COUNT(*) "Unique Routes" 
FROM airlines_and_routes 
GROUP BY AIRLINE_NAME 
;