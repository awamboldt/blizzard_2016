WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, timestamp::TIME AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , TO_CHAR(timestamp, 'FMday') AS weekday         -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, DATE_PART('month', timestamp) AS date_month
		, DATE_PART('year', timestamp) AS date_year
		, EXTRACT(WEEK FROM timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN time BETWEEN '20:01:00' AND '11:59:59' THEN 'night'
			WHEN time BETWEEN '00:00:00' AND '06:00:00' THEN 'night'
			WHEN time BETWEEN '06:01:00' AND '17:00:00' THEN 'day'
			WHEN time BETWEEN '17:01:00' AND '20:00:00' THEN 'evening'
		END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features