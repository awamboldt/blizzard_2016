WITH flights_data AS (
    SELECT * 
    FROM {{ref('staging_flights')}}
),
flights_cleaned AS(
    SELECT flight_date::DATE
            , TO_CHAR(f.dep_time, 'fm0000')::TIME AS dep_time
            , TO_CHAR(f.sched_dep_time, 'fm0000')::TIME AS sched_dep_time
            , TO_CHAR(f.dep_time,'HH24:MI') as dep_hour  -- time (hours:minutes) as TEXT data type
            ,f.dep_delay
		    ,(f.dep_delay * '1 minute'::INTERVAL) AS dep_delay_interval
            ,TO_CHAR(f.arr_time, 'fm0000')::TIME AS arr_time
            ,TO_CHAR(f.sched_arr_time, 'fm0000')::TIME AS sched_arr_time
            , TO_CHAR(f.arr_time,'HH24:MI') as arr_hour  -- time (hours:minutes) as TEXT data type
            ,f.arr_delay
            ,(f.arr_delay * '1 minute'::INTERVAL) AS arr_delay_interval
            ,f.airline
            ,f.tail_number
            ,f.flight_number
            ,f.origin
            ,air1.city AS origin_city
            ,air1.name AS origin_name
            ,f.dest
            ,air2.city AS dest_city
            ,air2.name AS dest_name
            ,f.air_time
            ,(f.air_time * '1 minute'::INTERVAL) AS air_time_interval
            ,f.actual_elapsed_time
            ,(f.actual_elapsed_time * '1 minute'::INTERVAL) AS actual_elapsed_time_interval
            ,(f.distance / 0.621371)::NUMERIC(6,2) AS distance_km -- see instruction hint
            ,f.cancelled
            ,f.diverted
    FROM flights_data as f
    LEFT JOIN {{ref('staging_airports')}} AS air1 ON air1.faa = f.origin
    LEFT JOIN {{ref('staging_airports')}} AS air2 ON air2.faa = f.dest
)
SELECT * FROM flights_cleaned