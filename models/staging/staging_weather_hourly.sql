WITH hourly_raw AS (
    SELECT
            airport_code,
            station_id,
            JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
    FROM {{source('weather_data', 'weather_hourly_raw')}}
),
hourly_data AS (
    SELECT  
            airport_code
            ,station_id
            ,(json_data->>'time')::TIMESTAMP AS timestamp	
            ,(json_data->>'temp')::NUMERIC AS temp_c
            ,(json_data->>'dwpt')::NUMERIC AS dewpoint_c
            ,(json_data->>'rhum')::NUMERIC AS humidity_perc
            ,(json_data->>'prcp')::NUMERIC AS precipitation_mm
            ,(json_data->>'snow')::INTEGER AS snow_mm
            ,((json_data->>'wdir')::NUMERIC)::INTEGER AS wind_direction
            ,(json_data->>'wspd')::NUMERIC AS wind_speed_kmh
            ,(json_data->>'wpgt')::NUMERIC AS wind_peakgust_kmh
            ,(json_data->>'pres')::NUMERIC AS pressure_hpa 
            ,(json_data->>'tsun')::INTEGER AS sun_minutes
            ,(json_data->>'coco')::INTEGER AS condition_code
    FROM hourly_raw
)
SELECT * 
FROM hourly_data