WITH daily_raw AS (
    SELECT
            airport_code,
            station_id,
            JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
    FROM {{source('weather_data', 'weather_daily_raw')}}
),
daily_flattened AS (
    SELECT  airport_code,
            station_id,
            (json_data->>'date')::DATE AS date,
            (json_data->>'tavg')::NUMERIC AS avg_temp_c,
            (json_data->>'tmin')::NUMERIC AS min_temp_c,
            (json_data->>'tmax')::NUMERIC AS max_temp_c,
            (json_data->>'prcp')::NUMERIC AS precipitation_mm,
            ((json_data->>'snow')::NUMERIC)::INTEGER AS max_snow_mm,
            ((json_data->>'wdir')::NUMERIC)::INTEGER AS avg_wind_direction,
            (json_data->>'wspd')::NUMERIC AS avg_wind_speed_kmh,
            (json_data->>'wpgt')::NUMERIC AS wind_peakgust_kmh,
            (json_data->>'pres')::NUMERIC AS avg_pressure_hpa,
            (json_data->>'tsun')::INTEGER AS sun_minutes
    FROM daily_raw
)
SELECT * 
FROM daily_flattened