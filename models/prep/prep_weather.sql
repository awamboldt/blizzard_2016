WITH merged_snow AS 
    (SELECT d.airport_code
    , d.date
    , h.time
    , AVG(d.max_snow_mm) AS snow
    FROM {{ref('staging_weather_hourly')}} AS h
    LEFT JOIN {{ref('staging_weather_daily')}} AS d ON (h.date=d.date AND d.airport_code = h.airport_code)
    GROUP BY d.airport_code, d.date, h.time
    ORDER BY d.date, h.time)
SELECT s.*
    , w.*
FROM merged_snow AS s
LEFT JOIN {{ref('staging_weather_hourly')}} AS w ON (s.date = w.date AND s.time = w.time AND s.airport_code = w.airport_code)
