SELECT
	airport_code
	, date
	, time
	, hour
	, snow
	, temp_c
	, humidity_perc
	, precipitation_mm
	, wind_direction
	, wind_speed_kmh
	, pressure_hpa
	, (CASE
		WHEN condition_code = 1 THEN 'clear'
		WHEN condition_code = 2 THEN 'fair'
		WHEN condition_code = 3 THEN 'cloudy'
		WHEN condition_code = 4 THEN 'overcast'
		WHEN condition_code = 5 THEN 'fog'
		WHEN condition_code = 6 THEN 'freezing_fog'
		WHEN condition_code = 7 THEN 'light_rain'
		WHEN condition_code = 8 THEN 'rain'
		WHEN condition_code = 9 THEN 'heavy_rain'
		WHEN condition_code = 10 THEN 'freezing_rain'
		WHEN condition_code = 11 THEN 'heavy_freezing_rain'
		WHEN condition_code = 12 THEN 'sleet'
		WHEN condition_code = 13 THEN 'heavy_sleet'
		WHEN condition_code = 14 THEN 'light_snowfall'
		WHEN condition_code = 15 THEN 'snowfall'
		WHEN condition_code = 16 THEN 'heavy_snowfall'
		WHEN condition_code = 17 THEN 'rain_shower'
		WHEN condition_code = 18 THEN 'heavy_rain_shower'
		WHEN condition_code = 19 THEN 'sleet_shower'
		WHEN condition_code = 20 THEN 'heavy_sleet_shower'
		WHEN condition_code = 21 THEN 'snow_shower'
		WHEN condition_code = 22 THEN 'heavy_snow_shower'
		WHEN condition_code = 23 THEN 'lightning'
		WHEN condition_code = 24 THEN 'hail'
		WHEN condition_code = 25 THEN 'thunderstorm'
		WHEN condition_code = 26 THEN 'heavy_thunderstorm'
		WHEN condition_code = 27 THEN 'storm'
		ELSE 'data_missing'
	END) AS condition	
FROM {{ref('prep_weather')}}