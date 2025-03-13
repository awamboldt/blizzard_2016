SELECT *
FROM {{ref('prep_flights')}} pf
LEFT JOIN {{ref('prep_weather')}} pw ON (pf.flight_date = pw.date AND pw.HOUR = pf.dep_hour AND pf.origin = pw.airport_code)
WHERE pf.origin IN ('EWR', 'PIT', 'IAD')
ORDER BY pf.flight_date, pf.origin, pf.dest