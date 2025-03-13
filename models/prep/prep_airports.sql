Select *
FROM {{ref('staging_airports')}}
WHERE faa IN ('EWR', 'IAD', 'PIT')