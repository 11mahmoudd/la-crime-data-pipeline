-- Test to ensure no future dates in crime data
SELECT 
    date_occ,
    COUNT(*) as future_date_count
FROM {{ ref('stg_crime') }}
WHERE date_occ > CURRENT_DATE
GROUP BY date_occ
HAVING COUNT(*) > 0
