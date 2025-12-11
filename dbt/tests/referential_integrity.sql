-- Test to ensure fact table has matching dimension records
SELECT 
    'Missing area keys' as test_name,
    COUNT(*) as failed_count
FROM {{ ref('fact_crimes') }} f
LEFT JOIN {{ ref('dim_area') }} d ON f.area_key = d.area_key
WHERE d.area_key IS NULL

UNION ALL

SELECT 
    'Missing crime type keys' as test_name,
    COUNT(*) as failed_count
FROM {{ ref('fact_crimes') }} f
LEFT JOIN {{ ref('dim_crime_type') }} d ON f.crime_type_key = d.crime_type_key
WHERE d.crime_type_key IS NULL

UNION ALL

SELECT 
    'Missing victim keys' as test_name,
    COUNT(*) as failed_count
FROM {{ ref('fact_crimes') }} f
LEFT JOIN {{ ref('dim_victim') }} d ON f.victim_key = d.victim_key
WHERE d.victim_key IS NULL

HAVING SUM(failed_count) > 0
