{{ config(schema='dwh') }}

with date_spine as (

    select
        generate_series(
            date '2000-01-01',
            date '2035-12-31',
            interval '1 day'
        )::date as date

)

select
    to_char(date, 'YYYYMMDD')::int as date_key,
    date as full_date,
    extract(year from date)::int as year,
    extract(month from date)::int as month,
    extract(day from date)::int as day,
    extract(dow from date)::int as weekday
from date_spine
