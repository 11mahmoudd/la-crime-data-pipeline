{{ config(schema='dwh') }}

with base as (
    select distinct
        area as area_key,
        area_name
    from {{ ref('stg_crimes') }}
    where area is not null
)

select * from base