{{ config(schema='dwh') }}

with base as (
    select distinct
        status as status_key,
        status_desc
    from {{ ref('stg_crime') }}
    where status is not null
)

select * from base