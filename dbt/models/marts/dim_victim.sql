{{ config(schema='dwh') }}

with base as (
    select distinct
        md5(
            coalesce(vict_age::text,'') ||
            coalesce(vict_sex,'UNKNOWN')  -- replace null/undefined with 'UNKNOWN'
        ) as victim_key,
        vict_age,
        coalesce(vict_sex, 'UNKNOWN') as vict_sex  -- replace null/undefined with 'UNKNOWN'
    from {{ ref('stg_crimes') }}
)

select * from base