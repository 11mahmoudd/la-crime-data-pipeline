{{ config(schema='dwh') }}

with base as (
    select distinct
        crm_cd as crime_type_key,
        crm_cd_desc
    from {{ ref('stg_crimes') }}
    where crm_cd is not null
)

select * from base