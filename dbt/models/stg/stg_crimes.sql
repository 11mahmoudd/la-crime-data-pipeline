{{ config(
    schema='stg',
    materialized='incremental'
) }}

with source as (    
    select * from raw.raw_crimes
    {% if is_incremental() %}
    -- Only process new records since last run
    where date_occ > (select coalesce(max(date_occ), '1900-01-01'::date) from {{ this }})
    {% endif %}
),

renamed as (
    select
        cast(date_occ as date)              as date_occ,
        cast(time_occ as time)              as time_occ,
        cast(area as integer)               as area,
        cast(area_name as text)             as area_name,
        cast(crm_cd as integer)             as crm_cd,
        cast(crm_cd_desc as text)           as crm_cd_desc,
        cast(vict_age as integer)           as vict_age,
        upper(trim(vict_sex))               as vict_sex,
        upper(trim(status))                 as status,
        trim(status_desc)                   as status_desc
    from source
)

select * from renamed