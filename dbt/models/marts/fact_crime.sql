{{ config(
    schema='dwh',
    materialized='incremental',
    unique_key='crime_id'
) }}

with crimes as (

    select *
    from {{ ref('stg_crime') }}

    {% if is_incremental() %}
    where date_occ > (
        select coalesce(max(d.full_date), date '1900-01-01')
        from {{ this }} f
        join {{ ref('dim_date') }} d
        on f.date_key = d.date_key
    )
    {% endif %}

),

victims as (
    select
        victim_key,
        vict_age,
        vict_sex
    from {{ ref('dim_victim') }}
),

areas as (
    select area_key, area_name
    from {{ ref('dim_area') }}
),

crime_types as (
    select crime_type_key, crm_cd_desc
    from {{ ref('dim_crime_type') }}
),

statuses as (
    select status_key, status_desc
    from {{ ref('dim_status') }}
),

dates as (
    select date_key, time_occ
    from {{ ref('dim_date') }}
)

select

    md5(
        coalesce(c.dr_no::text,'') ||
        coalesce(c.date_occ::text,'') ||
        coalesce(c.time_occ::text,'') ||
        coalesce(c.area::text,'') ||
        coalesce(c.crm_cd::text,'') ||
        coalesce(c.vict_age::text,'') ||
        coalesce(c.vict_sex,'UNKNOWN') ||
        coalesce(c.status,'')
    ) as crime_id,  -- surrogate primary key

    c.dr_no,
    d.date_key,
    a.area_key,
    ct.crime_type_key,
    v.victim_key,
    s.status_key

from crimes c

left join dates d
    on to_char(c.date_occ, 'YYYYMMDD')::int = d.date_key

left join areas a
    on c.area = a.area_key

left join crime_types ct
    on c.crm_cd = ct.crime_type_key

left join victims v
    on c.vict_age = v.vict_age
    and c.vict_sex = v.vict_sex

left join statuses s
    on c.status = s.status_key
