{{ config(schema='dwh') }}

with crimes as (
    select *
    from {{ ref('stg_crimes') }}
),

victims as (
    select
        md5(
            coalesce(vict_age::text,'') ||
            coalesce(vict_sex,'UNKNOWN')
        ) as victim_key,
        vict_age,
        coalesce(vict_sex,'UNKNOWN') as vict_sex
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
        coalesce(c.date_occ::text,'') ||
        coalesce(c.time_occ::text,'') ||
        coalesce(c.area::text,'') ||
        coalesce(c.crm_cd::text,'') ||
        coalesce(c.vict_age::text,'') ||
        coalesce(c.vict_sex,'UNKNOWN') ||
        coalesce(c.status,'')
    ) as crime_id,  -- surrogate primary key

    d.date_key,
    a.area_key,
    ct.crime_type_key,
    v.victim_key,
    s.status_key

from crimes c
left join dates d
    on to_char(c.date_occ, 'YYYYMMDD')::int = d.date_key
    and c.time_occ = d.time_occ
left join areas a
    on c.area = a.area_key
left join crime_types ct
    on c.crm_cd = ct.crime_type_key
left join victims v
    on md5(
        coalesce(c.vict_age::text,'') ||
        coalesce(c.vict_sex,'UNKNOWN')
    ) = v.victim_key
left join statuses s
    on c.status = s.status_key
