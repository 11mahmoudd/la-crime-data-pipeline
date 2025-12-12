{{ config(schema='stg') }}

with source as (
    select * from raw.raw_crime
),

renamed as (
    select
        cast(dr_no as integer)              as dr_no,
        cast(date_occ as date)              as date_occ,
        -- Convert time_occ from integer (845, 1530) to time format (08:45:00, 15:30:00)
        to_timestamp(lpad(time_occ::text, 4, '0'), 'HH24MI')::time as time_occ,
        cast(area as integer)               as area,
        cast(area_name as text)             as area_name,
        cast(crm_cd as integer)             as crm_cd,
        cast(crm_cd_desc as text)           as crm_cd_desc,
        cast(vict_age as integer)           as vict_age,
        -- Clean vict_sex: replace H, -, and NULL with UNKNOWN
        case 
            when upper(trim(vict_sex)) in ('M', 'F', 'X') then upper(trim(vict_sex))
            else 'UNKNOWN'
        end as vict_sex,
        upper(trim(status))                 as status,
        trim(status_desc)                   as status_desc
    from source
    -- Filter out negative ages
    where vict_age > 0
),

deduplicated as (
    select distinct on (dr_no)
        dr_no,
        date_occ,
        time_occ,
        area,
        area_name,
        crm_cd,
        crm_cd_desc,
        vict_age,
        vict_sex,
        status,
        status_desc
    from renamed
    order by dr_no, date_occ desc
)

select * from deduplicated