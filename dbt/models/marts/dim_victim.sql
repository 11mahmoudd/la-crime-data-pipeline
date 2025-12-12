{{ config(schema='dwh') }}

with base as (
    select distinct
                -- Generate victim surrogate key
        md5(
            coalesce(vict_age::text,'') ||
            case 
                when upper(trim(vict_sex)) in ('M', 'F', 'X') then upper(trim(vict_sex))
                else 'UNKNOWN'
            end
        ) as victim_key,
        vict_age,
        vict_sex  
    from {{ ref('stg_crime') }}
)

select * from base