{{ config(schema='dwh') }}

with date_time_spine as (
    select distinct
        date_occ as date,
        time_occ as time
    from {{ ref('stg_crime') }}
    where date_occ is not null and time_occ is not null
),

date_details as (
    select
        -- Primary Key: DateKey in YYYYMMDD format
        to_char(date, 'YYYYMMDD')::int as date_key,
        
        -- Date Components
        date as full_date,
        extract(day from date)::int as day,
        case 
            when extract(day from date) in (1, 21, 31) then 'st'
            when extract(day from date) in (2, 22) then 'nd'
            when extract(day from date) in (3, 23) then 'rd'
            else 'th'
        end as day_suffix,
        
        -- Weekday Information
        extract(dow from date)::int as weekday, -- 0=Sunday, 6=Saturday
        to_char(date, 'Day') as weekday_name,
        to_char(date, 'Dy') as weekday_name_short,
        left(to_char(date, 'Day'), 1) as weekday_name_first_letter,
        extract(doy from date)::int as day_of_year,
        
        -- Week Information
        ceil(extract(day from date) / 7.0)::int as week_of_month,
        extract(week from date)::int as week_of_year,
        
        -- Month Information
        extract(month from date)::int as month,
        to_char(date, 'Month') as month_name,
        to_char(date, 'Mon') as month_name_short,
        left(to_char(date, 'Month'), 1) as month_name_first_letter,
        to_char(date, 'MMYYYY') as mmyyyy,
        to_char(date, 'YYYY-Mon') as month_year,
        
        -- Quarter Information
        extract(quarter from date)::int as quarter,
        case extract(quarter from date)
            when 1 then 'First'
            when 2 then 'Second'
            when 3 then 'Third'
            when 4 then 'Fourth'
        end as quarter_name,
        
        -- Year Information
        extract(year from date)::int as year,
        
        -- Time Information
        time as time_occ,
        extract(hour from time)::int as hour_of_day,
        extract(minute from time)::int as minute_of_hour,
        case 
            when extract(hour from time) between 0 and 5 then 'Night'
            when extract(hour from time) between 6 and 11 then 'Morning'
            when extract(hour from time) between 12 and 17 then 'Afternoon'
            else 'Evening'
        end as time_of_day,
        case 
            when extract(hour from time) between 7 and 18 then 'Business Hours'
            else 'Off Hours'
        end as business_hours_flag,
        
        -- Boolean Flags
        case when extract(dow from date) in (0, 6) then true else false end as is_weekend,
        
        -- First/Last Dates
        date_trunc('year', date)::date as first_date_of_year,
        (date_trunc('year', date) + interval '1 year - 1 day')::date as last_date_of_year,
        date_trunc('quarter', date)::date as first_date_of_quarter,
        (date_trunc('quarter', date) + interval '3 months - 1 day')::date as last_date_of_quarter,
        date_trunc('month', date)::date as first_date_of_month,
        (date_trunc('month', date) + interval '1 month - 1 day')::date as last_date_of_month,
        (date - extract(dow from date)::int)::date as first_date_of_week,
        (date + (6 - extract(dow from date)::int))::date as last_date_of_week,
        
        -- Current Indicators (relative to today)
        case when extract(year from date) = extract(year from current_date) then 1 else 0 end as is_current_year,
        case when date_trunc('quarter', date) = date_trunc('quarter', current_date) then 1 else 0 end as is_current_quarter,
        case when date_trunc('month', date) = date_trunc('month', current_date) then 1 else 0 end as is_current_month,
        case when date_trunc('week', date) = date_trunc('week', current_date) then 1 else 0 end as is_current_week,
        case when date = current_date then 1 else 0 end as is_current_day
        
    from date_time_spine
)

select * from date_details
order by date_key
