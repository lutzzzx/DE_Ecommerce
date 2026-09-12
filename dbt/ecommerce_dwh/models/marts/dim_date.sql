with bounds as (
    select
        min(order_purchase_timestamp)::date as min_date,
        current_date as max_date
    from {{ ref('stg_orders') }}
),
date_spine as (
    select generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval '1 day'
    )::date as date_day
)
select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dow from date_day) as day_of_week,
    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Month') as month_name,
    case when extract(dow from date_day) in (0,6) then true else false end as is_weekend
from date_spine
