with orders as (
    select * from {{ ref('stg_orders') }}
),
items as (
    select
        order_id,
        count(*) as total_items,
        sum(price) as gross_merchandise_value,
        sum(freight_value) as total_freight
    from {{ ref('stg_order_items') }}
    group by order_id
),
payments as (
    select
        order_id,
        sum(payment_value) as total_paid,
        count(distinct payment_type) as payment_methods_used
    from {{ ref('stg_payments') }}
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp::date as order_date,
    o.order_status,
    o.delivery_days,
    coalesce(i.total_items, 0) as total_items,
    coalesce(i.gross_merchandise_value, 0) as gross_merchandise_value,
    coalesce(i.total_freight, 0) as total_freight,
    coalesce(p.total_paid, 0) as total_paid,
    coalesce(p.payment_methods_used, 0) as payment_methods_used
from orders o
left join items i on o.order_id = i.order_id
left join payments p on o.order_id = p.order_id
