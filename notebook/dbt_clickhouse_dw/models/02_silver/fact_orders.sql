/*
=============================================================================
* 모델명      : fact_orders
* 모델 목적   : 주문 및 품목 상세 트랜잭션 팩트 테이블 (Silver Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 주문/결제/리뷰 결합, 파티셔닝 적용
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='(order_id, order_item_id)',
    partition_by="toYYYYMM(coalesce(order_purchase_timestamp, toDateTime('1970-01-01 00:00:00')))"
) }}

with payments_summary as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        any(payment_type) as primary_payment_type
    from {{ ref('stg_order_payments') }}
    group by order_id
),

reviews_summary as (
    select
        order_id,
        avg(review_score) as avg_review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
)

select
    oi.order_id as order_id,
    oi.order_item_id as order_item_id,
    o.customer_id as customer_id,
    oi.product_id as product_id,
    oi.seller_id as seller_id,
    o.order_status as order_status,
    o.order_purchase_timestamp as order_purchase_timestamp,
    o.order_approved_at as order_approved_at,
    o.order_delivered_carrier_date as order_delivered_carrier_date,
    o.order_delivered_customer_date as order_delivered_customer_date,
    o.order_estimated_delivery_date as order_estimated_delivery_date,
    oi.shipping_limit_date as shipping_limit_date,
    oi.price as price,
    oi.freight_value as freight_value,
    (oi.price + oi.freight_value) as item_total_value,
    coalesce(p.total_payment_value, 0.0) as order_total_payment,
    coalesce(p.primary_payment_type, 'unknown') as primary_payment_type,
    coalesce(r.avg_review_score, 0.0) as review_score,
    oi.op as op,
    oi.ts_ms as ts_ms
from {{ ref('stg_order_items') }} as oi
inner join {{ ref('stg_orders') }} as o on oi.order_id = o.order_id
left join payments_summary as p on oi.order_id = p.order_id
left join reviews_summary as r on oi.order_id = r.order_id
