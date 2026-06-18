/*
=============================================================================
* 모델명      : fact_orders
* 모델 목적   : 주문 상세 트랜잭션 팩트 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='order_line_id',
    primary_key='order_line_id'
) }}

select
    order_line_id,
    order_id,
    date_id,
    order_date,
    customer_id,
    product_id,
    category,
    seller_id,
    seller_rating,
    location_id,
    delivery_person_id,
    payment_id,
    campaign_id,
    fulfillment_id,
    quantity,
    unit_price,
    discount_percentage,
    tax_percentage,
    shipping_fee,
    expected_delivery_date,
    actual_delivery_date,
    delivery_rating,
    order_status,
    return_flag,
    refund_amount,
    return_date,
    customer_rating,
    review_text,
    sentiment_score,
    delivery_delay_days,
    delivery_sla_days,
    gross_amount,
    discount_amount,
    tax_amount,
    net_amount,
    ts_ms
from {{ ref('stg_fact_orders') }}
