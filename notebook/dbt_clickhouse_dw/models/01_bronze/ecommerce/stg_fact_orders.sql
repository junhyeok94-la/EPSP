/*
=============================================================================
* 모델명      : stg_fact_orders
* 모델 목적   : 주문 트랜잭션 팩트 원천 데이터 뷰 (customer_id SHA-256 마스킹 적용)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성 및 가명화 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    order_line_id,
    order_id,
    date_id,
    toDate(order_date) as order_date,
    hex(SHA256(concat(customer_id, 'my_secure_salt_123!'))) as customer_id,
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
    toDate(expected_delivery_date) as expected_delivery_date,
    toDate(actual_delivery_date) as actual_delivery_date,
    delivery_rating,
    order_status,
    return_flag,
    refund_amount,
    toDate(return_date) as return_date,
    customer_rating,
    review_text,
    sentiment_score,
    delivery_delay_days,
    delivery_sla_days,
    gross_amount,
    discount_amount,
    tax_amount,
    net_amount,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_fact_orders') }}
