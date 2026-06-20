/*
=============================================================================
* 모델명      : stg_fact_returns
* 모델 목적   : 반품 트랜잭션 팩트 원천 데이터 뷰 (customer_id SHA-256 마스킹 적용)
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
    hex(SHA256(concat(customer_id, 'my_secure_salt_123!'))) as customer_id,
    product_id,
    location_id,
    payment_id,
    fulfillment_id,
    quantity,
    unit_price,
    refund_amount,
    review_text,
    days_to_return,
    restocking_fee,
    customer_rating,
    sentiment_score,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse', 'stg_fact_returns') }}
