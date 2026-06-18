/*
=============================================================================
* 모델명      : fact_returns
* 모델 목적   : 반품 트랜잭션 팩트 테이블 (ReplacingMergeTree 기반 중복 제거)
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
    customer_id,
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
    ts_ms
from {{ ref('stg_fact_returns') }}
