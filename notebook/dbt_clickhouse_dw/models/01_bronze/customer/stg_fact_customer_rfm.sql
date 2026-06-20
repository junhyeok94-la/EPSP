/*
=============================================================================
* 모델명      : stg_fact_customer_rfm
* 모델 목적   : 고객 RFM 분석 팩트 원천 데이터 뷰 (customer_id SHA-256 마스킹 적용)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성 및 가명화 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    hex(SHA256(concat(customer_id, 'my_secure_salt_123!'))) as customer_id,
    recency_days,
    frequency_orders,
    monetary_value,
    avg_order_value,
    toDate(first_purchase_date) as first_purchase_date,
    toDate(last_purchase_date) as last_purchase_date,
    customer_lifetime_days,
    rfm_score,
    customer_segment,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse', 'stg_fact_customer_rfm') }}
