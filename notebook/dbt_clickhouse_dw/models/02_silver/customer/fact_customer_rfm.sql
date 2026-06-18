/*
=============================================================================
* 모델명      : fact_customer_rfm
* 모델 목적   : 고객 RFM 지표 팩트 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='customer_id',
    primary_key='customer_id'
) }}

select
    customer_id,
    recency_days,
    frequency_orders,
    monetary_value,
    avg_order_value,
    first_purchase_date,
    last_purchase_date,
    customer_lifetime_days,
    rfm_score,
    customer_segment,
    ts_ms
from {{ ref('stg_fact_customer_rfm') }}
