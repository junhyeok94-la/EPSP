/*
=============================================================================
* 모델명      : fact_fulfillment_performance
* 모델 목적   : 배송 및 풀필먼트 실적 팩트 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='(date_id, fulfillment_id, location_id, delivery_person_id)',
    primary_key='(date_id, fulfillment_id, location_id, delivery_person_id)'
) }}

select
    date_id,
    fulfillment_id,
    location_id,
    delivery_person_id,
    total_deliveries,
    completed_deliveries,
    returned_deliveries,
    avg_delivery_delay_days,
    avg_delivery_rating,
    total_shipping_cost,
    on_time_deliveries,
    late_deliveries,
    sla_breach_rate,
    return_rate,
    ts_ms
from {{ ref('stg_fact_fulfillment_performance') }}
