/*
=============================================================================
* 모델명      : stg_fact_fulfillment_performance
* 모델 목적   : 물류 배송 성과 팩트 원천 데이터 뷰
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='view'
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
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse', 'stg_fact_fulfillment_performance') }}
