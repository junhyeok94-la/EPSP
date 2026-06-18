/*
=============================================================================
* 모델명      : dim_fulfillment
* 모델 목적   : 물류 풀필먼트 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='fulfillment_id',
    primary_key='fulfillment_id'
) }}

select
    fulfillment_id,
    shipping_method,
    service_level,
    delivery_sla_days,
    base_shipping_cost,
    ts_ms
from {{ ref('stg_dim_fulfillment') }}
