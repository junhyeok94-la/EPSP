/*
=============================================================================
* 모델명      : stg_dim_fulfillment
* 모델 목적   : 물류 풀필먼트 및 배송 수단 차원 원천 데이터 뷰
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    fulfillment_id,
    shipping_method,
    service_level,
    delivery_sla_days,
    base_shipping_cost,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_dim_fulfillment') }}
