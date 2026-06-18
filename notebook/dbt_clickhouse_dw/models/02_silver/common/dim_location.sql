/*
=============================================================================
* 모델명      : dim_location
* 모델 목적   : 지역 정보 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='location_id',
    primary_key='location_id'
) }}

select
    location_id,
    state,
    city,
    postal_code,
    region,
    latitude,
    longitude,
    area_type,
    location_category,
    ts_ms
from {{ ref('stg_dim_location') }}
