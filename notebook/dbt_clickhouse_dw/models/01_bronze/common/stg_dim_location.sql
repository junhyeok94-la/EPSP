/*
=============================================================================
* 모델명      : stg_dim_location
* 모델 목적   : 지역 정보 차원 원천 데이터 뷰
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='view'
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
    op,
    ts_ms
from {{ source('clickhouse', 'stg_dim_location') }}
