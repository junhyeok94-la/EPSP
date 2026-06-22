/*
=============================================================================
* 모델명      : stg_geolocation
* 모델 목적   : Olist 지오로케이션 원천 데이터 정제 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 KST 타임존 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_olist_geolocation') }}
