/*
=============================================================================
* 모델명      : dim_sellers
* 모델 목적   : 판매자(셀러) 마스터 차원 테이블 (Silver Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 지오로케이션 결합, ReplacingMergeTree 설정
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    primary_key='seller_id',
    order_by='seller_id'
) }}

with geo as (
    select
        geolocation_zip_code_prefix,
        avg(geolocation_lat) as lat,
        avg(geolocation_lng) as lng,
        any(geolocation_city) as city,
        any(geolocation_state) as state
    from {{ ref('stg_geolocation') }}
    group by geolocation_zip_code_prefix
)

select
    s.seller_id,
    s.seller_zip_code_prefix,
    coalesce(nullif(s.seller_city, ''), geo.city) as seller_city,
    coalesce(nullif(s.seller_state, ''), geo.state) as seller_state,
    geo.lat as latitude,
    geo.lng as longitude,
    s.op,
    s.ts_ms
from {{ ref('stg_sellers') }} as s
left join geo on s.seller_zip_code_prefix = geo.geolocation_zip_code_prefix
