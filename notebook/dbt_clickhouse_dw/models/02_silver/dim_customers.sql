/*
=============================================================================
* 모델명      : dim_customers
* 모델 목적   : 고객 마스터 차원 테이블 (Silver Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 지오로케이션 결합, ReplacingMergeTree 설정
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    primary_key='customer_id',
    order_by='customer_id'
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
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    coalesce(nullif(c.customer_city, ''), geo.city) as customer_city,
    coalesce(nullif(c.customer_state, ''), geo.state) as customer_state,
    geo.lat as latitude,
    geo.lng as longitude,
    c.op,
    c.ts_ms
from {{ ref('stg_customers') }} as c
left join geo on c.customer_zip_code_prefix = geo.geolocation_zip_code_prefix
