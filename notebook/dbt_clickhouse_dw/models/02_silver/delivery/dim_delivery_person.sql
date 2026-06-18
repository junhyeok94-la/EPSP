/*
=============================================================================
* 모델명      : dim_delivery_person
* 모델 목적   : 배송 기사 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='delivery_person_id',
    primary_key='delivery_person_id'
) }}

select
    delivery_person_id,
    delivery_person_name,
    phone_number,
    gender,
    date_of_joining,
    employment_type,
    vehicle_type,
    location_id,
    ts_ms
from {{ ref('stg_dim_delivery_person') }}
