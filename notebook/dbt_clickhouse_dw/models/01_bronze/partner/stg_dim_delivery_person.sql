/*
=============================================================================
* 모델명      : stg_dim_delivery_person
* 모델 목적   : 배송 기사 차원 원천 데이터 뷰 (날짜 파싱 적용)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성 및 날짜 변환 추가
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    delivery_person_id,
    delivery_person_name,
    phone_number,
    gender,
    toDate(date_of_joining) as date_of_joining,
    employment_type,
    vehicle_type,
    location_id,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse', 'stg_dim_delivery_person') }}
