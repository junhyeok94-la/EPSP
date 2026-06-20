/*
=============================================================================
* 모델명      : stg_dim_customer
* 모델 목적   : 고객 마스터 차원 원천 데이터 뷰 (customer_id 마스킹 및 날짜 파싱 적용)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성 및 가명화, 날짜 변환 추가
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    hex(SHA256(concat(customer_id, 'my_secure_salt_123!'))) as customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    gender,
    toDate(date_of_birth) as date_of_birth,
    toDate(registration_date) as registration_date,
    income_bracket,
    marital_status,
    location_id,
    upi_id,
    credit_card_number,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse', 'stg_dim_customer') }}
