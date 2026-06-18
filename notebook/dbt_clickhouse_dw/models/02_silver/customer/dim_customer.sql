/*
=============================================================================
* 모델명      : dim_customer
* 모델 목적   : 고객 정보 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='customer_id',
    primary_key='customer_id'
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    gender,
    date_of_birth,
    registration_date,
    income_bracket,
    marital_status,
    location_id,
    upi_id,
    credit_card_number,
    ts_ms
from {{ ref('stg_dim_customer') }}
