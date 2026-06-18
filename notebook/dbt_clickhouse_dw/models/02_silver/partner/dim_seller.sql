/*
=============================================================================
* 모델명      : dim_seller
* 모델 목적   : 판매자 정보 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='seller_id',
    primary_key='seller_id'
) }}

select
    seller_id,
    seller_name,
    email,
    phone_number,
    join_date,
    location_id,
    rating,
    category_focus,
    bank_name,
    bank_account_number,
    ifsc_code,
    ts_ms
from {{ ref('stg_dim_seller') }}
