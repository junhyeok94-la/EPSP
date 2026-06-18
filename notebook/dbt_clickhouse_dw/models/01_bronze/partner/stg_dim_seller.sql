/*
=============================================================================
* 모델명      : stg_dim_seller
* 모델 목적   : 입점 판매자(셀러) 차원 원천 데이터 뷰 (날짜 파싱 적용)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성 및 날짜 변환 추가
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    seller_id,
    seller_name,
    email,
    phone_number,
    toDate(join_date) as join_date,
    location_id,
    rating,
    category_focus,
    bank_name,
    bank_account_number,
    ifsc_code,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_dim_seller') }}
