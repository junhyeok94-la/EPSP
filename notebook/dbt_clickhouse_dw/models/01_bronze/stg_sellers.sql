/*
=============================================================================
* 모델명      : stg_sellers
* 모델 목적   : Olist 판매자 정보 원천 데이터 정제 및 가명화 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 seller_id 가명화, KST 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    lower(hex(SHA256(concat(seller_id, 'epsp_secure_salt')))) as seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_olist_sellers') }}
