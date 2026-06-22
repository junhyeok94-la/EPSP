/*
=============================================================================
* 모델명      : stg_customers
* 모델 목적   : Olist 고객 원천 데이터 정제 및 가명화 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 SHA-256 가명화, KST 타임존 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    lower(hex(sha256(concat(customer_id, 'epsp_secure_salt')))) as customer_id,
    lower(hex(sha256(concat(customer_unique_id, 'epsp_secure_salt')))) as customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_olist_customers') }}
