/*
=============================================================================
* 모델명      : stg_category_translation
* 모델 목적   : 상품 카테고리 한국어/포르투갈어 영문 번역 맵핑 데이터 정제 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 KST 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    product_category_name,
    product_category_name_english,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_product_category_name_translation') }}
