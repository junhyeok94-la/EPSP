/*
=============================================================================
* 모델명      : stg_products
* 모델 목적   : Olist 상품 정보 원천 데이터 정제 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 KST 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_olist_products') }}
