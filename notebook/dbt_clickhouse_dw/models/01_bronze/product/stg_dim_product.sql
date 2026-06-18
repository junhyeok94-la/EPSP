/*
=============================================================================
* 모델명      : stg_dim_product
* 모델 목적   : 상품 마스터 차원 원천 데이터 뷰
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    product_id,
    product_name,
    brand,
    category,
    sub_category,
    list_price,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_dim_product') }}
