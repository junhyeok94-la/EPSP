/*
=============================================================================
* 모델명      : dim_product
* 모델 목적   : 상품 정보 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='product_id',
    primary_key='product_id'
) }}

select
    product_id,
    product_name,
    brand,
    category,
    sub_category,
    list_price,
    ts_ms
from {{ ref('stg_dim_product') }}
