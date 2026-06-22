/*
=============================================================================
* 모델명      : dim_products
* 모델 목적   : 상품 마스터 차원 테이블 (Silver Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 카테고리 번역 매핑 결합, ReplacingMergeTree 설정
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    primary_key='product_id',
    order_by='product_id'
) }}

select
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.op,
    p.ts_ms
from {{ ref('stg_products') }} as p
left join {{ ref('stg_category_translation') }} as t on p.product_category_name = t.product_category_name
