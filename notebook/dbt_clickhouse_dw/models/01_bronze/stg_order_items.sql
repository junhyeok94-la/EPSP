/*
=============================================================================
* 모델명      : stg_order_items
* 모델 목적   : Olist 주문 상세 품목 원천 데이터 정제 및 가명화 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 seller_id 가명화, KST 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    order_id,
    order_item_id,
    product_id,
    lower(hex(SHA256(concat(seller_id, 'epsp_secure_salt')))) as seller_id,
    toTimeZone(fromUnixTimestamp64Micro(shipping_limit_date), 'Asia/Seoul') as shipping_limit_date,
    price,
    freight_value,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_olist_order_items') }}


