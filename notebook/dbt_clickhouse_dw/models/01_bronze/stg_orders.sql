/*
=============================================================================
* 모델명      : stg_orders
* 모델 목적   : Olist 주문 원천 데이터 정제 및 가명화 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 customer_id 가명화, KST 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    order_id,
    lower(hex(SHA256(concat(customer_id, 'epsp_secure_salt')))) as customer_id,
    order_status,
    toTimeZone(fromUnixTimestamp64Micro(order_purchase_timestamp), 'Asia/Seoul') as order_purchase_timestamp,
    toTimeZone(fromUnixTimestamp64Micro(order_approved_at), 'Asia/Seoul') as order_approved_at,
    toTimeZone(fromUnixTimestamp64Micro(order_delivered_carrier_date), 'Asia/Seoul') as order_delivered_carrier_date,
    toTimeZone(fromUnixTimestamp64Micro(order_delivered_customer_date), 'Asia/Seoul') as order_delivered_customer_date,
    toTimeZone(fromUnixTimestamp64Micro(order_estimated_delivery_date), 'Asia/Seoul') as order_estimated_delivery_date,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_olist_orders') }}


