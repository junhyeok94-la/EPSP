/*
=============================================================================
* 모델명      : stg_order_payments
* 모델 목적   : Olist 주문 결제 정보 원천 데이터 정제 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 KST 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_olist_order_payments') }}
