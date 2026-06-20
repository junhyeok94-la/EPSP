/*
=============================================================================
* 모델명      : stg_dim_payment
* 모델 목적   : 결제 수단 및 대행사 차원 원천 데이터 뷰
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    payment_id,
    payment_method,
    payment_provider,
    description,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse', 'stg_dim_payment') }}
