/*
=============================================================================
* 모델명      : stg_payments
* 모델 목적   : 원천 DB의 payments 테이블 CDC 로그를 Staging 영역에 적재
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
=============================================================================
*/

-- [Config]
{{ config(materialized='view') }}

-- [SQL 본문]
select
    payment_id,
    order_id,
    payment_method,
    amount,
    status,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_payments') }}
