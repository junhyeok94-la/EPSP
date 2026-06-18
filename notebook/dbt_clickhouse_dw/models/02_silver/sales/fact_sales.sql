/*
=============================================================================
* 모델명      : fact_sales
* 모델 목적   : 주문과 결제 정보를 조인한 정규화된 팩트 테이블
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
=============================================================================
*/

-- [Config]
{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ts_ms)',
    primary_key='order_id',
    order_by='order_id'
) }}

-- [SQL 본문]
with orders as (
    select * from {{ ref('fact_orders_hourly') }} FINAL
),
payments as (
    select * from {{ ref('stg_payments') }} FINAL
    where op != 'd'
)
select
    o.order_id,
    o.user_id,
    o.product_id,
    o.quantity,
    o.total_price,
    o.status as order_status,
    p.payment_method,
    p.status as payment_status,
    greatest(o.ts_ms, p.ts_ms) as ts_ms
from orders o
left join payments p on o.order_id = p.order_id
{% if is_incremental() %}
where greatest(o.ts_ms, p.ts_ms) > (select max(ts_ms) from {{ this }})
{% endif %}
