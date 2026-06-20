/*
=============================================================================
* 모델명      : mart_daily_sales_wide
* 모델 목적   : 매출 현황 분석용 초광폭(OBT) 테이블 (일 단위/차원 결합)
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
=============================================================================
*/

-- [Config]
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='sales_date'
) }}

-- [SQL 본문]
with sales as (
    select * from {{ ref('fact_sales') }} FINAL
),
users as (
    select * from {{ ref('dim_users') }} FINAL
),
products as (
    select * from {{ ref('dim_products') }} FINAL
)
select
    toDate(s.ts_ms) as sales_date,
    u.age_group,
    u.gender,
    u.location,
    u.membership_tier,
    p.product_name,
    p.category,
    s.payment_method,
    s.order_status,
    count(s.order_id) as total_orders,
    sum(s.quantity) as total_quantity_sold,
    sum(s.total_price) as total_revenue
from sales s
left join users u on s.user_id = u.user_id
left join products p on s.product_id = p.product_id
group by 
    sales_date,
    u.age_group,
    u.gender,
    u.location,
    u.membership_tier,
    p.product_name,
    p.category,
    s.payment_method,
    s.order_status
