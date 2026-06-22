/*
=============================================================================
* 모델명      : mart_customer_rfm
* 모델 목적   : 고객 RFM 세그먼테이션 분석 마트 (Gold Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 FINAL 키워드 기반 ReplacingMergeTree 집계 적용
=============================================================================
*/

{{ config(
    materialized='table',
    order_by='customer_unique_id'
) }}

with customer_orders as (
    select
        c.customer_unique_id as customer_unique_id,
        o.order_id as order_id,
        o.order_purchase_timestamp as order_purchase_timestamp,
        o.price as price
    from {{ ref('fact_orders') }} FINAL as o
    inner join {{ ref('dim_customers') }} FINAL as c on o.customer_id = c.customer_id
),

max_date_ref as (
    select max(order_purchase_timestamp) as max_date from customer_orders
),

rfm_raw as (
    select
        co.customer_unique_id as customer_unique_id,
        dateDiff('day', max(co.order_purchase_timestamp), (select max_date from max_date_ref)) as recency,
        count(distinct co.order_id) as frequency,
        sum(co.price) as monetary
    from customer_orders as co
    group by co.customer_unique_id
)

select
    customer_unique_id,
    recency,
    frequency,
    monetary,
    case 
        when frequency >= 4 and monetary >= 300 then 'VIP'
        when frequency >= 2 and recency <= 60 then 'Loyal'
        when recency >= 180 then 'Churned'
        when recency >= 90 then 'At Risk'
        else 'General'
    end as customer_segment
from rfm_raw
