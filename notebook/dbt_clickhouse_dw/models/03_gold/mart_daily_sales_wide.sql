/*
=============================================================================
* 모델명      : mart_daily_sales_wide
* 모델 목적   : 일별 매출 및 핵심 물류/리뷰 지표 분석 마트 (Gold Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 FINAL 키워드 기반 ReplacingMergeTree 집계 적용
=============================================================================
*/

{{ config(
    materialized='table',
    order_by='order_date'
) }}

select
    toDate(order_purchase_timestamp) as order_date,
    count(distinct order_id) as total_orders,
    count(order_item_id) as total_items_sold,
    sum(price) as total_price_amount,
    sum(freight_value) as total_freight_amount,
    (total_price_amount + total_freight_amount) as total_gross_revenue,
    avg(review_score) as avg_review_score,
    avg(case 
        when order_status = 'delivered' and isNotNull(order_delivered_customer_date) 
        then dateDiff('day', order_purchase_timestamp, order_delivered_customer_date) 
        else null 
    end) as avg_delivery_days
from {{ ref('fact_orders') }} FINAL
group by order_date
