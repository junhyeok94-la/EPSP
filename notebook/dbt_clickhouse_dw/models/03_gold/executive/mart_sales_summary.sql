/*
=============================================================================
* 모델명      : mart_sales_summary
* 모델 목적   : 이커머스 매출 및 마케팅 성과 분석을 위한 OBT (One Big Table) 마트
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='order_line_id',
    primary_key='order_line_id'
) }}

select
    -- 주문 식별 정보
    o.order_line_id as order_line_id,
    o.order_id,
    o.order_date,
    o.order_status,
    o.quantity,
    o.unit_price,
    o.discount_percentage,
    o.tax_percentage,
    o.shipping_fee,
    o.gross_amount,
    o.discount_amount,
    o.tax_amount,
    o.net_amount,
    o.delivery_delay_days,
    o.return_flag,
    o.refund_amount,
    o.customer_rating as order_customer_rating,

    -- 상품 정보
    p.product_name,
    p.brand as product_brand,
    p.category as product_category,
    p.sub_category as product_sub_category,
    p.list_price as product_list_price,

    -- 고객 세그먼트 정보 (가명화된 customer_id 포함)
    o.customer_id,
    c.gender as customer_gender,
    c.income_bracket as customer_income_bracket,
    c.marital_status as customer_marital_status,

    -- 고객 거주지 지역 정보
    loc.state as customer_state,
    loc.city as customer_city,
    loc.region as customer_region,
    loc.area_type as customer_area_type,

    -- 일자별 달력 정보
    cal.year as order_year,
    cal.quarter as order_quarter,
    cal.month as order_month,
    cal.month_name as order_month_name,
    cal.day_name as order_day_name,
    cal.is_weekend as order_is_weekend,

    -- 마케팅 및 캠페인 유입 정보
    camp.campaign_name,
    camp.campaign_type,
    camp.budget as campaign_budget,
    chan.channel_name,
    chan.channel_type

from (select * from {{ ref('fact_orders') }} FINAL) as o
left join (select * from {{ ref('dim_product') }} FINAL) as p 
    on o.product_id = p.product_id
left join (select * from {{ ref('dim_customer') }} FINAL) as c 
    on o.customer_id = c.customer_id
left join (select * from {{ ref('dim_location') }} FINAL) as loc 
    on o.location_id = loc.location_id
left join (select * from {{ ref('dim_calendar') }} FINAL) as cal 
    on o.date_id = cal.date_id
left join (select * from {{ ref('dim_campaign') }} FINAL) as camp 
    on o.campaign_id = camp.campaign_id
left join (select * from {{ ref('dim_channel') }} FINAL) as chan 
    on camp.channel_id = chan.channel_id
