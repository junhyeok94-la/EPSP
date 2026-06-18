/*
=============================================================================
* 모델명      : stg_fact_marketing_spend
* 모델 목적   : 광고 집행 성과 팩트 원천 데이터 뷰
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    marketing_id,
    date_id,
    campaign_id,
    channel_id,
    spend_amount,
    impressions,
    clicks,
    conversions,
    revenue_generated,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_fact_marketing_spend') }}
