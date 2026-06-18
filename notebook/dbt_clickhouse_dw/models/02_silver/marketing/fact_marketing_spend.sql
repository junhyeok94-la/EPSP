/*
=============================================================================
* 모델명      : fact_marketing_spend
* 모델 목적   : 마케팅 성과 팩트 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='marketing_id',
    primary_key='marketing_id'
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
    ts_ms
from {{ ref('stg_fact_marketing_spend') }}
