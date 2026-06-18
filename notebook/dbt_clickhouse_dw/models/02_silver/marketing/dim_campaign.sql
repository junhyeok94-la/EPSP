/*
=============================================================================
* 모델명      : dim_campaign
* 모델 목적   : 마케팅 캠페인 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='campaign_id',
    primary_key='campaign_id'
) }}

select
    campaign_id,
    campaign_name,
    campaign_type,
    channel_id,
    start_date,
    end_date,
    budget,
    objective,
    ts_ms
from {{ ref('stg_dim_campaign') }}
