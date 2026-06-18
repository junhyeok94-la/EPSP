/*
=============================================================================
* 모델명      : stg_dim_campaign
* 모델 목적   : 마케팅 캠페인 차원 원천 데이터 뷰 (날짜 파싱 적용)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성 및 날짜 변환 추가
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    campaign_id,
    campaign_name,
    campaign_type,
    channel_id,
    toDate(start_date) as start_date,
    toDate(end_date) as end_date,
    budget,
    objective,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_dim_campaign') }}
