/*
=============================================================================
* 모델명      : dim_channel
* 모델 목적   : 마케팅 채널 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='channel_id',
    primary_key='channel_id'
) }}

select
    channel_id,
    channel_name,
    channel_type,
    description,
    ts_ms
from {{ ref('stg_dim_channel') }}
