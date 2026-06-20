/*
=============================================================================
* 모델명      : stg_dim_channel
* 모델 목적   : 마케팅 및 유입 채널 차원 원천 데이터 뷰
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    channel_id,
    channel_name,
    channel_type,
    description,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse', 'stg_dim_channel') }}
