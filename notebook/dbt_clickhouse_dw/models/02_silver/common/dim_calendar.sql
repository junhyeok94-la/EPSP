/*
=============================================================================
* 모델명      : dim_calendar
* 모델 목적   : 달력 차원 테이블 (ReplacingMergeTree 기반 중복 제거)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성
=============================================================================
*/

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(ts_ms)',
    order_by='date_id',
    primary_key='date_id'
) }}

select
    date_id,
    full_date,
    day,
    month,
    month_name,
    quarter,
    year,
    day_name,
    is_weekend,
    ts_ms
from {{ ref('stg_dim_calendar') }}
