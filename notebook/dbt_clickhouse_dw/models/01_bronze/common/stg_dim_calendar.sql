/*
=============================================================================
* 모델명      : stg_dim_calendar
* 모델 목적   : 달력 차원 원천 데이터 뷰 (날짜 파싱 적용)
* 
* [수정 이력]
* - 2026-06-18 [Antigravity] : 최초 생성 및 날짜 변환 추가
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    date_id,
    toDate(full_date) as full_date,
    day,
    month,
    month_name,
    quarter,
    year,
    day_name,
    is_weekend,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_dim_calendar') }}
