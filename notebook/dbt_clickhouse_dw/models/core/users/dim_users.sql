/*
=============================================================================
* 모델명      : dim_users
* 모델 목적   : 고객 정보 차원(Dimension) 테이블로, 최신 상태 유지
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
=============================================================================
*/

-- [Config]
{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ts_ms)',
    primary_key='user_id',
    order_by='user_id'
) }}

-- [SQL 본문]
select
    user_id,
    age_group,
    gender,
    location,
    membership_tier,
    ts_ms
from {{ ref('stg_users') }} FINAL
where op != 'd'
{% if is_incremental() %}
  and ts_ms > (select max(ts_ms) from {{ this }})
{% endif %}
