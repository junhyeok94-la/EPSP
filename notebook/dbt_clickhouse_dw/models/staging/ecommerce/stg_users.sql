/*
=============================================================================
* 모델명      : stg_users
* 모델 목적   : 원천 DB의 users 테이블 CDC 로그를 Staging 영역에 적재 (개인정보 마스킹)
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
=============================================================================
*/

-- [Config]
{{ config(materialized='view') }}

-- [SQL 본문]
select
    lower(hex(sha256(concat(user_id, 'EPSP_SALT_2026')))) as user_id,
    age_group,
    gender,
    location,
    membership_tier,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_users') }}
