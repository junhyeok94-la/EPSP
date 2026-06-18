/*
=============================================================================
* 모델명      : stg_events
* 모델 목적   : Kafka를 통해 직수집된 Clickstream 행동 로그 적재
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
=============================================================================
*/

-- [Config]
{{ config(materialized='view') }}

-- [SQL 본문]
select
    JSONExtractString(message, 'event_id') as event_id,
    lower(hex(sha256(concat(JSONExtractString(message, 'user_id'), 'EPSP_SALT_2026')))) as user_id,
    JSONExtractString(message, 'session_id') as session_id,
    JSONExtractString(message, 'event_type') as event_type,
    JSONExtractString(message, 'product_id') as product_id,
    toDateTime(JSONExtractString(message, 'event_time')) as event_time
from {{ source('clickhouse', 'kafka_clickstream') }}
