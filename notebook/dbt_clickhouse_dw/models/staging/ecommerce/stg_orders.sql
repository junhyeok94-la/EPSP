/*
=============================================================================
* 모델명      : stg_orders
* 모델 목적   : 원천 DB의 orders 테이블 CDC 로그를 Staging 영역에 적재 (user_id SHA-256 마스킹 포함)
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
* - 2026-06-18 [Agent] : dbt 표준 규격(주석, Hook) 반영 리팩토링 및 디렉토리 구조 변경
=============================================================================
*/

-- [Pre/Post Hooks]
{{ config(
    pre_hook="-- 사전 로깅 예시: SELECT 'stg_orders 실행 시작'"
) }}

-- [Config]
{{ config(materialized='view') }}

-- [SQL 본문]
select
    order_id,
    lower(hex(sha256(concat(user_id, 'EPSP_SALT_2026')))) as user_id,
    product_id,
    quantity,
    total_price,
    status,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_orders') }}
