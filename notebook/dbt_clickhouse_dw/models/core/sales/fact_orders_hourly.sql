/*
=============================================================================
* 모델명      : fact_orders_hourly
* 모델 목적   : 주문 정보 팩트(Fact) 테이블로, 삭제된 데이터를 제외하고 최신 주문 상태 유지
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
* - 2026-06-18 [Agent] : dbt 표준 규격(주석, Hook) 반영 리팩토링 및 디렉토리 구조 변경
=============================================================================
*/

-- [Pre/Post Hooks]
{{ config(
    pre_hook="-- 사전 검증 로직이나 로깅을 여기에 작성"
) }}

-- [Config]
{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ts_ms)',
    primary_key='order_id',
    order_by='order_id',
    unique_key='order_id'
) }}

-- [SQL 본문]
select
    order_id,
    user_id,
    product_id,
    quantity,
    total_price,
    status,
    ts_ms
from {{ ref('stg_orders') }} FINAL
where op != 'd'
{% if is_incremental() %}
  and ts_ms > (select max(ts_ms) from {{ this }})
{% endif %}
