/*
=============================================================================
* 모델명      : dim_products
* 모델 목적   : 상품 정보 차원(Dimension) 테이블로, 삭제된 레코드를 제외하고 최신 상태를 유지
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
    primary_key='product_id',
    order_by='product_id',
    unique_key='product_id'
) }}

-- [SQL 본문]
select
    product_id,
    product_name,
    category,
    stock_quantity,
    ts_ms
from {{ ref('stg_products') }} FINAL
where op != 'd'
{% if is_incremental() %}
  and ts_ms > (select max(ts_ms) from {{ this }})
{% endif %}
