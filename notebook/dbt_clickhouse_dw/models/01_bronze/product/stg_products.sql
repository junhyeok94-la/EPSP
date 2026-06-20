/*
=============================================================================
* 모델명      : stg_products
* 모델 목적   : 원천 DB의 products 테이블 CDC 로그를 Staging 영역에 적재
* 
* [수정 이력]
* - 2026-06-18 [Agent] : 최초 생성
* - 2026-06-18 [Agent] : dbt 표준 규격(주석, Hook) 반영 리팩토링 및 디렉토리 구조 변경
=============================================================================
*/

-- [Config]
{{ config(materialized='view') }}

-- [SQL 본문]
select
    product_id,
    product_name,
    category,
    stock_quantity,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse', 'stg_products') }}
