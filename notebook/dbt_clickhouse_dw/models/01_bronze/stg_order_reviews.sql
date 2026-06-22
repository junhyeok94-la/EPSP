/*
=============================================================================
* 모델명      : stg_order_reviews
* 모델 목적   : Olist 주문 리뷰 정보 원천 데이터 정제 (Bronze Layer)
* 
* [수정 이력]
* - 2026-06-22 [Antigravity] : 최초 생성 및 KST 변환 적용
=============================================================================
*/

{{ config(
    materialized='view'
) }}

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    toTimeZone(fromUnixTimestamp64Micro(review_creation_date), 'Asia/Seoul') as review_creation_date,
    toTimeZone(fromUnixTimestamp64Micro(review_answer_timestamp), 'Asia/Seoul') as review_answer_timestamp,
    op,
    toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms
from {{ source('clickhouse_source', 'stg_olist_order_reviews') }}


