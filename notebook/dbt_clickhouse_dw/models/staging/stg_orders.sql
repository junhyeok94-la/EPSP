{{ config(materialized='view') }}

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
