{{ config(materialized='view') }}

select
    product_id,
    product_name,
    category,
    stock_quantity,
    op,
    ts_ms
from {{ source('clickhouse', 'stg_products') }}
