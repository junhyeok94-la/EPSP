{{
  config(
    materialized='incremental',
    engine='ReplacingMergeTree(ts_ms)',
    primary_key='product_id',
    order_by='product_id',
    unique_key='product_id'
  )
}}

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
