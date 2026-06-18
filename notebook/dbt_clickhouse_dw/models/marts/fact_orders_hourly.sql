{{
  config(
    materialized='incremental',
    engine='ReplacingMergeTree(ts_ms)',
    primary_key='order_id',
    order_by='order_id',
    unique_key='order_id'
  )
}}

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
