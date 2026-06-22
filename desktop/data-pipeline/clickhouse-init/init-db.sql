-- 1. stg_olist_customers (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_olist_customers (
    customer_id String,
    customer_unique_id String,
    customer_zip_code_prefix Int32,
    customer_city String,
    customer_state String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY customer_id 
ORDER BY customer_id;
ALTER TABLE default.stg_olist_customers MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 2. stg_olist_geolocation (ReplacingMergeTree)
-- geolocation은 기본키가 없으므로 정렬 조건(ORDER BY)에 모든 핵심 차원값 주입
CREATE TABLE IF NOT EXISTS default.stg_olist_geolocation (
    geolocation_zip_code_prefix Int32,
    geolocation_lat Float64,
    geolocation_lng Float64,
    geolocation_city String,
    geolocation_state String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
ORDER BY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng);
ALTER TABLE default.stg_olist_geolocation MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 3. stg_olist_orders (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_olist_orders (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp Nullable(DateTime64(3)),
    order_approved_at Nullable(DateTime64(3)),
    order_delivered_carrier_date Nullable(DateTime64(3)),
    order_delivered_customer_date Nullable(DateTime64(3)),
    order_estimated_delivery_date Nullable(DateTime64(3)),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY order_id 
ORDER BY order_id;
ALTER TABLE default.stg_olist_orders MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 4. stg_olist_order_items (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_olist_order_items (
    order_id String,
    order_item_id Int32,
    product_id String,
    seller_id String,
    shipping_limit_date Nullable(DateTime64(3)),
    price Decimal(12, 2),
    freight_value Decimal(12, 2),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY (order_id, order_item_id) 
ORDER BY (order_id, order_item_id);
ALTER TABLE default.stg_olist_order_items MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 5. stg_olist_order_payments (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_olist_order_payments (
    order_id String,
    payment_sequential Int32,
    payment_type String,
    payment_installments Int32,
    payment_value Decimal(12, 2),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY (order_id, payment_sequential) 
ORDER BY (order_id, payment_sequential);
ALTER TABLE default.stg_olist_order_payments MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 6. stg_olist_order_reviews (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_olist_order_reviews (
    review_id String,
    order_id String,
    review_score Int32,
    review_comment_title Nullable(String),
    review_comment_message Nullable(String),
    review_creation_date Nullable(DateTime64(3)),
    review_answer_timestamp Nullable(DateTime64(3)),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY (review_id, order_id) 
ORDER BY (review_id, order_id);
ALTER TABLE default.stg_olist_order_reviews MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 7. stg_olist_products (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_olist_products (
    product_id String,
    product_category_name Nullable(String),
    product_name_lenght Nullable(Int32),
    product_description_lenght Nullable(Int32),
    product_photos_qty Nullable(Int32),
    product_weight_g Nullable(Int32),
    product_length_cm Nullable(Int32),
    product_height_cm Nullable(Int32),
    product_width_cm Nullable(Int32),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY product_id 
ORDER BY product_id;
ALTER TABLE default.stg_olist_products MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 8. stg_olist_sellers (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_olist_sellers (
    seller_id String,
    seller_zip_code_prefix Int32,
    seller_city String,
    seller_state String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY seller_id 
ORDER BY seller_id;
ALTER TABLE default.stg_olist_sellers MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 9. stg_product_category_name_translation (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_product_category_name_translation (
    product_category_name String,
    product_category_name_english String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY product_category_name 
ORDER BY product_category_name;
ALTER TABLE default.stg_product_category_name_translation MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 10. Clickstream 분석용 및 Kafka UI 테스트용 테이블
CREATE TABLE IF NOT EXISTS default.kafka_clickstream (
    message String
) ENGINE = MergeTree()
ORDER BY tuple();
