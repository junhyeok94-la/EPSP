-- 1. 주문 스테이징 테이블 생성 (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_orders (
    order_id String,
    user_id String,
    product_id String,
    quantity Int32,
    total_price Decimal(12, 2),
    status String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY order_id 
ORDER BY order_id;

-- 7일 TTL 설정 (toDateTime 형변환 적용)
ALTER TABLE default.stg_orders MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 2. 상품 스테이징 테이블 생성 (ReplacingMergeTree)
CREATE TABLE IF NOT EXISTS default.stg_products (
    product_id String,
    product_name String,
    category String,
    stock_quantity Int32,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY product_id
ORDER BY product_id;

-- 7일 TTL 설정 (toDateTime 형변환 적용)
ALTER TABLE default.stg_products MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

-- 3. Kaggle Ecommerce DW 스테이징 테이블 생성 (ReplacingMergeTree)
-- Debezium 날짜(Date) 전송 포맷(Epoch Days 정수)과의 호환성을 위해 날짜 필드는 Nullable(Int32)로 정의
CREATE TABLE IF NOT EXISTS default.stg_dim_calendar (
    date_id Int32,
    full_date Int32,
    day Int32,
    month Int32,
    month_name String,
    quarter Int32,
    year Int32,
    day_name String,
    is_weekend Bool,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY date_id ORDER BY date_id;
ALTER TABLE default.stg_dim_calendar MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_channel (
    channel_id String,
    channel_name String,
    channel_type String,
    description String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY channel_id ORDER BY channel_id;
ALTER TABLE default.stg_dim_channel MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_campaign (
    campaign_id String,
    campaign_name String,
    campaign_type String,
    channel_id String,
    start_date Nullable(Int32),
    end_date Nullable(Int32),
    budget Nullable(Decimal(15, 2)),
    objective String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY campaign_id ORDER BY campaign_id;
ALTER TABLE default.stg_dim_campaign MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_location (
    location_id String,
    state String,
    city String,
    postal_code Int32,
    region String,
    latitude Nullable(Decimal(10, 6)),
    longitude Nullable(Decimal(10, 6)),
    area_type String,
    location_category String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY location_id ORDER BY location_id;
ALTER TABLE default.stg_dim_location MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_customer (
    customer_id String,
    first_name String,
    last_name String,
    email String,
    phone_number String,
    gender String,
    date_of_birth Nullable(Int32),
    registration_date Nullable(Int32),
    income_bracket String,
    marital_status String,
    location_id String,
    upi_id String,
    credit_card_number String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY customer_id ORDER BY customer_id;
ALTER TABLE default.stg_dim_customer MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_delivery_person (
    delivery_person_id String,
    delivery_person_name String,
    phone_number String,
    gender String,
    date_of_joining Nullable(Int32),
    employment_type String,
    vehicle_type String,
    location_id String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY delivery_person_id ORDER BY delivery_person_id;
ALTER TABLE default.stg_dim_delivery_person MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_fulfillment (
    fulfillment_id String,
    shipping_method String,
    service_level String,
    delivery_sla_days Int32,
    base_shipping_cost Decimal(10, 2),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY fulfillment_id ORDER BY fulfillment_id;
ALTER TABLE default.stg_dim_fulfillment MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_payment (
    payment_id String,
    payment_method String,
    payment_provider String,
    description String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY payment_id ORDER BY payment_id;
ALTER TABLE default.stg_dim_payment MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_product (
    product_id String,
    product_name String,
    brand String,
    category String,
    sub_category String,
    list_price Decimal(15, 2),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY product_id ORDER BY product_id;
ALTER TABLE default.stg_dim_product MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_dim_seller (
    seller_id String,
    seller_name String,
    email String,
    phone_number String,
    join_date Nullable(Int32),
    location_id String,
    rating Nullable(Decimal(3, 2)),
    category_focus String,
    bank_name String,
    bank_account_number String,
    ifsc_code String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY seller_id ORDER BY seller_id;
ALTER TABLE default.stg_dim_seller MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_fact_customer_rfm (
    customer_id String,
    recency_days Int32,
    frequency_orders Int32,
    monetary_value Decimal(15, 4),
    avg_order_value Decimal(15, 4),
    first_purchase_date Nullable(Int32),
    last_purchase_date Nullable(Int32),
    customer_lifetime_days Int32,
    rfm_score Int32,
    customer_segment String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY customer_id ORDER BY customer_id;
ALTER TABLE default.stg_fact_customer_rfm MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_fact_fulfillment_performance (
    date_id Int32,
    fulfillment_id String,
    location_id String,
    delivery_person_id String,
    total_deliveries Int32,
    completed_deliveries Int32,
    returned_deliveries Int32,
    avg_delivery_delay_days Decimal(10, 2),
    avg_delivery_rating Decimal(3, 2),
    total_shipping_cost Decimal(15, 2),
    on_time_deliveries Int32,
    late_deliveries Int32,
    sla_breach_rate Decimal(5, 4),
    return_rate Decimal(5, 4),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY (date_id, fulfillment_id, location_id, delivery_person_id) 
ORDER BY (date_id, fulfillment_id, location_id, delivery_person_id);
ALTER TABLE default.stg_fact_fulfillment_performance MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_fact_marketing_spend (
    marketing_id String,
    date_id Int32,
    campaign_id String,
    channel_id String,
    spend_amount Decimal(15, 2),
    impressions Int32,
    clicks Int32,
    conversions Int32,
    revenue_generated Decimal(15, 2),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY marketing_id ORDER BY marketing_id;
ALTER TABLE default.stg_fact_marketing_spend MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_fact_orders (
    order_line_id String,
    order_id String,
    date_id Int32,
    order_date Int32,
    customer_id String,
    product_id String,
    category String,
    seller_id String,
    seller_rating Nullable(Decimal(3, 2)),
    location_id String,
    delivery_person_id String,
    payment_id String,
    campaign_id String,
    fulfillment_id String,
    quantity Int32,
    unit_price Decimal(15, 2),
    discount_percentage Decimal(5, 2),
    tax_percentage Decimal(5, 2),
    shipping_fee Decimal(15, 2),
    expected_delivery_date Nullable(Int32),
    actual_delivery_date Nullable(Int32),
    delivery_rating Nullable(Decimal(3, 2)),
    order_status String,
    return_flag Bool,
    refund_amount Decimal(15, 2),
    return_date Nullable(Int32),
    customer_rating Nullable(Decimal(3, 2)),
    review_text String,
    sentiment_score Nullable(Decimal(5, 4)),
    delivery_delay_days Int32,
    delivery_sla_days Int32,
    gross_amount Decimal(15, 2),
    discount_amount Decimal(15, 2),
    tax_amount Decimal(15, 2),
    net_amount Decimal(15, 2),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY order_line_id ORDER BY order_line_id;
ALTER TABLE default.stg_fact_orders MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS default.stg_fact_returns (
    order_line_id String,
    order_id String,
    date_id Int32,
    customer_id String,
    product_id String,
    location_id String,
    payment_id String,
    fulfillment_id String,
    quantity Int32,
    unit_price Decimal(15, 2),
    refund_amount Decimal(15, 2),
    review_text String,
    days_to_return Int32,
    restocking_fee Decimal(15, 2),
    customer_rating Nullable(Decimal(3, 2)),
    sentiment_score Nullable(Decimal(5, 4)),
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY order_line_id ORDER BY order_line_id;
ALTER TABLE default.stg_fact_returns MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;
