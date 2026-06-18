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

-- 7일 TTL 설정
ALTER TABLE default.stg_orders MODIFY TTL ts_ms + INTERVAL 7 DAY;

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

-- 7일 TTL 설정
ALTER TABLE default.stg_products MODIFY TTL ts_ms + INTERVAL 7 DAY;
