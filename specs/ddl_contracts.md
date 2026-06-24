# DDL Contracts Specification

본 문서는 EPSP(E-Commerce Pipeline with Segmented Platforms) 프로젝트의 데이터 원천(PostgreSQL) 및 데이터 레이크하우스 랜딩존(ClickHouse) 간의 데이터 스키마 및 물리적 DDL 계약 명세서이다.

---

## 1. Source: PostgreSQL Olist 운영 DB

PostgreSQL은 Olist CSV를 적재하는 운영 원천 DB 역할을 한다. 모든 테이블은 Debezium CDC 수집을 위해 `REPLICA IDENTITY FULL`을 적용한다.

### 1.1 Olist 원천 DDL 스키마
```sql
CREATE TABLE olist_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state VARCHAR(20) NOT NULL
);

CREATE TABLE olist_geolocation (
    geolocation_zip_code_prefix INT NOT NULL,
    geolocation_lat DOUBLE PRECISION NOT NULL,
    geolocation_lng DOUBLE PRECISION NOT NULL,
    geolocation_city VARCHAR(100) NOT NULL,
    geolocation_state VARCHAR(20) NOT NULL
);

CREATE TABLE olist_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP NOT NULL
);

CREATE TABLE olist_order_items (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price DECIMAL(12, 2) NOT NULL,
    freight_value DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE olist_order_payments (
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE olist_order_reviews (
    review_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    review_score INT NOT NULL,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (review_id, order_id)
);

CREATE TABLE olist_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE olist_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state VARCHAR(20) NOT NULL
);

CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100) NOT NULL
);
```

---

## 2. Landing/Raw: ClickHouse `default.stg_olist_*`

ClickHouse의 `default` DB는 Kafka Connect가 적재하는 CDC landing zone이다.

### 2.1 ClickHouse Landing ReplacingMergeTree DDL 예시
Debezium CDC 특성상 무수히 발생하는 중복 변경 로그(op = c, u)를 수용하고 자동 최신화하기 위해 동일 PK 기준 최신 타임스탬프(ts_ms)만 백그라운드에서 병합하도록 설정합니다.

```sql
CREATE TABLE default.stg_olist_orders (
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

-- 데이터 생명주기 및 디스크 통제 (TTL)
ALTER TABLE default.stg_olist_orders MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;
```

### 2.2 Landing 설계 표준
* **엔진**: `ReplacingMergeTree(ts_ms)`
* **PK/ORDER BY**: 원천 PK 또는 복합 PK (결합 튜플)
* **공통 메타 컬럼**: CDC 수집 시 주입되는 `op`(operation 종류) 및 `ts_ms`(타임스탬프 밀리초)
* **TTL**: 로컬 디스크 리소스 방지를 위해 `toDateTime(ts_ms) + INTERVAL 7 DAY` 의무 적용
