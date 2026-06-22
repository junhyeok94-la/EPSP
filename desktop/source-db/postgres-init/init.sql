-- 1. Olist E-Commerce OLTP 테이블 생성
CREATE TABLE IF NOT EXISTS olist_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_geolocation (
    geolocation_zip_code_prefix INT NOT NULL,
    geolocation_lat DOUBLE PRECISION NOT NULL,
    geolocation_lng DOUBLE PRECISION NOT NULL,
    geolocation_city VARCHAR(100) NOT NULL,
    geolocation_state VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_order_items (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price DECIMAL(12, 2) NOT NULL,
    freight_value DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS olist_order_payments (
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS olist_order_reviews (
    review_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    review_score INT NOT NULL,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (review_id, order_id)
);

CREATE TABLE IF NOT EXISTS olist_products (
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

CREATE TABLE IF NOT EXISTS olist_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100) NOT NULL
);

-- 2. CDC(Debezium) 감지를 위한 REPLICA IDENTITY FULL 설정
ALTER TABLE olist_customers REPLICA IDENTITY FULL;
ALTER TABLE olist_geolocation REPLICA IDENTITY FULL;
ALTER TABLE olist_orders REPLICA IDENTITY FULL;
ALTER TABLE olist_order_items REPLICA IDENTITY FULL;
ALTER TABLE olist_order_payments REPLICA IDENTITY FULL;
ALTER TABLE olist_order_reviews REPLICA IDENTITY FULL;
ALTER TABLE olist_products REPLICA IDENTITY FULL;
ALTER TABLE olist_sellers REPLICA IDENTITY FULL;
ALTER TABLE product_category_name_translation REPLICA IDENTITY FULL;

-- 3. CSV 데이터 벌크 로드 (NULL AS '' 옵션 추가로 결측치 처리 안전화)
COPY olist_customers FROM '/kaggle_data/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER NULL AS '';
COPY olist_geolocation FROM '/kaggle_data/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER NULL AS '';
COPY olist_orders FROM '/kaggle_data/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER NULL AS '';
COPY olist_order_items FROM '/kaggle_data/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER NULL AS '';
COPY olist_order_payments FROM '/kaggle_data/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER NULL AS '';
COPY olist_order_reviews FROM '/kaggle_data/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER NULL AS '';
COPY olist_products FROM '/kaggle_data/olist_products_dataset.csv' DELIMITER ',' CSV HEADER NULL AS '';
COPY olist_sellers FROM '/kaggle_data/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER NULL AS '';
COPY product_category_name_translation FROM '/kaggle_data/product_category_name_translation.csv' DELIMITER ',' CSV HEADER NULL AS '';
