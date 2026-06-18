-- 1. 테이블 생성
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    age_group VARCHAR(20),
    gender VARCHAR(10),
    location VARCHAR(100),
    membership_tier VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    stock_quantity INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(user_id),
    product_id VARCHAR(50) REFERENCES products(product_id),
    quantity INT NOT NULL,
    total_price DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'CREATED', 'CANCELLED', 'RETURNED'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    payment_method VARCHAR(50) NOT NULL, -- 'CREDIT_CARD', 'POINT', 'BANK_TRANSFER'
    amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'REFUNDED'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. CDC(Debezium)를 위한 REPLICA IDENTITY FULL 설정 (업데이트/삭제 시 이전 값 추적용)
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;

-- 3. Kaggle Ecommerce DW 테이블 및 코멘트 생성 (AI 에이전트 자연어 쿼리 최적화 표준)
CREATE TABLE IF NOT EXISTS dim_calendar (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
COMMENT ON TABLE dim_calendar IS '달력 차원 테이블. 일자별 연도, 분기, 월, 주말 여부 등의 메타데이터를 저장';
COMMENT ON COLUMN dim_calendar.date_id IS '날짜 고유 ID (YYYYMMDD 형식의 정수)';
COMMENT ON COLUMN dim_calendar.full_date IS '날짜 (YYYY-MM-DD)';
COMMENT ON COLUMN dim_calendar.is_weekend IS '주말 여부 (True: 주말, False: 평일)';

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id VARCHAR(50) PRIMARY KEY,
    channel_name VARCHAR(100) NOT NULL,
    channel_type VARCHAR(50) NOT NULL,
    description TEXT
);
COMMENT ON TABLE dim_channel IS '마케팅 및 유입 채널 차원 테이블';
COMMENT ON COLUMN dim_channel.channel_id IS '채널 고유 ID';
COMMENT ON COLUMN dim_channel.channel_name IS '채널명 (예: Organic, Paid Search, Social)';
COMMENT ON COLUMN dim_channel.channel_type IS '채널 유형 (예: Digital, Offline)';

CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_type VARCHAR(50) NOT NULL,
    channel_id VARCHAR(50) REFERENCES dim_channel(channel_id),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(15, 2),
    objective VARCHAR(100)
);
COMMENT ON TABLE dim_campaign IS '마케팅 캠페인 차원 테이블';
COMMENT ON COLUMN dim_campaign.campaign_id IS '캠페인 고유 ID';
COMMENT ON COLUMN dim_campaign.channel_id IS '연계된 마케팅 채널 ID (dim_channel 참조)';
COMMENT ON COLUMN dim_campaign.budget IS '캠페인 예산 금액';

CREATE TABLE IF NOT EXISTS dim_location (
    location_id VARCHAR(50) PRIMARY KEY,
    state VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    postal_code INT NOT NULL,
    region VARCHAR(50),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    area_type VARCHAR(50),
    location_category VARCHAR(50)
);
COMMENT ON TABLE dim_location IS '지역 정보 차원 테이블. 시도, 구군, 우편번호 및 위경도를 관리';
COMMENT ON COLUMN dim_location.location_id IS '지역 고유 ID';
COMMENT ON COLUMN dim_location.region IS '권역구분 (예: East, West, North, South)';

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    gender VARCHAR(10),
    date_of_birth DATE,
    registration_date DATE,
    income_bracket VARCHAR(50),
    marital_status VARCHAR(50),
    location_id VARCHAR(50) REFERENCES dim_location(location_id),
    upi_id VARCHAR(100),
    credit_card_number VARCHAR(100)
);
COMMENT ON TABLE dim_customer IS '고객 마스터 차원 테이블. 인적사항 및 소득수준, 거주지역 관리';
COMMENT ON COLUMN dim_customer.customer_id IS '고객 고유 ID';
COMMENT ON COLUMN dim_customer.location_id IS '고객 거주지 ID (dim_location 참조)';

CREATE TABLE IF NOT EXISTS dim_delivery_person (
    delivery_person_id VARCHAR(50) PRIMARY KEY,
    delivery_person_name VARCHAR(255) NOT NULL,
    phone_number VARCHAR(50),
    gender VARCHAR(20),
    date_of_joining DATE,
    employment_type VARCHAR(50),
    vehicle_type VARCHAR(50),
    location_id VARCHAR(50) REFERENCES dim_location(location_id)
);
COMMENT ON TABLE dim_delivery_person IS '배송 기사 차원 테이블';
COMMENT ON COLUMN dim_delivery_person.delivery_person_id IS '배송기사 고유 ID';

CREATE TABLE IF NOT EXISTS dim_fulfillment (
    fulfillment_id VARCHAR(50) PRIMARY KEY,
    shipping_method VARCHAR(100) NOT NULL,
    service_level VARCHAR(50) NOT NULL,
    delivery_sla_days INT NOT NULL,
    base_shipping_cost DECIMAL(10, 2) NOT NULL
);
COMMENT ON TABLE dim_fulfillment IS '물류 풀필먼트 및 배송 수단 차원 테이블';
COMMENT ON COLUMN dim_fulfillment.fulfillment_id IS '풀필먼트 고유 ID';

CREATE TABLE IF NOT EXISTS dim_payment (
    payment_id VARCHAR(50) PRIMARY KEY,
    payment_method VARCHAR(100) NOT NULL,
    payment_provider VARCHAR(100) NOT NULL,
    description TEXT
);
COMMENT ON TABLE dim_payment IS '결제 수단 및 대행사 차원 테이블';

CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    list_price DECIMAL(15, 2) NOT NULL
);
COMMENT ON TABLE dim_product IS '상품 마스터 차원 테이블';

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone_number VARCHAR(50),
    join_date DATE,
    location_id VARCHAR(50) REFERENCES dim_location(location_id),
    rating DECIMAL(3, 2),
    category_focus VARCHAR(100),
    bank_name VARCHAR(100),
    bank_account_number VARCHAR(100),
    ifsc_code VARCHAR(50)
);
COMMENT ON TABLE dim_seller IS '입점 판매자(셀러) 차원 테이블';

CREATE TABLE IF NOT EXISTS fact_customer_rfm (
    customer_id VARCHAR(50) PRIMARY KEY REFERENCES dim_customer(customer_id),
    recency_days INT,
    frequency_orders INT,
    monetary_value DECIMAL(15, 4),
    avg_order_value DECIMAL(15, 4),
    first_purchase_date DATE,
    last_purchase_date DATE,
    customer_lifetime_days INT,
    rfm_score INT,
    customer_segment VARCHAR(100)
);
COMMENT ON TABLE fact_customer_rfm IS '고객 RFM 세그먼트 및 라이프타임 가치 분석 팩트 테이블';

CREATE TABLE IF NOT EXISTS fact_fulfillment_performance (
    date_id INT REFERENCES dim_calendar(date_id),
    fulfillment_id VARCHAR(50) REFERENCES dim_fulfillment(fulfillment_id),
    location_id VARCHAR(50) REFERENCES dim_location(location_id),
    delivery_person_id VARCHAR(50) REFERENCES dim_delivery_person(delivery_person_id),
    total_deliveries INT,
    completed_deliveries INT,
    returned_deliveries INT,
    avg_delivery_delay_days DECIMAL(10, 2),
    avg_delivery_rating DECIMAL(3, 2),
    total_shipping_cost DECIMAL(15, 2),
    on_time_deliveries INT,
    late_deliveries INT,
    sla_breach_rate DECIMAL(5, 4),
    return_rate DECIMAL(5, 4),
    PRIMARY KEY (date_id, fulfillment_id, location_id, delivery_person_id)
);
COMMENT ON TABLE fact_fulfillment_performance IS '배송 및 물류 수행 실적 일별/기사별/지역별 팩트 테이블';

CREATE TABLE IF NOT EXISTS fact_marketing_spend (
    marketing_id VARCHAR(50) PRIMARY KEY,
    date_id INT REFERENCES dim_calendar(date_id),
    campaign_id VARCHAR(50) REFERENCES dim_campaign(campaign_id),
    channel_id VARCHAR(50) REFERENCES dim_channel(channel_id),
    spend_amount DECIMAL(15, 2),
    impressions INT,
    clicks INT,
    conversions INT,
    revenue_generated DECIMAL(15, 2)
);
COMMENT ON TABLE fact_marketing_spend IS '캠페인 및 유입 경로별 광고 집행 비용 및 성과 팩트 테이블';

CREATE TABLE IF NOT EXISTS fact_orders (
    order_line_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    date_id INT REFERENCES dim_calendar(date_id),
    order_date DATE,
    customer_id VARCHAR(50) REFERENCES dim_customer(customer_id),
    product_id VARCHAR(50) REFERENCES dim_product(product_id),
    category VARCHAR(100),
    seller_id VARCHAR(50) REFERENCES dim_seller(seller_id),
    seller_rating DECIMAL(3, 2),
    location_id VARCHAR(50) REFERENCES dim_location(location_id),
    delivery_person_id VARCHAR(50) REFERENCES dim_delivery_person(delivery_person_id),
    payment_id VARCHAR(50) REFERENCES dim_payment(payment_id),
    campaign_id VARCHAR(50) REFERENCES dim_campaign(campaign_id),
    fulfillment_id VARCHAR(50) REFERENCES dim_fulfillment(fulfillment_id),
    quantity INT,
    unit_price DECIMAL(15, 2),
    discount_percentage DECIMAL(5, 2),
    tax_percentage DECIMAL(5, 2),
    shipping_fee DECIMAL(15, 2),
    expected_delivery_date DATE,
    actual_delivery_date DATE,
    delivery_rating DECIMAL(3, 2),
    order_status VARCHAR(50),
    return_flag BOOLEAN,
    refund_amount DECIMAL(15, 2),
    return_date DATE,
    customer_rating DECIMAL(3, 2),
    review_text TEXT,
    sentiment_score DECIMAL(5, 4),
    delivery_delay_days INT,
    delivery_sla_days INT,
    gross_amount DECIMAL(15, 2),
    discount_amount DECIMAL(15, 2),
    tax_amount DECIMAL(15, 2),
    net_amount DECIMAL(15, 2)
);
COMMENT ON TABLE fact_orders IS '주문 상세 트랜잭션 팩트 테이블';

CREATE TABLE IF NOT EXISTS fact_returns (
    order_line_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    date_id INT REFERENCES dim_calendar(date_id),
    customer_id VARCHAR(50) REFERENCES dim_customer(customer_id),
    product_id VARCHAR(50) REFERENCES dim_product(product_id),
    location_id VARCHAR(50) REFERENCES dim_location(location_id),
    payment_id VARCHAR(50) REFERENCES dim_payment(payment_id),
    fulfillment_id VARCHAR(50) REFERENCES dim_fulfillment(fulfillment_id),
    quantity INT,
    unit_price DECIMAL(15, 2),
    refund_amount DECIMAL(15, 2),
    review_text TEXT,
    days_to_return INT,
    restocking_fee DECIMAL(15, 2),
    customer_rating DECIMAL(3, 2),
    sentiment_score DECIMAL(5, 4)
);
COMMENT ON TABLE fact_returns IS '반품 및 환불 트랜잭션 팩트 테이블';

-- 4. Debezium CDC를 위한 REPLICA IDENTITY FULL 설정
ALTER TABLE dim_calendar REPLICA IDENTITY FULL;
ALTER TABLE dim_channel REPLICA IDENTITY FULL;
ALTER TABLE dim_campaign REPLICA IDENTITY FULL;
ALTER TABLE dim_location REPLICA IDENTITY FULL;
ALTER TABLE dim_customer REPLICA IDENTITY FULL;
ALTER TABLE dim_delivery_person REPLICA IDENTITY FULL;
ALTER TABLE dim_fulfillment REPLICA IDENTITY FULL;
ALTER TABLE dim_payment REPLICA IDENTITY FULL;
ALTER TABLE dim_product REPLICA IDENTITY FULL;
ALTER TABLE dim_seller REPLICA IDENTITY FULL;
ALTER TABLE fact_customer_rfm REPLICA IDENTITY FULL;
ALTER TABLE fact_fulfillment_performance REPLICA IDENTITY FULL;
ALTER TABLE fact_marketing_spend REPLICA IDENTITY FULL;
ALTER TABLE fact_orders REPLICA IDENTITY FULL;
ALTER TABLE fact_returns REPLICA IDENTITY FULL;

-- 5. CSV 데이터 자동 복사 (COPY)
COPY dim_calendar FROM '/kaggle_data/DIM_CALENDAR.csv' DELIMITER ',' CSV HEADER;
COPY dim_channel FROM '/kaggle_data/DIM_CHANNEL.csv' DELIMITER ',' CSV HEADER;
COPY dim_location FROM '/kaggle_data/DIM_LOCATION.csv' DELIMITER ',' CSV HEADER;
COPY dim_fulfillment FROM '/kaggle_data/DIM_FULFILLMENT.csv' DELIMITER ',' CSV HEADER;
COPY dim_payment FROM '/kaggle_data/DIM_PAYMENT.csv' DELIMITER ',' CSV HEADER;
COPY dim_product FROM '/kaggle_data/DIM_PRODUCT.csv' DELIMITER ',' CSV HEADER;

COPY dim_campaign FROM '/kaggle_data/DIM_CAMPAIGN.csv' DELIMITER ',' CSV HEADER;
COPY dim_customer FROM '/kaggle_data/DIM_CUSTOMER.csv' DELIMITER ',' CSV HEADER;
COPY dim_delivery_person FROM '/kaggle_data/DIM_DELIVERY_PERSON.csv' DELIMITER ',' CSV HEADER;

COPY dim_seller FROM '/kaggle_data/DIM_SELLER.csv' DELIMITER ',' CSV HEADER;
COPY fact_customer_rfm FROM '/kaggle_data/FACT_CUSTOMER_RFM.csv' DELIMITER ',' CSV HEADER;
COPY fact_fulfillment_performance FROM '/kaggle_data/FACT_FULFILLMENT_PERFORMANCE.csv' DELIMITER ',' CSV HEADER;
COPY fact_marketing_spend FROM '/kaggle_data/FACT_MARKETING_SPEND.csv' DELIMITER ',' CSV HEADER;
COPY fact_orders FROM '/kaggle_data/FACT_ORDERS.csv' DELIMITER ',' CSV HEADER;
COPY fact_returns FROM '/kaggle_data/FACT_RETURNS.csv' DELIMITER ',' CSV HEADER;

