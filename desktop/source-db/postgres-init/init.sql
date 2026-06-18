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
