import os
import sys
import time
import random
import uuid
import json
from datetime import datetime

# 필요한 라이브러리 동적 로드 시도 및 안내
try:
    import psycopg2
    from faker import Faker
    from confluent_kafka import Producer
except ImportError:
    print("[Error] 필수 라이브러리(psycopg2-binary, Faker, confluent-kafka)가 설치되어 있지 않습니다.")
    print("설치 명령어: pip install psycopg2-binary faker confluent-kafka")
    sys.exit(1)

# 설정 (환경 변수 또는 기본값)
DB_HOST = os.getenv("TAILSCALE_DESKTOP_IP", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "ecommerce")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

KAFKA_BROKER = f"{DB_HOST}:9092"
CLICKSTREAM_TOPIC = "clickstream_events"

# 상품 정보 마스터 (임의의 가격과 함께 프로그램 메모리상 관리)
PRODUCT_MASTER = [
    {"id": "PROD-001", "name": "Premium Wireless Mouse", "category": "Electronics", "price": 49.99},
    {"id": "PROD-002", "name": "Mechanical Keyboard Blue Switch", "category": "Electronics", "price": 89.99},
    {"id": "PROD-003", "name": "UltraWide Gaming Monitor 34inch", "category": "Electronics", "price": 349.99},
    {"id": "PROD-004", "name": "Ergonomic Office Chair", "category": "Furniture", "price": 199.99},
    {"id": "PROD-005", "name": "Adjustable Standing Desk", "category": "Furniture", "price": 299.99},
    {"id": "PROD-006", "name": "Organic Coffee Beans 1kg", "category": "Food", "price": 24.50},
    {"id": "PROD-007", "name": "Stainless Steel Water Bottle", "category": "Kitchen", "price": 19.99},
    {"id": "PROD-008", "name": "Noise Cancelling Headphones", "category": "Electronics", "price": 149.99},
    {"id": "PROD-009", "name": "Eco-friendly Yoga Mat", "category": "Sports", "price": 29.99},
    {"id": "PROD-010", "name": "Leather Passport Holder", "category": "Accessories", "price": 15.00}
]

fake = Faker()

# 유저 풀 생성
USER_MASTER = []
for i in range(1, 101):
    USER_MASTER.append({
        "id": f"USR-{i:04d}",
        "age_group": random.choice(["10s", "20s", "30s", "40s", "50s+"]),
        "gender": random.choice(["M", "F", "Other"]),
        "location": fake.city(),
        "membership_tier": random.choice(["BRONZE", "SILVER", "GOLD", "VIP"])
    })

def get_connection(retries=5, delay=3):
    """네트워크 단절에 대비한 재시도 연결 로직"""
    conn = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[Info] 데이터베이스 연결 시도 중... ({DB_HOST}:{DB_PORT}) - 시도 {attempt}/{retries}")
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=5
            )
            print("[Info] 데이터베이스 연결 성공!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[Warning] 연결 실패: {e}")
            if attempt < retries:
                print(f"[Info] {delay}초 대기 후 재시도합니다.")
                time.sleep(delay)
            else:
                print("[Error] 최대 재시도 횟수를 초과했습니다. 연결 실패.")
                raise e

def initialize_db(conn):
    """Postgres DB에 마스터 유저 및 상품 데이터를 삽입 (멱등성 보장)"""
    with conn.cursor() as cur:
        # 1. Users 초기화
        cur.execute("SELECT to_regclass('public.users');")
        if not cur.fetchone()[0]:
            print("[Error] users 테이블이 데이터베이스에 존재하지 않습니다. init.sql을 확인하세요.")
            return False

        for u in USER_MASTER:
            cur.execute(
                """
                INSERT INTO users (user_id, age_group, gender, location, membership_tier, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING;
                """,
                (u["id"], u["age_group"], u["gender"], u["location"], u["membership_tier"], datetime.now())
            )
        
        # 2. Products 초기화
        cur.execute("SELECT to_regclass('public.products');")
        if not cur.fetchone()[0]:
            print("[Error] products 테이블이 데이터베이스에 존재하지 않습니다.")
            return False

        for prod in PRODUCT_MASTER:
            cur.execute(
                """
                INSERT INTO products (product_id, product_name, category, stock_quantity, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING;
                """,
                (prod["id"], prod["name"], prod["category"], 500, datetime.now())
            )
        conn.commit()
        print("[Info] 유저 및 상품 마스터 정보 초기화 완료.")
        return True

def send_clickstream_event(producer, user_id, product_id, event_type):
    """Kafka로 행동 로그 JSON 전송"""
    if producer is None: return
    
    event = {
        "event_id": f"EVT-{uuid.uuid4().hex[:12].upper()}",
        "user_id": user_id,
        "session_id": f"SESS-{uuid.uuid4().hex[:8].upper()}",
        "event_type": event_type,
        "product_id": product_id,
        "event_time": datetime.now().isoformat()
    }
    
    try:
        producer.produce(CLICKSTREAM_TOPIC, key=user_id, value=json.dumps(event))
        producer.poll(0)
    except Exception as e:
        print(f"[Warning] Kafka Clickstream 전송 실패: {e}")

def generate_order(conn, producer):
    """주문 및 결제 트랜잭션, 그리고 Clickstream 전송"""
    product = random.choice(PRODUCT_MASTER)
    user = random.choice(USER_MASTER)
    
    user_id = user["id"]
    order_id = f"ORD-{uuid.uuid4().hex[:12].upper()}"
    payment_id = f"PAY-{uuid.uuid4().hex[:12].upper()}"
    quantity = random.randint(1, 5)
    total_price = round(product["price"] * quantity, 2)
    
    # 1. 구매 전 행동 로그 시뮬레이션 (조회 -> 장바구니)
    send_clickstream_event(producer, user_id, product["id"], "page_view")
    if random.random() < 0.7:  # 70% 확률로 장바구니 담기
        send_clickstream_event(producer, user_id, product["id"], "add_to_cart")
    
    with conn.cursor() as cur:
        try:
            # 2. 재고 체크
            cur.execute("SELECT stock_quantity FROM products WHERE product_id = %s FOR UPDATE;", (product["id"],))
            row = cur.fetchone()
            if not row or row[0] < quantity:
                print(f"[Warning] {product['id']} 재고 부족. 주문 스킵.")
                conn.rollback()
                return

            now = datetime.now()
            
            # 3. 주문 등록
            cur.execute(
                """
                INSERT INTO orders (order_id, user_id, product_id, quantity, total_price, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (order_id, user_id, product["id"], quantity, total_price, "CREATED", now, now)
            )
            
            # 4. 결제 등록
            payment_method = random.choice(["CREDIT_CARD", "POINT", "BANK_TRANSFER"])
            cur.execute(
                """
                INSERT INTO payments (payment_id, order_id, payment_method, amount, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (payment_id, order_id, payment_method, total_price, "SUCCESS", now)
            )
            
            # 5. 재고 차감
            cur.execute(
                """
                UPDATE products 
                SET stock_quantity = stock_quantity - %s, updated_at = %s
                WHERE product_id = %s;
                """,
                (quantity, now, product["id"])
            )
            
            conn.commit()
            print(f"[Order & Payment Created] {order_id} | {user_id} | {product['id']} | {total_price} USD")
            
            # 구매 완료 행동 로그 전송
            send_clickstream_event(producer, user_id, product["id"], "purchase")
            
        except Exception as e:
            conn.rollback()
            print(f"[Error] 주문 트랜잭션 실패: {e}")

def simulate_cancellation(conn, producer):
    """결제 취소 시뮬레이션"""
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT order_id, product_id, quantity 
                FROM orders 
                WHERE status = 'CREATED' 
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """
            )
            row = cur.fetchone()
            if not row: return
            
            order_id, product_id, quantity = row
            now = datetime.now()
            
            # 1. 주문 상태 변경
            cur.execute("UPDATE orders SET status = 'CANCELLED', updated_at = %s WHERE order_id = %s;", (now, order_id))
            # 2. 결제 상태 변경
            cur.execute("UPDATE payments SET status = 'REFUNDED' WHERE order_id = %s;", (order_id,))
            # 3. 재고 복구
            cur.execute("UPDATE products SET stock_quantity = stock_quantity + %s, updated_at = %s WHERE product_id = %s;", (quantity, now, product_id))
            
            conn.commit()
            print(f"[Order Cancelled & Refunded] {order_id} | 재고 복구 완료")
            
        except Exception as e:
            conn.rollback()
            print(f"[Error] 취소 트랜잭션 실패: {e}")

def main():
    print("==================================================")
    print("     E-Commerce 실시간 데이터 파이프라인 제너레이터")
    print(f"     DB Host: {DB_HOST}:{DB_PORT}")
    print(f"     Kafka Broker: {KAFKA_BROKER}")
    print("==================================================")
    
    producer = None
    try:
        producer = Producer({'bootstrap.servers': KAFKA_BROKER, 'message.timeout.ms': 3000})
        print("[Info] Kafka 프로듀서 초기화 성공!")
    except Exception as e:
        print(f"[Warning] Kafka 프로듀서 초기화 실패 (Clickstream 무시됨): {e}")
    
    try:
        conn = get_connection()
    except Exception:
        print("[Error] DB 연결에 실패하여 종료합니다.")
        sys.exit(1)
        
    if not initialize_db(conn):
        conn.close()
        sys.exit(1)
        
    try:
        while True:
            # 행동 로그만 발생하는 유저 시뮬레이션 (구매 안 함)
            if random.random() < 0.2:
                u = random.choice(USER_MASTER)
                p = random.choice(PRODUCT_MASTER)
                send_clickstream_event(producer, u["id"], p["id"], "page_view")
                
            if random.random() < 0.85:
                generate_order(conn, producer)
            else:
                simulate_cancellation(conn, producer)
                
            if producer: producer.flush(0)
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\n[Info] 시뮬레이션이 사용자에 의해 중단되었습니다.")
    finally:
        if producer: producer.flush()
        if conn:
            conn.close()
            print("[Info] DB 연결을 닫았습니다.")

if __name__ == "__main__":
    main()
