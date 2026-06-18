import os
import sys
import time
import random
import uuid
from datetime import datetime

# 필요한 라이브러리 동적 로드 시도 및 안내
try:
    import psycopg2
    from faker import Faker
except ImportError:
    print("[Error] 필수 라이브러리(psycopg2-binary, Faker)가 설치되어 있지 않습니다.")
    print("설치 명령어: pip install psycopg2-binary faker")
    sys.exit(1)

# 설정 (환경 변수 또는 기본값)
DB_HOST = os.getenv("TAILSCALE_DESKTOP_IP", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")  # Phase 1 호스트 외부 포트 5433
DB_NAME = os.getenv("DB_NAME", "ecommerce")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

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

def initialize_products(conn):
    """Postgres DB에 마스터 상품 데이터를 삽입 및 재고 셋팅 (멱등성 보장)"""
    with conn.cursor() as cur:
        # 테이블 존재 확인
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
        print("[Info] 상품 마스터 정보 초기화 완료.")
        return True

def generate_order(conn):
    """1건의 주문을 생성하고 해당 상품의 재고를 차감하는 트랜잭션 수행"""
    product = random.choice(PRODUCT_MASTER)
    order_id = f"ORD-{uuid.uuid4().hex[:12].upper()}"
    user_id = f"USR-{random.randint(1000, 9999)}"
    quantity = random.randint(1, 5)
    total_price = round(product["price"] * quantity, 2)
    
    with conn.cursor() as cur:
        try:
            # 1. 재고 체크
            cur.execute("SELECT stock_quantity FROM products WHERE product_id = %s FOR UPDATE;", (product["id"],))
            row = cur.fetchone()
            if not row or row[0] < quantity:
                print(f"[Warning] {product['id']} 상품의 재고 부족 (요청: {quantity}, 현재고: {row[0] if row else 0}). 주문 스킵.")
                conn.rollback()
                return

            # 2. 주문 등록
            cur.execute(
                """
                INSERT INTO orders (order_id, user_id, product_id, quantity, total_price, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (order_id, user_id, product["id"], quantity, total_price, "CREATED", datetime.now(), datetime.now())
            )
            
            # 3. 재고 차감
            cur.execute(
                """
                UPDATE products 
                SET stock_quantity = stock_quantity - %s, updated_at = %s
                WHERE product_id = %s;
                """,
                (quantity, datetime.now(), product["id"])
            )
            
            conn.commit()
            print(f"[Order Created] {order_id} | {user_id} | {product['id']} ({quantity}개) | {total_price} USD")
            
        except Exception as e:
            conn.rollback()
            print(f"[Error] 주문 트랜잭션 실패: {e}")
            raise e

def simulate_cancellation(conn):
    """랜덤하게 기존 주문 중 하나를 결제 취소(Status -> CANCELLED)하고 재고를 환원함"""
    with conn.cursor() as cur:
        try:
            # 최근 생성된 주문 중 CREATED 상태인 주문 1개 획득
            cur.execute(
                """
                SELECT order_id, product_id, quantity 
                FROM orders 
                WHERE status = 'CREATED' 
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """
            )
            row = cur.fetchone()
            if not row:
                return
            
            order_id, product_id, quantity = row
            
            # 1. 주문 상태 변경
            cur.execute(
                "UPDATE orders SET status = 'CANCELLED', updated_at = %s WHERE order_id = %s;",
                (datetime.now(), order_id)
            )
            
            # 2. 재고 복구
            cur.execute(
                "UPDATE products SET stock_quantity = stock_quantity + %s, updated_at = %s WHERE product_id = %s;",
                (quantity, datetime.now(), product_id)
            )
            
            conn.commit()
            print(f"[Order Cancelled] {order_id} | 상품 {product_id} 재고 {quantity}개 복구 완료")
            
        except Exception as e:
            conn.rollback()
            print(f"[Error] 취소 트랜잭션 실패: {e}")
            raise e

def main():
    print("==================================================")
    print("     E-Commerce 실시간 주문 생성 시뮬레이터")
    print(f"     Target Host: {DB_HOST}:{DB_PORT}")
    print("==================================================")
    
    try:
        conn = get_connection()
    except Exception:
        print("[Error] DB 연결에 실패하여 프로그램을 종료합니다.")
        sys.exit(1)
        
    if not initialize_products(conn):
        conn.close()
        sys.exit(1)
        
    try:
        while True:
            # 85% 확률로 주문 생성, 15% 확률로 결제 취소 시뮬레이션
            if random.random() < 0.85:
                generate_order(conn)
            else:
                simulate_cancellation(conn)
                
            # 생성 주기: 0.5초 ~ 2초 무작위
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\n[Info] 시뮬레이션이 사용자에 의해 중단되었습니다.")
    finally:
        if conn:
            conn.close()
            print("[Info] DB 연결을 닫았습니다.")

if __name__ == "__main__":
    main()
