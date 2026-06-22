import os
import sys
import subprocess
import pandas as pd
from datetime import datetime

# clickhouse-connect 설치 시도
try:
    import clickhouse_connect
except ImportError:
    print("[Info] clickhouse-connect 라이브러리가 없습니다. 자동 설치를 진행합니다...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "clickhouse-connect"])
        import clickhouse_connect
        print("[Info] clickhouse-connect 설치가 완료되었습니다.")
    except Exception as e:
        print(f"[Error] clickhouse-connect 설치에 실패했습니다: {e}")
        print("수동 설치 명령어: pip install clickhouse-connect")
        sys.exit(1)

# 호스트 설정 (환경 변수 또는 기본값)
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "default")

DATA_DIR = r"d:\01.DEV\EPSP\notebook\kaggle_data"

# 파일명과 클릭하우스 테이블 매핑
MAPPING = {
    "olist_customers_dataset.csv": "stg_olist_customers",
    "olist_geolocation_dataset.csv": "stg_olist_geolocation",
    "olist_orders_dataset.csv": "stg_olist_orders",
    "olist_order_items_dataset.csv": "stg_olist_order_items",
    "olist_order_payments_dataset.csv": "stg_olist_order_payments",
    "olist_order_reviews_dataset.csv": "stg_olist_order_reviews",
    "olist_products_dataset.csv": "stg_olist_products",
    "olist_sellers_dataset.csv": "stg_olist_sellers",
    "product_category_name_translation.csv": "stg_product_category_name_translation"
}

def to_datetime_safe(series):
    """날짜 문자열 컬럼을 Pandas DateTime 객체로 변환하여 ClickHouse의 DateTime64에 올바르게 매핑되도록 함"""
    try:
        return pd.to_datetime(series, errors='coerce').where(pd.notnull(series), None)
    except Exception as e:
        print(f"[Warning] Date conversion warning: {e}")
        return series

def main():
    print("==================================================")
    print("  Olist CSV -> ClickHouse 적재 스크립트 시작")
    print(f"  Target ClickHouse: {CH_HOST}:{CH_PORT} (DB: {CH_DB})")
    print("==================================================")

    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASS,
            database=CH_DB
        )
        print("[Info] ClickHouse 연결 성공!")
    except Exception as e:
        print(f"[Error] ClickHouse 연결에 실패했습니다: {e}")
        sys.exit(1)

    # DateTime64(3) 변환 대상 날짜 컬럼 정의
    date_columns = [
        "order_purchase_timestamp", "order_approved_at", 
        "order_delivered_carrier_date", "order_delivered_customer_date", 
        "order_estimated_delivery_date", "shipping_limit_date", 
        "review_creation_date", "review_answer_timestamp"
    ]

    for csv_file, table_name in MAPPING.items():
        csv_path = os.path.join(DATA_DIR, csv_file)
        if not os.path.exists(csv_path):
            print(f"[Warning] {csv_file} 파일이 존재하지 않아 건너뜁니다. ({csv_path})")
            continue

        print(f"[Process] {csv_file} -> {table_name} 적재 준비 중...")
        try:
            # CSV 로드
            df = pd.read_csv(csv_path)

            # 날짜 컬럼들을 DateTime 객체로 변환
            for col in df.columns:
                if col in date_columns:
                    print(f"  - Converting column `{col}` to Datetime...")
                    df[col] = to_datetime_safe(df[col])

            # CDC 테이블 스키마에 필요한 공통 컬럼 추가 (op, ts_ms)
            df['op'] = 'c'
            df['ts_ms'] = datetime.now()

            # DB의 컬럼 목록 및 타입 정보 조회
            table_info = client.query(f"DESCRIBE TABLE {table_name}")
            db_cols = {}
            for row in table_info.result_rows:
                db_cols[row[0]] = row[1]  # 예: {'order_id': 'String', 'price': 'Decimal(12, 2)'}

            # CSV/DataFrame에 있고 DB에 없는 컬럼 필터링
            insert_cols = [col for col in df.columns if col in db_cols]
            df_to_insert = df[insert_cols].copy()

            # ClickHouse 컬럼 타입과 Nullable 여부에 맞춰 데이터 안전하게 변환
            for col in df_to_insert.columns:
                col_type = db_cols[col]
                is_nullable = "Nullable" in col_type
                is_string = "String" in col_type

                if is_string:
                    if is_nullable:
                        df_to_insert[col] = df_to_insert[col].apply(
                            lambda x: str(x) if pd.notnull(x) and x is not None else None
                        )
                    else:
                        df_to_insert[col] = df_to_insert[col].apply(
                            lambda x: str(x) if pd.notnull(x) and x is not None else ""
                        )
                elif "DateTime" in col_type:
                    # DateTime 형식이면 Pandas NaT를 None으로 변환
                    df_to_insert[col] = df_to_insert[col].where(pd.notnull(df_to_insert[col]), None)
                else:
                    # 숫자 및 기타 타입 처리
                    if is_nullable:
                        df_to_insert[col] = df_to_insert[col].where(pd.notnull(df_to_insert[col]), None)
                    else:
                        # Non-Nullable 인데 결측치가 있는 경우 기본값으로 메워줌
                        if "Int" in col_type:
                            df_to_insert[col] = df_to_insert[col].fillna(0).astype(int)
                        elif "Decimal" in col_type or "Float" in col_type:
                            df_to_insert[col] = df_to_insert[col].fillna(0.0).astype(float)
                        elif "Bool" in col_type:
                            df_to_insert[col] = df_to_insert[col].fillna(False).astype(bool)

            print(f"[Info] {len(df_to_insert)} 행을 {table_name} 에 삽입합니다... (컬럼수: {len(df_to_insert.columns)})")
            
            client.insert_df(table=table_name, df=df_to_insert)
            print(f"[Success] {table_name} 적재 완료!")

        except Exception as e:
            print(f"[Error] {csv_file} 적재 중 오류 발생: {e}")
            continue

    print("==================================================")
    print("  Olist CSV -> ClickHouse 적재 프로세스 완료")
    print("==================================================")

if __name__ == "__main__":
    main()
