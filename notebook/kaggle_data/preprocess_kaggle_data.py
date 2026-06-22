import pandas as pd
import os

def preprocess_all_csvs():
    data_dir = r"d:\01.DEV\EPSP\notebook\kaggle_data"
    print("Starting Olist Kaggle CSV Preprocessing...")

    # 전처리 대상 파일 및 날짜 컬럼 정의
    date_mappings = {
        "olist_orders_dataset.csv": [
            "order_purchase_timestamp", 
            "order_approved_at", 
            "order_delivered_carrier_date", 
            "order_delivered_customer_date", 
            "order_estimated_delivery_date"
        ],
        "olist_order_items_dataset.csv": [
            "shipping_limit_date"
        ],
        "olist_order_reviews_dataset.csv": [
            "review_creation_date", 
            "review_answer_timestamp"
        ]
    }

    # 1. 날짜 타임스탬프 표준화 처리
    for csv_file, date_cols in date_mappings.items():
        file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(file_path):
            print(f"Standardizing dates in {csv_file}...")
            df = pd.read_csv(file_path)
            for col in date_cols:
                if col in df.columns:
                    # 빈 값 및 포맷팅 처리
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # 결측치를 빈 문자열로 내보내 PostgreSQL COPY NULL AS '' 에 대응
            df.to_csv(file_path, index=False, na_rep="")
            print(f"{csv_file} dates preprocessed successfully.")

    # 2. 기타 텍스트 및 수치형 컬럼 공백 제거 및 결측치 표준화
    all_files = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv"
    ]
    for csv_file in all_files:
        file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(file_path):
            print(f"Standardizing nulls in {csv_file}...")
            df = pd.read_csv(file_path)
            # 모든 텍스트형 컬럼의 앞뒤 공백 정제
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str).str.strip()
                # 'nan' 이나 'None' 등으로 잘못 파싱된 문자열을 결측치 처리
                df[col] = df[col].replace({'nan': None, 'None': None, '': None})

            # 결측치를 빈 문자열로 내보내 저장
            df.to_csv(file_path, index=False, na_rep="")
            print(f"{csv_file} nulls standardized.")

    print("Olist Kaggle CSV Preprocessing Finished Successfully.")

if __name__ == "__main__":
    preprocess_all_csvs()
