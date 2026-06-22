import pandas as pd
import os

def preprocess_all_csvs():
    data_dir = r"d:\01.DEV\EPSP\notebook\kaggle_data"
    print("Starting Olist Kaggle CSV Preprocessing...")

    # 1. 날짜 타임스탬프 표준화 처리
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

    for csv_file, date_cols in date_mappings.items():
        file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(file_path):
            print(f"Standardizing dates in {csv_file}...")
            df = pd.read_csv(file_path)
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # 아래 2단계 정수형 변환이 공존할 수 있도록 저장 보류 후 일괄 처리
            if csv_file == "olist_order_items_dataset.csv":
                df["order_item_id"] = pd.to_numeric(df["order_item_id"], errors='coerce').astype('Int64')
            elif csv_file == "olist_order_reviews_dataset.csv":
                df["review_score"] = pd.to_numeric(df["review_score"], errors='coerce').astype('Int64')
                
            df.to_csv(file_path, index=False, na_rep="")
            print(f"{csv_file} dates preprocessed successfully.")

    # 2. olist_products_dataset.csv의 정수/실수형 결측치 Nullable Int64 처리 (.0 소수점 제거)
    products_path = os.path.join(data_dir, "olist_products_dataset.csv")
    if os.path.exists(products_path):
        print("Formatting olist_products_dataset.csv integer columns to Int64...")
        df = pd.read_csv(products_path)
        int_cols = [
            "product_name_lenght", "product_description_lenght", 
            "product_photos_qty", "product_weight_g", 
            "product_length_cm", "product_height_cm", "product_width_cm"
        ]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        df.to_csv(products_path, index=False, na_rep="")
        print("olist_products_dataset.csv formatted successfully.")

    # 3. 기타 파일 우편번호(Zip Code) 등 소수점 탈락 및 문자열 정제
    all_files = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv",
        "olist_order_payments_dataset.csv"
    ]
    for csv_file in all_files:
        file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(file_path):
            print(f"Standardizing nulls and zip codes in {csv_file}...")
            df = pd.read_csv(file_path)
            
            # 우편번호/순서 번호가 float로 소수점화 되는 현상 방지
            zip_cols = ["customer_zip_code_prefix", "geolocation_zip_code_prefix", "seller_zip_code_prefix", "payment_sequential", "payment_installments"]
            for col in zip_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            # 모든 텍스트형 컬럼의 앞뒤 공백 정제
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({'nan': None, 'None': None, '': None})

            # 결측치를 빈 문자열로 내보내 저장
            df.to_csv(file_path, index=False, na_rep="")
            print(f"{csv_file} processed.")

    print("Olist Kaggle CSV Preprocessing Finished Successfully.")

if __name__ == "__main__":
    preprocess_all_csvs()
