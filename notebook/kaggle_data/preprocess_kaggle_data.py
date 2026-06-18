import pandas as pd
import glob
import os
import numpy as np

def preprocess_all_csvs():
    data_dir = r"d:\01.DEV\EPSP\notebook\kaggle_data"
    print("Starting Kaggle CSV Preprocessing...")

    # 1. DIM_CUSTOMER.csv 전처리
    customer_path = os.path.join(data_dir, "DIM_CUSTOMER.csv")
    if os.path.exists(customer_path):
        print("Preprocessing DIM_CUSTOMER.csv...")
        df = pd.read_csv(customer_path)
        # date_of_birth, registration_date 포맷 변환 (DD-MM-YYYY -> YYYY-MM-DD)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%d-%m-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        df['registration_date'] = pd.to_datetime(df['registration_date'], format='%d-%m-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        # 신용카드 및 전화번호 문자열화 및 공백 정제
        df['phone_number'] = df['phone_number'].astype(str).str.strip()
        df['credit_card_number'] = df['credit_card_number'].astype(str).str.strip()
        df.to_csv(customer_path, index=False)
        print("DIM_CUSTOMER.csv preprocessed successfully.")

    # 2. DIM_DELIVERY_PERSON.csv 전처리
    dp_path = os.path.join(data_dir, "DIM_DELIVERY_PERSON.csv")
    if os.path.exists(dp_path):
        print("Preprocessing DIM_DELIVERY_PERSON.csv...")
        df = pd.read_csv(dp_path)
        # date_of_joining (DD-MM-YYYY -> YYYY-MM-DD)
        df['date_of_joining'] = pd.to_datetime(df['date_of_joining'], format='%d-%m-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        df.to_csv(dp_path, index=False)
        print("DIM_DELIVERY_PERSON.csv preprocessed successfully.")

    # 3. DIM_SELLER.csv 전처리
    seller_path = os.path.join(data_dir, "DIM_SELLER.csv")
    if os.path.exists(seller_path):
        print("Preprocessing DIM_SELLER.csv...")
        df = pd.read_csv(seller_path)
        # join_date (DD-MM-YYYY -> YYYY-MM-DD)
        df['join_date'] = pd.to_datetime(df['join_date'], format='%d-%m-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        # bank_account_number가 float64로 읽혀 소수점이 들어가는 현상 방지
        # 예: 1.89265e+11 -> 189265000000
        df['bank_account_number'] = df['bank_account_number'].apply(lambda x: str(int(x)) if not pd.isna(x) else "")
        df.to_csv(seller_path, index=False)
        print("DIM_SELLER.csv preprocessed successfully.")

    # 4. FACT_ORDERS.csv 전처리
    orders_path = os.path.join(data_dir, "FACT_ORDERS.csv")
    if os.path.exists(orders_path):
        print("Preprocessing FACT_ORDERS.csv...")
        df = pd.read_csv(orders_path)
        # date_id ('2026-02-20' -> 20260220)
        df['date_id'] = pd.to_datetime(df['date_id'], errors='coerce').dt.strftime('%Y%m%d')
        # NaN인 경우에 대비하여 정수형 문자열 처리
        df['date_id'] = pd.to_numeric(df['date_id'], errors='coerce').fillna(0).astype(int)
        
        # expected_delivery_date, actual_delivery_date, return_date 포맷 정형화
        df['expected_delivery_date'] = pd.to_datetime(df['expected_delivery_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['actual_delivery_date'] = pd.to_datetime(df['actual_delivery_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['return_date'] = pd.to_datetime(df['return_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        # NaN 값들은 빈 문자열로 내보내어 Postgres COPY 시 NULL로 치환되도록 함
        df.to_csv(orders_path, index=False, na_rep="")
        print("FACT_ORDERS.csv preprocessed successfully.")

    # 5. 그 외 파일들 결측치 표준화 (na_rep="")
    other_files = [
        "DIM_CALENDAR.csv", "DIM_CAMPAIGN.csv", "DIM_CHANNEL.csv", "DIM_FULFILLMENT.csv",
        "DIM_LOCATION.csv", "DIM_PAYMENT.csv", "DIM_PRODUCT.csv", "FACT_CUSTOMER_RFM.csv",
        "FACT_FULFILLMENT_PERFORMANCE.csv", "FACT_MARKETING_SPEND.csv", "FACT_RETURNS.csv"
    ]
    for filename in other_files:
        file_path = os.path.join(data_dir, filename)
        if os.path.exists(file_path):
            print(f"Standardizing nulls in {filename}...")
            df = pd.read_csv(file_path)
            # FACT_RETURNS의 경우 return_date가 있을 수 있으나 일단 csv 컬럼 확인
            if 'date_id' in df.columns and df['date_id'].dtype == 'float64':
                df['date_id'] = df['date_id'].fillna(0).astype(int)
            
            # FACT_RETURNS의 review_text나 sentiment_score 등의 결측치를 안전하게 비워줌
            df.to_csv(file_path, index=False, na_rep="")
            print(f"{filename} standardized.")

    print("Kaggle CSV Preprocessing Finished Successfully.")

if __name__ == "__main__":
    preprocess_all_csvs()
