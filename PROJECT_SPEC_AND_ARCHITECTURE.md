본 문서는 실시간/준실시간 데이터 파이프라인(Kafka, CDC) 구축 및 대용량 DW 설계(ReplacingMergeTree, dbt)에 대한 데이터 엔지니어 핵심 기술 역량 확보를 궁극적 목적으로 하는 '100% 오픈소스 기반 이커머스 실시간 주문 및 재고 변동 파이프라인'의 최종 마스터 명세서입니다. AI 에이전트가 코드를 생성하고 인프라를 빌드할 때 지켜야 할 절대적인 그라운드 트루스(Ground Truth) 자산입니다.

---

## 📌 목차 (Table of Contents)
1. [1. 프로젝트 개요 및 아키텍처 배경 (Architecture Overview)](#1-프로젝트-개요-및-아키텍처-배경-architecture-overview)
    - [1.2 실시간/준실시간 이원화 및 파티셔닝 전략](#12-실시간준실시간-이원화-및-파티셔닝-전략-lambda-architecture--partitioning)
2. [2. 노드별 상세 기술 스펙 (Node Specifications)](#2-노드별-상세-기술-스펙-node-specifications)
3. [3. 데이터 컨트랙트 및 스키마 정의 (Data Contracts)](#3-데이터-컨트랙트-및-스키마-정의-data-contracts)
4. [4. 메달리온 아키텍처(Medallion Architecture) 표준](#4-메달리온-아키텍처medallion-architecture-표준)
5. [5. ClickHouse 특화 dbt 모범사례](#5-clickhouse-특화-dbt-모범사례)
6. [6. PostgreSQL 원천 DB 상세 및 CDC DDL (Source DB)](#6-postgresql-원천-db-상세-및-cdc-ddl-source-db)
7. [7. Kafka & Debezium CDC 스트리밍 설정 (Data Pipeline)](#7-kafka--debezium-cdc-스트리밍-설정-data-pipeline)
8. [8. ClickHouse 실시간 CDC 테이블 상세 (Target DW)](#8-clickhouse-실시간-cdc-테이블-상세-target-dw)
9. [9. Airflow 오케스트레이션 및 배치 파이프라인 (Batch Workflow)](#9-airflow-오케스트레이션-및-배치-파이프라인-batch-workflow)
10. [10. dbt Analytics Engineering 개발 표준 가이드 (AI Agent 행동 지침)](#10-dbt-analytics-engineering-개발-표준-가이드-ai-agent-행동-지침)
11. [11. 데이터 카탈로그 및 AI 에이전트 질의 최적화 표준](#11-데이터-카탈로그-및-ai-에이전트-질의-최적화-표준)
    - [11.1 시맨틱 레이어 정의 (Data Dictionary & Relationships)](#111-시맨틱-레이어-정의-data-dictionary--relationships)
    - [11.2 DB 레벨 메타데이터 영구 주입 (Database Comments Sync)](#112-db-레벨-메타데이터-영구-주입-database-comments-sync)
    - [11.3 ClickHouse ReplacingMergeTree FINAL 조회 표준](#113-clickhouse-replacingmergetree-final-조회-표준)
    - [11.4 레이어별 물리 데이터베이스 격리 표준 (ClickHouse 2계층 대응)](#114-레이어별-물리-데이터베이스-격리-표준-clickhouse-2계층-대응)
    - [11.5 도메인 기반 dbt 모델 디렉토리 설계 표준](#115-도메인-기반-dbt-모델-디렉토리-설계-표준)

---

1. 프로젝트 개요 및 아키텍처 배경 (Architecture Overview)
본 프로젝트의 궁극적인 목적은 데이터 엔지니어로서 실시간 데이터 수집(CDC, Kafka), 분산 적재, 그리고 OLAP 데이터 웨어하우스(ClickHouse)와 데이터 모델링(dbt)의 전체 라이프사이클을 직접 설계하고 조율하는 기술 역량을 강화하는 것입니다. 물리 2노드 하이브리드 아키텍처(집의 데스크톱 PC와 공유오피스의 노트북) 및 Tailscale Mesh VPN을 통한 결합은, 상용 클라우드 리소스 비용을 $0로 통제하면서 실제 물리적인 데이터 전달 및 네트워크 제약을 모방하기 위해 현재의 개발 여건하에 도출된 실용적인 아키텍처적 솔루션입니다.

1.1 하이브리드 분산 토폴로지 구조
[ 노드 2: 노트북 (공유오피스) ]                 [ 노드 1: 데스크톱 PC (집) ]
Orchestration & Ingestion 레이어               Data Infrastructure 레이어 (중앙 허브)
┌──────────────────────────────┐              ┌────────────────────────────────────────┐
│ (1) 가상 주문 생성기          │              │ (2) 원천 운영 DB (Source DB)           │
│  - Python (Tailscale IP 타겟) ├─(트랜잭션)──>│  - PostgreSQL 15 (Docker)              │
└──────────────────────────────┘              └───────────────────┬────────────────────┘
                                                                  │ (Logical Replication / WAL)
                                                                  ▼
┌──────────────────────────────┐              ┌────────────────────────────────────────┐
│ (5) 오케스트레이션 & 마트 가공 │              │ (3) 메시징 및 수집 엔진                  │
│  - Apache Airflow 3.2.2      │              │  - Apache Kafka (KRaft 단일 노드)       │
│  - dbt-core (ClickHouse 연결) │              │  - Debezium Connect / CH Sink          │
└──────────────┬───────────────┘              └───────────────────┬────────────────────┘
               │                                                  │ (실시간 스트리밍 적재)
               │ (15분 크론 / OLAP SQL 명령)                        ▼
               └────────────────────────────────────────> (4) 실시간 데이터 웨어하우스 (DW)  │
                                              │  - ClickHouse (OLAP Engine)            │
                                              └────────────────────────────────────────┘
                              [ 가상 사설망 통신 기저: Tailscale Mesh VPN (WireGuard) ]

---

### 1.2 실시간/준실시간 이원화 및 파티셔닝 전략 (Lambda Architecture & Partitioning)

본 프로젝트는 이커머스 비즈니스의 데이터 다양성을 고려하여, 대규모 과거 이력 분석 및 BI를 위한 준실시간/배치 파이프라인과, 초개인화 추천 및 실시간 경보를 위한 스트리밍 파이프라인을 이원화하여 설계합니다.

#### 1.2.1 람다/카파 아키텍처 기반 이원화 데이터 흐름
* **배치 & 준실시간 (ClickHouse + dbt)**: PostgreSQL CDC 로그 -> Kafka -> ClickHouse 적재 후 Airflow가 15분 마이크로 배치 단위로 dbt 마트를 가공합니다. 데이터의 엄격한 멱등성 및 원장 비교 검증이 강조되는 BI 리포팅 및 회계 정산 용도로 활용합니다.
* **실시간 스트리밍 (Stream Processor)**: Kafka의 `clickstream_events` 토픽 및 `ecommerce_orders_stream` 토픽에서 발행되는 실시간 이벤트를 Flink(또는 Spark Streaming)로 직접 Consume하여, 사용자의 실시간 페이지 뷰 및 구매 전환 여부를 실시간 상태 기반(Stateful)으로 가공합니다. 가공 결과는 Redis나 인메모리 저장소에 캐싱하여 프론트엔드/API에서 실시간 초개인화 추천으로 직접 소비합니다.

#### 1.2.2 이커머스 도메인을 위한 파티셔닝(Partitioning) 표준 및 설계 원칙

실시간 분산 스트림 처리의 병렬성과 데이터 정렬 순서 보장, 그리고 OLAP DW의 저장 공간 및 스캔 속도 최적화를 위해 아래의 명확한 아키텍처 제약조건과 설계 원칙을 강제 적용합니다.

##### 1. Kafka 파티션 키 (Partition Key) 선정 원칙
Kafka는 **동일한 파티션 키를 가진 메시지를 동일 파티션에 인입시키고, 해당 파티션 내부에서만 절대적인 시간 순서(FIFO)를 보장**합니다. 따라서 파티션 키는 **"어떤 비즈니스 식별자 단위를 기준으로 메시지가 정렬되어야 하는가?"**에 의거해 엄격히 구분 지정해야 합니다.

* **회원/유저 행동 식별자 (`user_id`) - 실시간 유저 타임라인 분석용**:
  * 대상 토픽: `clickstream_events`, `ecommerce_orders_stream`
  * 이유: 사용자의 구매 여정(조회 -> 장바구니 -> 주문 -> 취소)은 **사용자 개별 타임라인** 내에서 순서가 절대적으로 정렬되어야 합니다. 만약 주문 취소(`CANCELLED`)가 주문 생성(`CREATED`)보다 파이프라인 순서상 먼저 처리되면, 비즈니스 상태 엔진(Flink)에서 "존재하지 않는 주문에 대한 취소 요청" 에러를 유발합니다. 따라서 사용자 단위의 실시간 분석 및 추천 파이프라인은 반드시 `user_id`를 파티션 키로 사용해야 합니다.
* **주문 식별자 (`order_id`) - 트랜잭션 수집 및 정산용**:
  * 대상 토픽: `ecommerce.public.orders` (Debezium CDC 수집 토픽)
  * 이유: 개별 주문서의 상태 변경 트래킹(`CREATED` -> `PAID` 3. 데이터 컨트랙트 및 스키마 정의 (Data Contracts)
3.1 원천 운영 DB (PostgreSQL DDL)

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

3.2 DW 스테이징 DB (ClickHouse ReplacingMergeTree DDL 예시)
Debezium CDC 특성상 무수히 발생하는 중복 변경 로그(op = c, u)를 수용하고 자동 최신화하기 위해 동일 PK 기준 최신 타임스탬프(ts_ms)만 백그라운드에서 병합하도록 설정합니다.

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

4. 데이터 거버넌스 및 품질 제어 원칙 (Governance & Quality)
AI 에이전트는 코드를 구현할 때 아래 기술적 거버넌스 지침 가이드라인을 절대 준수하여 빌드해야 합니다.

    1. 데이터 생명주기 및 디스크 통제 (TTL): 로컬 자원 고갈을 막기 위해 변경 로그 원본을 적재하는 
    stg_olist_* 테이블들은 인입 시점 기준 7일이 지나면 자동으로 폐기되도록 ClickHouse 내장 TTL 문법을 
    의무 적용합니다.
    
    ALTER TABLE default.stg_olist_orders MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

    2. 데이터 보안 및 가명화 (Masking): 민감 정보인 고유 사용자 식별자(customer_id)는 dbt bronze 모델 
    연산 과정에서 외부 유출을 차단하기 위해 고유 솔트(Salt) 값을 결합한 SHA-256 단방향 해시 
    알고리즘으로 난독화 가명 처리를 강제합니다.

    3. 데이터 정합성 사후 대조 감사 (Data Reconciliation): Airflow 3.2.2 스케줄러 내부에 24시간 주기로 작동하는 data_reconciliation_audit DAG를 구축합니다. 이 DAG는 원천 Postgres DB의 특정 시간대 olist_order_items SUM(price) 연산 결과와, ClickHouse의 동일 시간대 stg_olist_order_items 테이블에 FINAL 제어어를 적용한 연산 결과를 상호 대조하여 격차가 발생할 경우 예외 경고를 발생시킵니다.

5. Airflow 3.2.2 신기능 'Asset' 체인 스케줄링 전략
고빈도 스트리밍 파일이 유입될 때 스케줄러가 터지는 현상을 막기 위해, 상위 수집 및 DW 물리 가공은 15분 크론 주기로 묶고, 후속 다운스트림 비정기 연산에 한해서만 Airflow 3.x Asset Data-Aware 트리거를 연쇄시킵니다.

from airflow.models.dag import DAG
from airflow.sdk import Asset

# 1. dbt 변환이 완료될 최종 ClickHouse 매출 마트 테이블을 상위 핵심 자산(Asset)으로 등록
CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/mart_daily_sales_wide")

6. 리포지토리 디렉토리 레이아웃 가이드

├── desktop/                         # [노드 1 데스크톱] 전용 인프라 컴포넌트
│   ├── source-db/                   # [도메인 1] 원천 데이터베이스 영역
│   │   ├── docker-compose-db.yaml   # Postgres 단독 컴포즈
│   │   └── postgres-init/
│   │       └── init.sql             # Postgres 초기화 DDL 및 REPLICA IDENTITY 설정
│   └── data-pipeline/               # [데이터 플랫폼] 스트리밍 및 ClickHouse DW 영역
│       ├── docker-compose-pipeline.yaml # Kafka, Connect, ClickHouse 통합 컴포즈
│       ├── clickhouse-init/
│       │   └── init-db.sql          # ClickHouse 초기 ReplacingMergeTree DDL 및 TTL 설정
│       └── kafka-connect/
│           ├── submit-pg-source.json # Debezium PostgreSQL Source 커넥터 설정서
│           └── submit-ch-sink.json   # ClickHouse Sink 커넥터 설정서
└── notebook/                        # [노드 2 노트북] 오케스트레이션 및 데이터 인입 컴포넌트
    ├── data-generator/
    ├── dbt_clickhouse_dw/           # ClickHouse 전용 dbt 변환 가공 프로젝트
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    │   └── models/
    │       ├── 01_bronze/           # 원천 1:1 매핑 및 마스킹/파싱 계층 (Bronze Layer)
    │       │   ├── sources.yml
    │       │   ├── stg_customers.sql
    │       │   ├── stg_geolocation.sql
    │       │   ├── stg_orders.sql
    │       │   ├── stg_order_items.sql
    │       │   ├── stg_order_payments.sql
    │       │   ├── stg_order_reviews.sql
    │       │   ├── stg_products.sql
    │       │   ├── stg_sellers.sql
    │       │   └── stg_category_translation.sql
    │       ├── 02_silver/           # 비즈니스 정규화 (Dim/Fact) 계층 (Silver Layer)
    │       │   ├── dim_customers.sql
    │       │   ├── dim_products.sql
    │       │   ├── dim_sellers.sql
    │       │   └── fact_orders.sql
    │       └── 03_gold/             # BI 시각화 및 집계용 (OBT/MV) 계층 (Gold Layer)
    │           ├── mart_daily_sales_wide.sql
    │           └── mart_customer_rfm.sql
    └── airflow_3_2/                 # Airflow 3.2.2 샌드박스
        ├── dags/
        │   ├── local_ecommerce_pipeline.py  # 15분 주기 마이크로 배치 메인 DAG
        │   └── data_reconciliation_audit.py # Postgres vs ClickHouse 데이터 원장 정합성 검증 DAG

7. AI 에이전트 동작 지침 (Instructions for AI Agent)
    1. 환경 설정 분리: 인프라 컨테이너 구동 파일 생성 시 100.X.X.X 주소는 환경 변수(${TAILSCALE_DESKTOP_IP}) 
    구조로 템플릿화하여 호스트 머신 환경에 따라 유연하게 매핑될 수 있도록 처리하십시오.
    
    2. 문법 엄격성: ClickHouse DDL 및 dbt 쿼리 모델링을 지시받을 때, 비동기 데이터 처리를 보정하기 위해 DDL 
    영역에는 ReplacingMergeTree(ts_ms)를, DML 쿼리 조회 영역에는 반드시 FINAL 키워드와 is_incremental() 
    시점 필터 문법을 결합하여 멱등성이 유지되도록 코드를 자동 고도화하십시오.
    
    3. 네트워크 연속성 가이드: 네트워크 장애나 레이턴시로 인한 데이터 지연이 발생할 수 있으므로, 카프카 
    프로듀서 소스 코드 구현 시 재시도(retries=5) 및 타임아웃 예외 처리 핸들러 로직을 강제 설계하십시오., 15분 주기 마이크로 배치 크론 스케줄링을 통해 가공 부하와 비용을 통제합니다.

가공 및 모델링 (dbt-core + dbt-clickhouse): 에어플로우 스케줄러의 통제를 받아 데스크톱의 ClickHouse 서버에 커넥션을 맺고, 원천 로그 테이블에서 비즈니스 마트 테이블로의 SQL 변환 연산을 ClickHouse 내부 컴퓨팅 리소스를 활용해 수행합니다.

3. 데이터 컨트랙트 및 스키마 정의 (Data Contracts)
3.1 원천 운영 DB (PostgreSQL DDL)

CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    age_group VARCHAR(20),
    gender VARCHAR(10),
    location VARCHAR(100),
    membership_tier VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    stock_quantity INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(user_id),
    product_id VARCHAR(50) REFERENCES products(product_id),
    quantity INT NOT NULL,
    total_price DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'CREATED', 'CANCELLED', 'RETURNED'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payments (
    payment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    payment_method VARCHAR(50) NOT NULL, -- 'CREDIT_CARD', 'POINT', 'BANK_TRANSFER'
    amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'REFUNDED'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 행동 로그 (Clickstream)는 Postgres를 거치지 않고 Python 생성기에서 Kafka로 직결 송출합니다.
-- 이벤트 구조: event_id, user_id, session_id, event_type(page_view, add_to_cart, checkout, purchase), product_id, event_time

3.2 DW 스테이징 DB (ClickHouse ReplacingMergeTree DDL)
Debezium CDC 특성상 무수히 발생하는 중복 변경 로그(op = c, u)를 수용하고 자동 최신화하기 위해 동일 PK 기준 최신 타임스탬프(ts_ms)만 백그라운드에서 병합하도록 설정합니다.

CREATE TABLE default.stg_orders (
    order_id String,
    user_id String,
    product_id String,
    quantity Int32,
    total_price Decimal(12, 2),
    status String,
    op String,
    ts_ms DateTime64(3)
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY order_id 
ORDER BY order_id;

4. 데이터 거버넌스 및 품질 제어 원칙 (Governance & Quality)
AI 에이전트는 코드를 구현할 때 아래 기술적 거버넌스 지침 가이드라인을 절대 준수하여 빌드해야 합니다.

    1. 데이터 생명주기 및 디스크 통제 (TTL): 로컬 자원 고갈을 막기 위해 변경 로그 원본을 적재하는 
    stg_orders 테이블은 인입 시점 기준 7일이 지나면 자동으로 폐기되도록 ClickHouse 내장 TTL 문법을 
    의무 적용합니다.
    
    ALTER TABLE default.stg_orders MODIFY TTL toDateTime(ts_ms) + INTERVAL 7 DAY;

    2. 데이터 보안 및 가명화 (Masking): 민감 정보인 고유 사용자 식별자(user_id)는 dbt bronze 모델 
    연산 과정에서 외부 유출을 차단하기 위해 고유 솔트(Salt) 값을 결합한 SHA-256 단방향 해시 
    알고리즘으로 난독화 가명 처리를 강제합니다.

    3. 데이터 정합성 사후 대조 감사 (Data Reconciliation): Airflow 3.2.2 스케줄러 내부에 24시간 주기로 작동하는 data_reconciliation_audit DAG를 구축합니다. 이 DAG는 원천 Postgres DB의 특정 시간대 SUM(total_price) 연산 결과와, ClickHouse의 동일 시간대 테이블에 FINAL 제어어를 적용한 연산 결과를 상호 대조하여 격차가 발생할 경우 예외 경고(Slack Alert 등)를 발생시키는 감사 추적성을 확보합니다.

5. Airflow 3.2.2 신기능 'Asset' 체인 스케줄링 전략
고빈도 스트리밍 파일이 유입될 때 스케줄러가 터지는 현상을 막기 위해, 상위 수집 및 DW 물리 가공은 15분 크론 주기로 묶고, 후속 다운스트림 비정기 연산에 한해서만 Airflow 3.x Asset Data-Aware 트리거를 연쇄시킵니다.

from airflow.models.dag import DAG
from airflow.sdk import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 1. dbt 변환이 완료될 최종 ClickHouse 매출 마트 테이블을 상위 핵심 자산(Asset)으로 등록
CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/fact_orders_hourly")

# [DAG 1] 15분 주기 주기적 배치 파이프라인 (인프라 안정성 확보)
with DAG(dag_id="batch_15min_dw_transform", schedule="*/15 * * * *", catchup=False) as dag_1:
    run_dbt_transform = SQLExecuteQueryOperator(
        task_id="execute_dbt_gold",
        conn_id="clickhouse_desktop",
        sql="-- dbt run invocation wrapper",
        outlets=[CLICKHOUSE_ORDER_GOLD_ASSET] # 태스크 성공 시 자산 이벤트 방출
    )

# [DAG 2] 데이터가 갱신되었을 때만 유기적으로 반응하는 이벤트 기반 다운스트림 (Data-Aware)
with DAG(dag_id="reactive_stock_alert_pipeline", schedule=[CLICKHOUSE_ORDER_GOLD_ASSET], catchup=False) as dag_2:
    check_stock_leak = SQLExecuteQueryOperator(
        task_id="detect_low_stock_and_notify",
        conn_id="clickhouse_desktop",
        sql="SELECT product_id, sum(quantity) FROM fact_orders_hourly GROUP BY product_id;"
    )

6. 리포지토리 디렉토리 레이아웃 가이드

├── desktop/                         # [노드 1 데스크톱] 전용 인프라 컴포넌트
│   ├── source-db/                   # [도메인 1] 원천 데이터베이스 영역
│   │   ├── docker-compose-db.yaml   # Postgres 단독 컴포즈
│   │   └── postgres-init/
│   │       └── init.sql             # Postgres 초기화 DDL 및 REPLICA IDENTITY 설정
│   └── data-pipeline/               # [데이터 플랫폼] 스트리밍 및 ClickHouse DW 영역
│       ├── docker-compose-pipeline.yaml # Kafka, Connect, ClickHouse 통합 컴포즈
│       ├── clickhouse-init/
│       │   └── init-db.sql          # ClickHouse 초기 ReplacingMergeTree DDL 및 TTL 설정
│       └── kafka-connect/
│           ├── Dockerfile           # Debezium + ClickHouse 어댑터 커스텀 이미지 빌드 파일
│           ├── submit-pg-source.json # Debezium PostgreSQL Source 커넥터 설정서
│           └── submit-ch-sink.json   # ClickHouse Sink 커넥터 설정서
└── notebook/                        # [노드 2 노트북] 오케스트레이션 및 데이터 인입 컴포넌트
    ├── data-generator/
    │   └── transaction_generator.py # 트래픽 유발 파이썬 엔진 (Tailscale IP 주입)
    ├── dbt_clickhouse_dw/           # ClickHouse 전용 dbt 변환 가공 프로젝트
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    │   └── models/
    │       ├── 01_bronze/           # 원천 1:1 매핑 및 마스킹/파싱 계층 (Bronze Layer)
    │       │   ├── ecommerce/       # Postgres 기반 CDC 도메인
    │       │   │   ├── sources.yml
    │       │   │   ├── stg_orders.sql
    │       │   │   ├── stg_products.sql
    │       │   │   ├── stg_users.sql
    │       │   │   └── stg_payments.sql
    │       │   └── clickstream/     # 행동 로그 도메인
    │       │       └── stg_events.sql
    │       ├── 02_silver/           # 비즈니스 정규화 (Dim/Fact) 계층 (Silver Layer)
    │       │   ├── users/
    │       │   │   └── dim_users.sql
    │       │   ├── products/
    │       │   │   └── dim_products.sql
    │       │   └── sales/
    │       │       └── fact_sales.sql
    │       └── 03_gold/             # BI 시각화 및 집계용 (OBT/MV) 계층 (Gold Layer)
    │           ├── marketing/
    │           │   └── mart_user_retention.sql
    │           └── executive/
    │               └── mart_daily_sales_wide.sql
    └── airflow_3_2/                 # Airflow 3.2.2 샌드박스
        ├── docker-compose.yaml
        └── dags/
            ├── local_ecommerce_pipeline.py  # 15분 주기 마이크로 배치 메인 DAG
            └── data_reconciliation_audit.py # Postgres vs ClickHouse 데이터 원장 정합성 검증 DAG

7. AI 에이전트 동작 지침 (Instructions for AI Agent)
    1. 환경 설정 분리: 인프라 컨테이너 구동 파일 생성 시 100.X.X.X 주소는 환경 변수(${TAILSCALE_DESKTOP_IP}) 
    구조로 템플릿화하여 호스트 머신 환경에 따라 유연하게 매핑될 수 있도록 처리하십시오.
    
    2. 문법 엄격성: ClickHouse DDL 및 dbt 쿼리 모델링을 지시받을 때, 비동기 데이터 처리를 보정하기 위해 DDL 
    영역에는 ReplacingMergeTree(ts_ms)를, DML 쿼리 조회 영역에는 반드시 FINAL 키워드와 is_incremental() 
    시점 필터 문법을 결합하여 멱등성이 유지되도록 코드를 자동 고도화하십시오.
    
    3. 네트워크 연속성 가이드: 네트워크 장애나 레이턴시로 인한 데이터 지연이 발생할 수 있으므로, 카프카 
    프로듀서 소스 코드 구현 시 재시도(retries=5) 및 타임아웃 예외 처리 핸들러 로직을 강제 설계하십시오.

8. 호스트 개발 환경 최적화 설정 (Windows WSL2 Config)
Windows 호스트(특히 노트북 노드)에서 Docker Desktop 및 WSL2를 구동할 때, `vmmemWSL` 프로세스의 과도한 메모리 점유 및 시스템 프리징을 방지하기 위해 사용자 프로필 디렉토리(`%USERPROFILE%`)에 `.wslconfig` 설정을 적용하여 가상 머신의 최대 자원 할당량을 강제 제한합니다.

파일명: `%USERPROFILE%\.wslconfig`
```ini
[wsl]
memory=6GB
processors=4
pageReporting=true
```
- **memory=6GB**: WSL 가상 머신에 할당할 최대 메모리 크기입니다. Airflow 및 로컬 테스트 수행 시 호스트 OS(Windows)의 메모리 고갈을 막기 위한 최적 타협선입니다.
- **processors=4**: 가상 머신이 사용할 최대 논리 프로세서(CPU Core) 개수입니다.
- **pageReporting=true**: WSL2 가상 머신 내부에서 더 이상 사용하지 않는 메모리를 호스트 Windows OS에 실시간으로 반환하도록 유도하여 메모리 누수를 경감시킵니다.

9. Airflow 3.2+ 개발 표준 가이드 (AI Agent 행동 지침)

본 가이드는 Airflow 3.0+ (AIP-72) 및 Task SDK 아키텍처에 맞추어 DAG를 작성할 때 에이전트 및 개발자가 반드시 준수해야 하는 최신 표준 그라운드 룰입니다.

### 9.1 핵심 개발 그라운드 룰 (Core Ground Rules)

1. **Task SDK 공식 네임스페이스 (`airflow.sdk`) 사용 의무화**
   - 기존 Airflow 2.x의 내부 모듈 임포트(예: `airflow.models.dag.DAG`, `airflow.decorators.task`)는 더 이상 사용하지 않습니다.
   - 모든 DAG 구성 요소 및 유틸리티는 `from airflow.sdk import ...` 경로로 통일합니다.
     - *대표 Import 대상*: `dag`, `task`, `Asset`, `Connection`, `Variable`, `get_current_context`

2. **메타데이터 DB 직접 접근 엄격 금지 (Execution API 강제)**
   - 워커의 태스크 코드 내에서 메타데이터 데이터베이스에 직접 커넥션을 맺거나 내부 모델(예: `DagRun`, `TaskInstance`)을 직접 임포트해 쿼리할 수 없습니다.
   - 데이터베이스와의 통신은 에어플로우 내부 REST API 및 Execution API를 거쳐야 하므로, 태스크 상태나 콘텍스트 정보 조회 시에는 반드시 `get_current_context()` 메소드를 통해 접근하십시오.

3. **태그(Tags) 메타데이터 강제화**
   - Airflow 3.x부터는 UI 및 오케스트레이션 관리 목적의 태깅이 표준화되어 권장됩니다.
   - 모든 DAG 선언부(`@dag` 데코레이터)에는 시스템 분류를 명시하는 `tags` 파라미터를 필수로 작성해야 합니다. (예: `tags=["ecommerce", "cdc", "clickhouse"]`)

4. **TaskFlow API 및 데코레이터 우선 표준**
   - 전통적인 Operator 클래스를 명시적으로 선언하여 연결하는 방식(예: `PythonOperator`) 대신, 가독성과 데이터 흐름 추적이 용이한 `@dag`, `@task`, `@task_group` 데코레이터를 이용한 TaskFlow API 작성을 기본 원칙으로 합니다.

5. **Asset(구 Dataset) 기반 스케줄링 적용**
   - 2.x 버전에서 사용되던 `Dataset` 명칭은 3.x 버전부터 `Asset`으로 일괄 개편되었습니다.
   - 스케줄링 간 이벤트 기반 연쇄 흐름을 설계할 때는 `from airflow.sdk import Asset`을 선언하여 데이터 의존성 체인을 구성하십시오.

---

### 9.2 Airflow 3.2+ 표준 코딩 모범 사례 (Best Practice)

```python
from datetime import datetime
from airflow.sdk import dag, task, Asset, get_current_context

# 1. 3.x 규격의 Asset 정의 (일관된 URI 스키마 사용)
CLICKHOUSE_GOLD_ASSET = Asset(uri="clickhouse://default/fact_orders_hourly")

# 2. dag 데코레이터 및 필수 태깅(tags) 적용
@dag(
    dag_id="ecommerce_3x_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",  # 15분 마이크로 배치
    catchup=False,
    tags=["ecommerce", "dbt", "dw"]  # 3.x 필수 권장 태깅
)
def my_pipeline():

    # 3. TaskFlow API 활용 및 return을 통한 XCom 데이터 연계
    @task(task_id="extract_postgres_watermark")
    def get_watermark():
        # 4. get_current_context()를 통한 안전한 콘텍스트 접근 (DB 직접 쿼리 절대 금지)
        context = get_current_context()
        ti = context["ti"]
        logical_date = context["logical_date"]
        
        print(f"Executing Task: {ti.task_id} for logical date: {logical_date}")
        return logical_date.strftime("%Y-%m-%d %H:%M:%S")

    @task(task_id="trigger_dbt_gold", outlets=[CLICKHOUSE_GOLD_ASSET])
    def run_gold(watermark_time):
        print(f"Running dbt gold with watermark: {watermark_time}")
        # ClickHouse OLAP 쿼리 및 dbt 변환 가공 로직 수행...
        return "SUCCESS"

    # Task 간 의존성 연결
    watermark = get_watermark()
    run_gold(watermark)

my_pipeline()
```

### 9.3 공통 로직 캡슐화 및 결합도 제어 가이드 (Plugins & Coupling Guide)

1. **공통 로직의 `plugins` 관리 원칙**
   - DAG 간에 재사용되는 공통 기능(예: 알림 유틸리티, 공통 DB API 래퍼, 특정 데이터 정제 모듈 등)은 `notebook/airflow_3_2/plugins` 디렉토리 하위에 모듈/패키지 형태로 작성하여 관리합니다.
   - 플러그인에 정의된 모듈은 Airflow 3.x 환경에서 임포트할 때 표준 파이썬 경로로 접근이 가능하며, 구조화된 네임스페이스를 준수해야 합니다.
   - **캡슐화(Encapsulation)**를 통해 공통 함수의 내부 세부 구현(예: 구체적인 SQL 연결 파라미터나 API endpoint 호출 구조)을 숨기고, DAG 비즈니스 로직에는 간결한 인터페이스만 노출시켜 재사용성과 가독성을 극대화합니다.

2. **강결합 방지를 위한 유연성 대처 원칙 (Loose Coupling)**
   - **강결합(Tight Coupling) 리스크 방지**: 만약 공통 로직이 특정 DAG의 비즈니스 도메인(예: 복잡한 쿼리 스키마 변형, 특정 로직 전용 예외 처리 등)과 밀접하게 연동된다면, 이를 무리하게 공통화하여 플러그인에 캡슐화하는 것은 피해야 합니다. 이는 해당 모듈을 참조하는 다른 DAG들의 동시 변경을 강제(사이드 이펙트 유발)하기 때문입니다.
   - **유연한 대처 규칙**:
     - *도메인 특화 쿼리 및 변환 로직*: 개별 DAG 또는 태스크 내부에 직접 인라인(Inline)으로 작성하거나, 해당 DAG 파일이 속한 로컬 디렉토리 내부에서만 사용될 헬퍼 모듈로 격리하여 결합을 차단합니다.
     - *의존성 주입(Dependency Injection) 지향*: 공통 코드가 필요하지만 특정 비즈니스 파라미터나 쿼리가 달라져야 하는 경우, 로직 내에 하드코딩하지 않고 매개변수(Parameter)나 콜백 함수(Callback) 형태로 외부에서 주입받도록 구성하여 결합도를 낮춥니다.

### 9.4 Airflow Connection 및 외부 인프라 연동 설정 표준 (Connections Specification)

에어플로우가 각 이기종 데이터 소스 및 타깃 DW와 통신하기 위해 정의하는 Connection(연결) 정보에 대한 공식 스펙입니다.

1. **원천 PostgreSQL 연결 (`postgres_desktop`)**
   - **Conn ID**: `postgres_desktop`
   - **Conn Type**: `Postgres`
   - **연결 매개변수 설정**:
     - *로컬 샌드박스 내부 테스트 시*:
       - **Host**: `ecommerce-postgres` (동일 도커 브리지 네트워크 상의 PostgreSQL 컨테이너명)
       - **Port**: `5432`
     - *노트북(노드 2)에서 데스크톱(노드 1) 원격 연결 시*:
       - **Host**: `${TAILSCALE_DESKTOP_IP}` (데스크톱의 Tailscale 가상 IP)
       - **Port**: `5433` (외부 포워딩 포트)
     - **Database**: `postgres`
     - **Login**: `postgres`
     - **Password**: `postgres` (또는 프로젝트 패스워드 설정값)
   - **원클릭 등록 CLI 예시**:
     ```bash
     docker exec -i --user airflow airflow-322_local_sandbox-airflow-scheduler-1 \
       airflow connections add postgres_desktop \
       --conn-type postgres \
       --conn-host ecommerce-postgres \
       --conn-login postgres \
       --conn-password postgres \
       --conn-port 5432 \
       --conn-schema postgres
     ```

2. **대상 ClickHouse 연결 (`clickhouse_desktop`)**
   - **Conn ID**: `clickhouse_desktop`
   - **Conn Type**: `Generic` (또는 ClickHouse 전용 Provider 사용 시 `clickhouse`)
   - **연결 매개변수 설정**:
     - *로컬 샌드박스 내부 테스트 시*:
       - **Host**: `ecommerce-clickhouse` (ClickHouse 컨테이너명)
       - **Port**: `8123` (HTTP interface)
     - *노트북(노드 2)에서 데스크톱(노드 1) 원격 연결 시*:
       - **Host**: `${TAILSCALE_DESKTOP_IP}`
       - **Port**: `8123`
     - **Database**: `default` (혹은 `analytics_bronze`, `analytics_silver` 등 레이어별 격리 DB)
     - **Login**: `default`
     - **Password**: (공란 또는 지정된 패스워드)
   - **원클릭 등록 CLI 예시**:
     ```bash
     docker exec -i --user airflow airflow-322_local_sandbox-airflow-scheduler-1 \
       airflow connections add clickhouse_desktop \
       --conn-type generic \
       --conn-host ecommerce-clickhouse \
       --conn-login default \
       --conn-port 8123 \
       --conn-schema default
     ```

10. dbt Analytics Engineering 개발 표준 가이드 (AI Agent 행동 지침)

본 프로젝트에서는 복잡한 DW 환경에서 일관성을 유지하기 위해 `Bronze -> Silver(Dim/Fact) -> Gold` 구조의 모델링을 지향하며, 모든 dbt SQL 모델은 다음 표준을 엄격히 준수해야 합니다.

1. **디렉토리 레이아웃 원칙**
   - 모델 파일은 반드시 `models/[스키마명]/[업무도메인별]/[모델명].sql` 경로에 위치해야 합니다.
   - 예: `models/01_bronze/ecommerce/stg_orders.sql`, `models/02_silver/users/dim_users.sql`

2. **명명 규칙 (Naming Conventions)**
   - 모델명은 반드시 생성되는 대상 테이블명과 1:1로 일치해야 합니다. (예: 파일명이 `stg_orders.sql`이면, 테이블명도 `stg_orders`여야 함)
   - 소문자와 스네이크 케이스(snake_case)만 사용합니다.

3. **코드 작성 구조 및 순서 원칙**
   모든 dbt 모델의 내부 코드는 반드시 다음 순서대로 작성되어야 합니다.

   - **[1] Header Comments (헤더 주석)**: 최상단에 모델의 명칭, 목적, 변경 이력을 표준 양식으로 작성.
   - **[2] Hooks (훅 설정)**: 모델 실행 전후에 중복 체킹이나 로깅 등이 필요할 경우 `pre_hook`, `post_hook` 선언. (불필요한 경우 생략 가능하나 위치는 헤더 다음)
   - **[3] Config (설정)**: ClickHouse 특성(`materialized`, `engine`, `order_by` 등)을 선언.
   - **[4] SQL Query (본문)**: 비즈니스 로직 작성.

   **[표준 템플릿 예시]**
   ```sql
   /*
   =============================================================================
   * 모델명      : 모델명_테이블명 (예: stg_orders)
   * 모델 목적   : 이 모델이 하는 역할에 대한 간략한 설명
   * 
   * [수정 이력]
   * - YYYY-MM-DD [작성자] : 최초 생성 및 내용 요약
   * - YYYY-MM-DD [작성자] : 기능 추가 또는 리팩토링 내용
   =============================================================================
   */

   -- [Pre/Post Hooks] (필요한 경우)
   {{ config(
       pre_hook="-- 사전 검증 로직이나 로깅을 여기에 작성"
   ) }}

   -- [Config]
   {{ config(
       materialized='view',
       -- ClickHouse 전용 설정 등
   ) }}

   -- [SQL 본문]
   select
       ...
   ```

4. **메달리온 아키텍처(Medallion Architecture) 표준**

메달리온 아키텍처는 원시(Raw) 데이터를 세 단계 계층(Bronze → Silver → Gold)으로 점진적으로 정제하여 고품질의 비즈니스 인사이트로 변환하는 데이터 레이크하우스 디자인 패턴입니다. 본 프로젝트의 dbt 데이터 파이프라인은 이 계층 구조를 명확히 준수해야 합니다.

* **브론즈 레이어 (Bronze Layer)**
  * **역할**: 외부 시스템(예: PostgreSQL CDC, Clickstream)에서 수집된 초기 데이터가 적재되는 영역입니다. 원본 데이터를 있는 그대로 보존하여, 데이터 유실을 방지하고 필요시 원본 데이터를 재처리(Replay)할 수 있게 합니다.
  * **특징**: 무수히 발생하는 원시 로그가 1:1로 매핑되는 영역으로, 주로 `view` 혹은 대용량 분석 데이터의 경우 `MergeTree` 엔진 테이블로 구체화합니다.
* **실버 레이어 (Silver Layer)**
  * **역할**: 브론즈 레이어 데이터를 기반으로 필터링, 중복 제거(Deduplication), 데이터 마스킹, 스키마 적용 및 정규화를 수행하여 정제된 엔터프라이즈 기준 데이터를 만듭니다. 신뢰할 수 있는 단일 소스(Single Source of Truth, SSOT) 역할을 합니다.
  * **특징**: ClickHouse 환경에서 중복 제거 및 최신화를 위해 `ReplacingMergeTree` 엔진을 적극 활용합니다.
* **골드 레이어 (Gold Layer)**
  * **역할**: BI 대시보드, 분석 보고서, 머신러닝 모델 등에 최종 요약된 비즈니스 수준 데이터를 제공합니다. 부서 및 분석 목적에 맞추어 사전에 집계(Aggregation)된 형태를 가집니다.
  * **특징**: 초고속 쿼리 성능을 보장하기 위해 `SummingMergeTree` 또는 `AggregatingMergeTree` 엔진을 활용하거나, 배치 주기 스캔 비용을 줄이기 위해 실시간 Materialized View 패턴을 채택합니다.

---

5. **ClickHouse 특화 dbt 모범사례**

ClickHouse는 컬럼 지향 OLAP DB로서 고유의 아키텍처적 특성을 가집니다. 성능 극대화 및 리소스 절약을 위해 dbt-clickhouse 결합 시 다음 5가지 모범사례를 의무적으로 준수해야 합니다.

#### [1] 레이어별 ClickHouse 엔진 및 구체화(Materialization) 매핑
dbt 모델의 `config()` 매크로 또는 `dbt_project.yml`을 통해 계층별로 최적의 테이블 엔진과 구체화 전략을 정의합니다.
* **Bronze**: 
  * 기본적으로 가벼운 `view`로 처리합니다. 단, 대량의 실시간 클릭스트림 데이터 등이 유입되어 원천 버퍼링이 필요하다면 `MergeTree` 엔진으로 구체화합니다.
* **Silver**:
  * 데이터 정제 및 중복 제거가 일어나는 계층입니다. ClickHouse는 `UPDATE` 연산 비용이 매우 크므로, 동일 PK 기준 최신 타임스탬프(`ts_ms`) 데이터만 병합하는 `ReplacingMergeTree` 엔진을 필수적으로 정의합니다.
  * *예시 (int_users.sql)*:
    ```sql
    {{ config(
        materialized='table',
        engine='ReplacingMergeTree(ts_ms)',
        order_by='user_id',
        primary_key='user_id'
    ) }}
    ```
* **Gold**:
  * 초고속 조회 및 리포팅을 위해 정렬/차원별로 사전에 집계해두는 `SummingMergeTree` 또는 고도화된 통계 분석용 `AggregatingMergeTree` 엔진을 지정하여 마트를 생성합니다.

#### [2] 실시간 파이프라인을 위한 Materialized View 패턴 활용
ClickHouse는 실시간 스트리밍 분석에 특화되어 있습니다. 주기적인 대규모 배치 스캔(전체 테이블 스캔)을 방지하고 실시간으로 지표를 반영하기 위해 `materialized_view` 구체화 방식을 씁니다.
* **모범사례**: 소스 테이블에 데이터가 인입될 때마다 백그라운드에서 실시간으로 트리거되어 요약 대상 테이블(Target Table)에 데이터를 증분 밀어 넣어주는 ClickHouse Native Materialized View를 dbt 모델로 관리하여, dbt DAG의 계보 및 가시성을 유지하면서 실시간 스트리밍 처리를 구현합니다.

#### [3] 성능 극대화를 위한 Index 및 Partition 전략 주입
ClickHouse는 희소 인덱스(Sparse Index)를 사용하므로, 적절한 정렬 및 파티셔닝 키 설정이 누락되면 스캔 범위 증가로 성능이 급격히 저하됩니다.
* **`order_by` (필수)**: 쿼리의 `WHERE` 조건이나 `JOIN` 조건에 주로 쓰이는 컬럼(예: `customer_id`, `event_type`)을 결합 튜플 형태로 지정하여 희소 인덱스를 자동 생성합니다.
* **`partition_by` (선택)**: 데이터 볼륨이 일별/월별로 수천만 건 이상 적재되는 팩트 테이블의 경우, `toYYYYMM(created_at)`과 같이 월/일 단위 파티셔닝 키를 명시하여 데이터 삭제(Drop Partition) 및 쿼리 시의 데이터 프루닝(Data Pruning)을 유도합니다.

#### [4] 대용량 데이터를 위한 Incremental(증분) 전략 최적화
전체 테이블 재빌드에 따른 리소스 낭비를 방지하기 위해 dbt의 증분(Incremental) 빌드를 적극 도입합니다. dbt-clickhouse 어댑터가 제공하는 다음 증분 전략을 목적에 맞게 매핑합니다.
1. **`append` 전략 (추천)**: 로그성 데이터처럼 과거 행이 수정되지 않는 Insert-only 형태인 경우, 단순 추가 연산만 수행하므로 ClickHouse 설계 철학에 가장 부합하며 가장 가볍습니다.
2. **`delete+insert` 전략**: 과거 특정 기간/파티션의 데이터 갱신이 발생하여 덮어쓰기가 필요할 때 사용합니다. dbt가 자동으로 기존 블록을 지우고 신규 블록을 삽입합니다.

#### [5] 연결 세션 설정 제어 및 프로필 최적화
dbt 모델 내에서 `pre-hook` 등을 통해 데이터베이스 세션 파라미터를 동적으로 변경(`SET ...`)하면 로드밸런서나 ClickHouse Cloud 등 분산 환경에서 세션 유실로 파이프라인이 오동작할 위험이 있습니다.
* **profiles.yml 최적화**: 특정 쿼리 최적화 옵션(예: `join_use_nulls=1` 등)이 전역적으로 필요한 경우, SQL 훅이 아닌 `profiles.yml` 파일의 `custom_settings` 속성에 설정해두고 활용합니다.
* **`quote_columns` 비활성화**: ClickHouse의 이스케이프 문자 처리로 인한 불필요한 파싱 리소스를 아끼기 위해 `dbt_project.yml` 내에 `+quote_columns: false` 설정을 명시합니다.

#### [6] CDC 메타데이터 타임스탬프(ts_ms)의 KST 타임존 변환
Debezium CDC 수집 파이프라인에서 생성되는 이벤트 수집 시간(`ts_ms`)은 카프카 커넥트 런타임에 의해 UTC 기준으로 기록됩니다. 이를 원천 DB 또는 로컬 시각(KST, UTC+9)과 정합성을 맞추기 위해 dbt Bronze 뷰 계층에서 명시적으로 타임존 변환 처리를 규정합니다.
* **표준 가이드**:
  dbt `01_bronze` 계층의 뷰 모델 작성 시, `ts_ms` 컬럼에 대해 반드시 `toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms` 처리를 강제하여 downstream 모델(Silver/Gold)들이 로컬 타임라인을 일관되게 조회하고 분석할 수 있도록 보장해야 합니다.

11. 데이터 카탈로그 및 AI 에이전트 질의 최적화 표준

본 프로젝트는 향후 LLM 기반 AI 에이전트가 자연어 쿼리를 해석하여 데이터베이스 테이블을 조인하고 정확한 데이터를 추출할 수 있도록 강력한 데이터 카탈로그 메타데이터 표준을 확립합니다. AI 에이전트가 그라운드 트루스로 사용할 메타데이터 및 카탈로그 관리 표준은 다음과 같습니다.

### 11.1 시맨틱 레이어 정의 (Data Dictionary & Relationships)
1. **물리 외래키(FK) 정보의 시스템 노출**: PostgreSQL(운영 DB) DDL 정의 시 외래키 제약조건(`FOREIGN KEY REFERENCES`)을 필수 선언하여 AI 에이전트가 `information_schema`를 통해 테이블 간의 조인 관계를 명시적으로 역추적할 수 있게 합니다.
2. **논리 관계의 문서화**: ClickHouse와 같이 물리 외래키 제약이 없는 환경의 경우, dbt `schema.yml` 내 `tests: - relationships`를 통해 논리 관계를 정형화하고, 마스터 카탈로그 문서인 [DATA_CATALOG.md](./notebook/DATA_CATALOG.md) 파일에 릴레이션 매핑 테이블을 지속 유지합니다.
3. **가명화 컬럼의 연계성 보장**: 개인정보 보호를 위해 해싱 처리되는 민감 식별자 컬럼(예: `customer_id`)은 테이블 전반에서 동일한 Salt 가명화 알고리즘(`SHA-256`)을 동시 적용하여, 마스킹 처리된 상태에서도 테이블 간 동등 조인(`JOIN`)이 안전하게 유지되도록 보장합니다.

### 11.2 DB 레벨 메타데이터 영구 주입 (Database Comments Sync)
1. **단일 진실 공급원(SSOT) 주석화**: 모든 테이블의 비즈니스 설명 및 컬럼에 대한 메타데이터 정의는 dbt 프로젝트의 `models/02_silver/schema.yml` 내 `description` 필드에 기록하는 것을 그라운드 트루스로 규정합니다.
2. **`persist_docs` 활성화**: dbt가 빌드될 때, `dbt_project.yml`에 설정된 `+persist_docs` 속성에 의거하여 ClickHouse 및 PostgreSQL 데이터베이스의 실제 스키마 테이블/컬럼 코멘트(`COMMENT ON` 또는 ClickHouse `COMMENT` 컬럼 데이터)에 이 설명들이 자동으로 영구 동기화 주입되도록 합니다.
3. **AI 에이전트 연계**: AI 에이전트는 DB 접속 즉시 테이블 주석 조회 쿼리(예: clickhouse `system.columns`의 `comment` 필드)를 우선 수행하여 최신 컬럼 사전 정보를 로드하여 추론에 활용해야 합니다.

### 11.3 ClickHouse ReplacingMergeTree FINAL 조회 표준
ClickHouse ReplacingMergeTree 엔진을 사용하는 실버 레이어 테이블에 대해 팩트/마트 가공 연산을 수행하거나 AI 에이전트가 직접 쿼리문을 작성할 때, 백그라운드 병합이 완료되지 않아 발생하는 변경 로그 중복 행 조회를 방지하기 위해 **반드시 테이블명 뒤에 `FINAL` 키워드를 기술하여 고유한 최신 데이터만 조회하도록 쿼리 규칙을 표준화**합니다.
- *예시*: `SELECT * FROM default.fact_orders FINAL WHERE order_status = 'Completed';`

### 11.4 레이어별 물리 데이터베이스 격리 표준 (ClickHouse 2계층 대응)
ClickHouse는 PostgreSQL과 달리 `Database > Schema > Table`과 같은 3단계 논리 구조를 지원하지 않으며, 오직 `Database > Table`의 2단계 구조만 허용합니다. 따라서 레이어별 시맨틱 격리를 위해 다음과 같이 물리 데이터베이스(Database)를 분리하여 스키마 격리 효과를 구현합니다.

1. **원천 CDC 영속 데이터베이스 (`default` 또는 `dw_raw`)**:
   - Kafka Connect가 실시간으로 수집한 원천 변경 이력 ReplacingMergeTree 테이블들(`stg_dim_calendar` 등)이 위치합니다.
2. **브론즈 정제 뷰 데이터베이스 (`analytics_bronze`)**:
   - `default` 영역의 테이블을 `source()` 매크로로 읽어와 날짜 정제 및 가명화를 적용한 뷰(View)들이 생성되는 영역입니다.
3. **실버 정제 테이블 데이터베이스 (`analytics_silver`)**:
   - 브론즈 뷰를 기반으로 ReplacingMergeTree 엔진을 통한 중복 제거 물리 테이블들이 실체화(Materialized Table)되는 영역입니다.
4. **골드 최종 마트 데이터베이스 (`analytics_gold`)**:
   - 비즈니스 요구사항에 따라 최종 결합 및 집계가 완료된 마트 모델(`mart_sales_summary` 등)이 적재되는 영역입니다.

dbt 빌드 실행 시 `dbt_project.yml`의 `+schema` 설정을 통해 해당 데이터베이스들이 자동으로 구분되어 생성 및 배포되도록 구성해야 합니다.

### 11.5 도메인 기반 dbt 모델 디렉토리 설계 표준
dbt 모델 관리의 효율성 및 모듈화를 극대화하기 위해, 브론즈(Bronze), 실버(Silver), 골드(Gold) 각 레이어 하위의 폴더 구조를 개별 테이블명이 아닌 **비즈니스 도메인(Domain)** 기준으로 일관되게 구조화합니다.

1. **표준 도메인 분류 기준**:
   - **`common`**: 전사 공통 마스터 코드 및 기준 데이터 (예: `dim_calendar`, `dim_location`)
   - **`customer`**: 고객 세그먼트, 프로필 및 활성 지표 데이터 (예: `dim_customer`, `fact_customer_rfm`)
   - **`product`**: 상품 정보 및 카테고리/브랜드 표준 (예: `dim_product`)
   - **`partner`**: 외부 연계 자원인 입점 셀러 및 소속 배송 기사 정보 (예: `dim_seller`, `dim_delivery_person`)
   - **`marketing`**: 캠페인 예산, 유입 채널 정보 및 광고비 집행 성과 (예: `dim_campaign`, `dim_channel`, `fact_marketing_spend`)
   - **`sales`**: 주문 거래, 반품/환불 상세 및 결제 수단 정보 (예: `fact_orders`, `fact_returns`, `dim_payment`)
   - **`logistics`**: 풀필먼트 물류 방식 및 배송 실적/지연 지표 (예: `dim_fulfillment`, `fact_fulfillment_performance`)
   - **`clickstream`**: 사용자 웹/앱 내 클릭 및 이벤트 활동 원시 로그

2. **메타데이터 파일 모듈화 규칙**:
   - `sources.yml` 및 `schema.yml` 등 dbt 설정 메타데이터 파일들은 단일 대형 파일로 병합하지 않고, 각 도메인 폴더 하위에 조각내어 **`sources.yml`** 및 **`schema.yml`**로 각각 관리하여 결합도를 낮추고 모듈 가독성을 확보합니다.

12. Airflow DAG 유효성 검증 및 임포트 에러 확인 표준 (DAG Validation Standard)

본 프로젝트는 Airflow 3.2.2 및 Task SDK 아키텍처 환경에서 배포되는 모든 DAG에 대해, 런타임 오류 및 임포트 예외를 사전에 감지하고 차단하기 위한 통일된 검증 절차를 표준화합니다.

### 12.1 로컬 Windows 개발 OS의 한계와 검증 철학
1. **OS 및 시스템 라이브러리 비호환성**:
   - Windows 호스트 환경에서는 POSIX 전용 시스템 호출(예: `fcntl` 모듈)의 부재로 인해 로컬 파이썬 인터프리터 수준에서 Airflow DAG를 직접 파싱하거나 실행하는 것이 원천적으로 불가능합니다.
2. **패키지 및 종속성 불일치**:
   - `astronomer-cosmos`, `clickhouse-connect` 등 컨테이너 내부 환경에만 설치된 파이썬 라이브러리가 로컬 호스트 PC에 구축되지 않은 경우가 많아, 로컬 파이썬 실행 시 무수한 `ModuleNotFoundError`를 유발합니다.
3. **그라운드 트루스 검증**:
   - 따라서 DAG의 최종 유효성 검증은 **실 구동 대상인 Docker 컨테이너 환경 내부에서 에어플로우 실행 계정의 보안 컨텍스트로 수행하는 것**을 표준 그라운드 트루스로 지정합니다.

### 12.2 컨테이너 기반 2단계 DAG 검증 절차

개발이 완료되거나 수정된 DAG는 배포 전 반드시 다음 2단계의 터미널 명령을 통해 무결성을 검증해야 합니다.

#### [1단계] 컨테이너 내부 Python 직접 파싱 테스트 (Syntax & Import 검증)
컨테이너의 Python 환경을 빌려 DAG 파일을 직접 컴파일/실행하여 문법(SyntaxError) 및 임포트 누락(ModuleNotFoundError)을 최우선으로 차단합니다.
- **실행 명령어**:
  ```bash
  docker exec -i --user airflow <Scheduler-컨테이너-이름> python /opt/airflow/dags/<검증할-dag-파일명>.py
  ```
  *실제 예시 (local_ecommerce_pipeline.py 검증)*:
  ```bash
  docker exec -i --user airflow airflow-322_local_sandbox-airflow-scheduler-1 python /opt/airflow/dags/local_ecommerce_pipeline.py
  ```
- **핵심 통제 조치 (`--user airflow`)**:
  - `--user airflow` 옵션을 생략하면 컨테이너 기본값인 `root` 사용자로 진입하게 됩니다. `root` 계정은 `/home/airflow/.local` 하위의 user site-packages(예: `airflow`, `cosmos`, `dbt_common_config`)를 로드하지 못해 잘못된 임포트 에러를 출력할 수 있습니다. **반드시 `airflow` 사용자로 실행하도록 컨텍스트를 고정하십시오.**
  - 정상 실행 시 오류 메시지(Stderr) 없이 깔끔하게 종료(Exit Code 0)되거나 Cosmos 파싱 로그가 출력되어야 합니다.

#### [2단계] Airflow CLI를 이용한 DAG 메타데이터 등록 검증 (Scheduler 인지 검증)
에어플로우 스케줄러와 데이터베이스가 해당 DAG를 정상적인 워크플로우로 등록하였는지, 파싱 주기 동안 숨겨진 Import Error가 없었는지 CLI 명령어로 최종 확인합니다.
- **실행 명령어**:
  ```bash
  docker exec -i --user airflow <Scheduler-컨테이너-이름> airflow dags list
  ```
  *실제 예시*:
  ```bash
  docker exec -i --user airflow airflow-322_local_sandbox-airflow-scheduler-1 airflow dags list
  ```
- **확인 사항**:
  - 명령 실행 결과 테이블 목록에 본인이 개발한 `dag_id`가 빠짐없이 등재되어 있는지 대조합니다.
  - 임포트 오류가 존재하는 경우, `list` 명령어의 결과 상단 혹은 CLI 오류 출력부에 구체적인 Stack Trace 에러가 출력되므로, 이를 통해 숨은 예외를 식별할 수 있습니다.

### 12.3 대표적인 임포트 오류 사례 및 대응 가이드

1. **`ModuleNotFoundError: No module named 'cosmos'`**
   - **원인**: astronomer-cosmos 패키지가 Airflow 컨테이너 빌드 이미지에 포함되지 않았을 때 발생합니다.
   - **조치**: [Dockerfile](./notebook/airflow_3_2/Dockerfile)의 `pip install` 목록 또는 `requirements.txt`에 `astronomer-cosmos`가 누락되었는지 확인하고 재빌드(`--build`)를 수행하십시오.

2. **`ModuleNotFoundError: No module named 'dbt_common_config'`**
   - **원인**: `notebook/airflow_3_2/plugins` 경로 하위에 플러그인을 정상 생성했음에도, Docker 볼륨 바인딩 설정 누락 또는 런타임 경로 지정 오류로 발생합니다.
   - **조치**: [docker-compose.yaml](./notebook/airflow_3_2/docker-compose.yaml)의 volumes 마운트에 `./plugins:/opt/airflow/plugins:ro`가 지정되어 있는지, 그리고 파이썬이 `import dbt_common_config` 또는 `from dbt_common_config import ...` 형태로 적절하게 참조하는지 확인합니다.

3. **`ModuleNotFoundError: No module named 'airflow'` (CLI 구동 시)**
   - **원인**: `docker exec` 수행 시 root 계정으로 명령어(`airflow dags list`)를 호출하여 에어플로우 파이썬 라이브러리 경로가 차단당한 경우입니다.
   - **조치**: 명령어에 반드시 `--user airflow` 옵션을 추가하여 실행 사용자를 에어플로우 공식 계정으로 스위칭하십시오.

---

## 13. 실시간 스트리밍 인프라 장애 트러블슈팅 가이드 (Troubleshooting)

실시간 데이터 파이프라인(Kafka, Connect, ClickHouse, Flink)의 테스트 및 운영 과정에서 마주하기 쉬운 대표적 인프라 에러와 실무 해결 시나리오입니다.

### 13.1 rdkafka 연결 실패 및 호스트 해결 오류 (`Failed to resolve 'kafka:9092'`)
* **현상**: 호스트 PC(노트북)에서 기동한 Python 트래픽 생성기가 카프카 브로커 `localhost:9092` 연결 시도 후 `Failed to resolve 'kafka:9092'` 에러로 전송 실패.
* **원인**: 카프카 브로커에 접속 성공 시 카프카가 반환하는 내부 Advertised Listener 정보인 `kafka:9092`를 호스트 컴퓨터가 네트워크 DNS단에서 직접 찾아가지 못해 발생 (카프카 리스너 설계의 물리적 격리 제약).
* **조치**: 외부 기기 및 호스트 환경에서 카프카 브로커에 접근할 때는 반드시 `KAFKA_LISTENERS`에 맵핑된 **EXTERNAL 포트인 `9094`**로 카프카 브로커 경로를 지정하여 통신해야 합니다. (`KAFKA_BROKER = localhost:9094` 또는 `TAILSCALE_IP:9094`).

### 13.2 카프카 커넥트 offset/config 토픽 cleanup.policy 검증 오류 (`ConfigException`)
* **현상**: 카프카 브로커 재기동 이후 카프카 커넥트(Connect) 컨테이너가 즉사(Exit)하며 로그에 `offset.storage.topic is required to have cleanup.policy=compact` 등의 ConfigException 발생.
* **원인**: 카프카 브로커의 `KAFKA_NUM_PARTITIONS` 변경 등으로 인해 내부 메타데이터 적재 토픽들이 자동 생성될 때, 카프카 기본값인 `cleanup.policy=delete`로 오설정 생성되어 커넥트의 멱등성 검증 루프를 통과하지 못한 현상.
* **조치**: 카프카 관리 CLI 도구를 활용해 아래와 같이 커넥터 내부 보관용 토픽의 클린업 정책을 `compact`로 변경하십시오.
  ```bash
  docker exec ecommerce-kafka kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name ecommerce_connect_offsets --alter --add-config cleanup.policy=compact
  docker exec ecommerce-kafka kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name ecommerce_connect_configs --alter --add-config cleanup.policy=compact
  docker exec ecommerce-kafka kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name ecommerce_connect_statuses --alter --add-config cleanup.policy=compact
  ```

### 13.3 config.storage.topic 파티션 개수(1개 초과) 불일치 오류
* **현상**: 커넥트 로그에 `config.storage.topic is required to have a single partition ... but found 3 partitions` 에러와 함께 기동 실패.
* **원인**: 브로커의 전역 파티션 기본값(`KAFKA_NUM_PARTITIONS=3`)에 의해 Connect 설정 보관 토픽(`ecommerce_connect_configs`)이 3개 파티션으로 자동 생성됨. 카프카 커넥트는 설정 정보의 일관성을 위해 이 토픽이 반드시 단 1개의 파티션으로 구성될 것을 요구함. (파티션 축소는 카프카 스펙상 불가).
* **조치**: 커넥트 컨테이너를 일시 중지시킨 후, 기존 토픽을 강제 삭제하고 파티션 개수 1개 규격으로 재생성하여 극복합니다.
  ```bash
  # 1. 커넥트 컨테이너 일시 중지
  docker stop ecommerce-connect
  # 2. 기존 다중 파티션 configs 토픽 삭제
  docker exec ecommerce-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic ecommerce_connect_configs
  # 3. 1개 파티션 및 compact 정책으로 configs 토픽 강제 재생성
  docker exec ecommerce-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic ecommerce_connect_configs --partitions 1 --replication-factor 1 --config cleanup.policy=compact
  # 4. 커넥트 재시작
  docker start ecommerce-connect
  ```
