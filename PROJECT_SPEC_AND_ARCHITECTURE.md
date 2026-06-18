본 문서는 물리적으로 분리된 두 기기(집의 데스크톱 PC와 공유오피스의 노트북)를 가상 사설망(Tailscale VPN)으로 결합하여 구동하는 '100% 오픈소스 기반 이커머스 실시간 주문 및 재고 변동 파이프라인'의 최종 마스터 명세서입니다. AI 에이전트가 코드를 생성하고 인프라를 빌드할 때 지켜야 할 절대적인 그라운드 트루스(Ground Truth) 자산입니다.

1. 프로젝트 개요 및 분산 아키텍처 (Architecture Overview)
본 프로젝트는 클라우드 관리형 서비스 비용을 $0로 통제하면서도 프로덕션 수준의 분산 환경을 모방하기 위해 물리 2노드 하이브리드 아키텍처를 채택합니다. 두 기기는 인터넷망 기반의 Tailscale Mesh VPN Overlay Network를 통해 하나의 가상 LAN 대역(100.X.X.X)으로 묶여 통신합니다.

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

2. 노드별 상세 기술 스펙 (Node Specifications)
2.1 [노드 1] 데스크톱 PC: 데이터 인프라 엔진
데스크톱은 데이터의 영속성 저장과 고속 스트리밍 처리를 전담합니다. Docker 내부의 공유 브리지 네트워크 ecommerce-data-network를 기반으로 모든 인프라가 연동됩니다.

원천 DB (PostgreSQL 15-alpine): wal_level = logical 설정을 적용하여 실시간 CDC의 원천 로그(WAL)를 생성합니다. Airflow 메타 DB와의 포트 충돌을 피하기 위해 외부 호스트 포트는 5433으로 포워딩합니다.

메시지 브로커 (Apache Kafka 7.4.0): 주키퍼(Zookeeper)를 배제한 KRaft 모드로 가동하여 리소스를 절감합니다.

네트워크 핵심: 외부 네트워크(노트북)에서 들어오는 프로듀서/컨슈머를 수용하기 위해 KAFKA_ADVERTISED_LISTENERS에 데스크톱의 Tailscale 고정 IP(100.X.X.X:9092)를 바인딩합니다.

수집 엔진 (Debezium Connect 2.4): PostgreSQL의 WAL을 읽어 카프카 토픽으로 쏘는 Source 커넥터와, 이를 ClickHouse로 밀어 넣는 Sink 커넥터를 REST API 기반 멀티 태스크로 구동합니다.

실시간 DW (ClickHouse Server): 단일 컨테이너로 초당 수만 건의 로깅을 실시간 소화하는 대용량 OLAP 데이터베이스입니다. 중복 제거를 위해 ReplacingMergeTree 엔진을 사용하며, dbt 가공을 위한 8123(HTTP) 포트를 외부(Tailscale)에 개방합니다.

2.2 [노드 2] 노트북: 오케스트레이션 및 데이터 인입
노트북은 중앙 제어 지휘소 역할을 수행하며, 공유오피스 와이파이망 환경에서 데스크톱 인프라로 원격 명령을 송신합니다.

트래픽 생성기 (Python Script): Faker 및 Confluent-Kafka 라이브러리를 기반으로 이커머스의 실시간 주문 및 결제 취소, 재고 변동 이벤트를 시뮬레이션하여 데스크톱의 Postgres DB(100.X.X.X:5433)에 트랜잭션을 인입합니다. 네트워크 지연을 방지하기 위해 batch.size=65536 및 linger.ms=20 튜닝을 반영합니다.

워크플로우 엔진 (Apache Airflow 3.2.2): 사용자가 구축한 로컬 샌드박스 컴포즈 환경 내부에서 구동됩니다. 24시간 스트리밍 처리를 에어플로우에 맡기지 않고, 15분 주기 마이크로 배치 크론 스케줄링을 통해 가공 부하와 비용을 통제합니다.

가공 및 모델링 (dbt-core + dbt-clickhouse): 에어플로우 스케줄러의 통제를 받아 데스크톱의 ClickHouse 서버에 커넥션을 맺고, 원천 로그 테이블에서 비즈니스 마트 테이블로의 SQL 변환 연산을 ClickHouse 내부 컴퓨팅 리소스를 활용해 수행합니다.

3. 데이터 컨트랙트 및 스키마 정의 (Data Contracts)
3.1 원천 운영 DB (PostgreSQL DDL)

CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    stock_quantity INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) REFERENCES products(product_id),
    quantity INT NOT NULL,
    total_price DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'CREATED', 'CANCELLED', 'RETURNED'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
    
    ALTER TABLE default.stg_orders MODIFY TTL ts_ms + INTERVAL 7 DAY;

    2. 데이터 보안 및 가명화 (Masking): 민감 정보인 고유 사용자 식별자(user_id)는 dbt staging 모델 
    연산 과정에서 외부 유출을 차단하기 위해 고유 솔트(Salt) 값을 결합한 SHA-256 단방향 해시 
    알고리즘으로 난독화 가명 처리를 강제합니다.

    3. 데이터 정합성 사후 대조 감사 (Data Reconciliation): Airflow 3.2.2 스케줄러 내부에 24시간 주기로 작동하는 data_reconciliation_audit DAG를 구축합니다. 이 DAG는 원천 Postgres DB의 특정 시간대 SUM(total_price) 연산 결과와, ClickHouse의 동일 시간대 테이블에 FINAL 제어어를 적용한 연산 결과를 상호 대조하여 격차가 발생할 경우 예외 경고(Slack Alert 등)를 발생시키는 감사 추적성을 확보합니다.

5. Airflow 3.2.2 신기능 'Asset' 체인 스케줄링 전략
고빈도 스트리밍 파일이 유입될 때 스케줄러가 터지는 현상을 막기 위해, 상위 수집 및 DW 물리 가공은 15분 크론 주기로 묶고, 후속 다운스트림 비정기 연산에 한해서만 Airflow 3.x Asset Data-Aware 트리거를 연쇄시킵니다.

from airflow.models.dag import DAG
from airflow.sdk import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 1. dbt 변환이 완료될 최종 ClickHouse 매출 마트 테이블을 상위 핵심 자산(Asset)으로 등록
CLICKHOUSE_ORDER_MART_ASSET = Asset(uri="clickhouse://default/fact_orders_hourly")

# [DAG 1] 15분 주기 주기적 배치 파이프라인 (인프라 안정성 확보)
with DAG(dag_id="batch_15min_dw_transform", schedule="*/15 * * * *", catchup=False) as dag_1:
    run_dbt_transform = SQLExecuteQueryOperator(
        task_id="execute_dbt_marts",
        conn_id="clickhouse_desktop",
        sql="-- dbt run invocation wrapper",
        outlets=[CLICKHOUSE_ORDER_MART_ASSET] # 태스크 성공 시 자산 이벤트 방출
    )

# [DAG 2] 데이터가 갱신되었을 때만 유기적으로 반응하는 이벤트 기반 다운스트림 (Data-Aware)
with DAG(dag_id="reactive_stock_alert_pipeline", schedule=[CLICKHOUSE_ORDER_MART_ASSET], catchup=False) as dag_2:
    check_stock_leak = SQLExecuteQueryOperator(
        task_id="detect_low_stock_and_notify",
        conn_id="clickhouse_desktop",
        sql="SELECT product_id, sum(quantity) FROM fact_orders_hourly GROUP BY product_id;"
    )

6. 리포지토리 디렉토리 레이아웃 가이드

├── desktop/                         # [노드 1 데스크톱] 전용 인프라 컴포넌트
│   ├── docker-compose-infra.yaml   # 전용 인프라 컴포즈 (Postgres, Kafka, Connect, ClickHouse)
│   ├── clickhouse-init/
│   │   └── init-db.sql
│   ├── postgres-init/
│   │   └── init.sql
│   └── kafka-connect/
│       ├── Dockerfile
│       ├── submit-pg-source.json   # Debezium Postgres CDC 연결 설정서
│       └── submit-ch-sink.json     # ClickHouse Sink 커넥터 연결 설정서
└── notebook/                        # [노드 2 노트북] 오케스트레이션 및 데이터 인입 컴포넌트
    ├── data-generator/
    │   └── transaction_generator.py # 트래픽 유발 파이썬 엔진 (Tailscale IP 주입)
    ├── dbt_clickhouse_dw/           # ClickHouse 전용 dbt 변환 가공 프로젝트
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    │   └── models/
    │       ├── staging/            # user_id SHA-256 마스킹 및 op 파싱
    │       └── marts/              # ReplacingMergeTree 기반 FINAL 문법 및 증분 마트 모델
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
CLICKHOUSE_MART_ASSET = Asset(uri="clickhouse://default/fact_orders_hourly")

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

    @task(task_id="trigger_dbt_marts", outlets=[CLICKHOUSE_MART_ASSET])
    def run_marts(watermark_time):
        print(f"Running dbt marts with watermark: {watermark_time}")
        # ClickHouse OLAP 쿼리 및 dbt 변환 가공 로직 수행...
        return "SUCCESS"

    # Task 간 의존성 연결
    watermark = get_watermark()
    run_marts(watermark)

my_pipeline()
```