# EPSP (E-Commerce Pipeline with Segmented Platforms)

> **"물리 네트워크 단절을 극복하는 100% 오픈소스 기반 실시간 하이브리드 CDC 데이터 파이프라인"**

본 프로젝트는 퍼블릭 클라우드 관리형 서비스 비용을 **$0원**으로 통제하면서도, 프로덕션 수준의 실제 물리적 분산 환경을 모방하기 위해 집의 데스크톱(데이터 엔진)과 공유오피스의 노트북(제어 및 인입)을 **Tailscale Mesh VPN** 가상 사설망으로 결합하여 구동하는 분산 실시간 주문 및 재고 변동 CDC 파이프라인입니다.

---

## 1. 하이브리드 분산 아키텍처 토폴로지

```text
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
               │ (15분 크론 / OLAP SQL 명령)                       ▼
               └────────────────────────────────────────> (4) 실시간 데이터 웨어하우스 (DW)  │
                                              │  - ClickHouse (OLAP Engine)            │
                                              └────────────────────────────────────────┘
                              [ 가상 사설망 통신 기저: Tailscale Mesh VPN (WireGuard) ]
```

---

## 2. 5대 핵심 도전 과제와 기술적 극복 과정 (Problem - Analyze - Solution)

### 📌 Challenge 1: 인프라 운영 비용의 한계 ($0 Infra 제약)
* **Problem**: AWS RDS, Confluent Cloud, ClickHouse Cloud 등 프로덕션급 클라우드 데이터 스택을 구축하는 데 월 수백 달러의 비용이 발생하며, 단순 단일 VM 내부 Docker-Compose 구성은 실제 물리적인 분산 환경(레이턴시, 패킷 유실 등)을 대변하지 못합니다.
* **Analyze**: 가용한 로컬 컴퓨팅 리소스(데스크톱 PC + 개인 노트북)를 물리 노드로 활용하되, 공유오피스의 방화벽과 가정용 인터넷의 유동 IP 한계를 통제하고 두 노드를 가상 LAN 대역으로 안전하게 묶을 수 있는 네트워크 터널링 기술이 필요했습니다.
* **Solution**: **Tailscale VPN Overlay Network(WireGuard 기반)**를 탑재하여 서로 다른 서브넷 방화벽 뒤에 있는 물리 기기들을 가상 프라이빗 LAN(`100.X.X.X` 대역)으로 결합했습니다. 이를 통해 네트워크 장비 및 포트 포워딩 비용 없이 **인프라 비용 $0원**으로 물리적 분산 스트리밍 토폴로지를 실현했습니다.

### 📌 Challenge 2: ReplacingMergeTree 병합 지연에 따른 정합성 저하 vs CPU 과부하 트레이드오프
* **Problem**: Debezium PostgreSQL CDC는 행의 잦은 변경(Update, Delete) 로그를 무수히 쏟아냅니다. ClickHouse의 `ReplacingMergeTree`는 백그라운드에서 중복 데이터를 병합하므로, 쿼리 실행 시점에 최신 상태(정합성)가 보장되지 않는 문제가 있습니다.
* **Analyze**: 100% 실시간 정합성을 위해 쿼리마다 `FINAL` 키워드를 사용하면, 수백만 건 유입 시 ClickHouse의 병합 리소스와 CPU 점유율이 극심하게 늘어나 쿼리 응답 속도가 기하급수적으로 저하(평균 10배 이상)됩니다.
* **Solution**: `FINAL` 남용을 방지하기 위해 dbt 증분(Incremental) 모델 파이프라인에서 최신 데이터 가공 전략을 이원화했습니다.
  - dbt 변환 가공 시에는 **최신 타임스탬프(`ts_ms`) 정렬 및 삭제 플래그(`op != 'd'`)** 필터링을 dbt 증분 모델 조건에 적절히 혼합하여 로드 부하를 격리하고,
  - 일관성과 지연 연산의 트레이드오프를 타협하여 최종 매출 마트 테이블 생성에만 `FINAL` 또는 대체 Aggregation 함수(예: `argMax`)를 선별 적용하여 **쿼리 속도 개선과 CPU 과부하 예방**을 동시에 달성했습니다.

### 📌 Challenge 3: 고빈도 스트리밍 데이터 유입으로 인한 오케스트레이터 병목 및 리소스 고갈
* **Problem**: 카프카 토픽에 적재되는 실시간 데이터 흐름을 매 순간 오케스트레이션 엔진이 이벤트 드리븐 형태로 스케줄링할 경우, 메타 DB 커넥션 병목과 스케줄러 먹통 현상이 발생합니다.
* **Analyze**: 실시간 적재(Kafka/CDC)는 무중단으로 흐르되, OLAP DW 내부의 계산 모델링 변환 주기는 효율적으로 격리해야 리소스 낭비를 차단할 수 있습니다.
* **Solution**: 오케스트레이션을 **15분 단위의 마이크로 배치 크론 스케줄링**으로 격리하여 물리 하드웨어의 CPU/메모리 부하를 방지했습니다. 또한, 마트 적재가 성공적으로 완료되는 시점에만 **Airflow 3.x의 'Asset'(Data-Aware scheduling)** 메커니즘을 발동시켜 비정기 다운스트림 파이프라인(예: 재고 부족 알림)을 유기적으로 연쇄 트리거하는 **이벤트-배치 하이브리드 제어권**을 확보했습니다.

### 📌 Challenge 4: 물리 노드 간 일시적인 네트워크 단절로 인한 데이터 정합성 깨짐 위험
* **Problem**: 공유오피스 와이파이 단절 등으로 노트북(인입 노드)의 네트워크 연결이 불안정할 때, 주문 발생기가 데스크톱 Postgres로 데이터를 입력하지 못하고 커넥션 에러와 함께 데이터 유실이 초래될 수 있습니다.
* **Analyze**: 분산 환경에서 일시적 단절은 필연적으로 발생하는 예외(Edge Case)이므로, 애플리케이션 레벨에서 탄력적인 재시도 구조가 강제되어야 합니다.
* **Solution**: 파이썬 트래픽 시뮬레이터에 연결 복원력(Connection Resilience)을 갖춘 재시도 메커니즘(`psycopg2` 커넥션 루프 내 재시도 정책: 최대 5회, 백오프 3초)을 구현했습니다. 이를 통해 네트워크 복구 즉시 이전 버퍼 트랜잭션을 일괄 재입력할 수 있도록 보장했습니다.

### 📌 Challenge 5: 비동기 CDC 파이프라인의 데이터 누수 및 유실 모니터링 한계
* **Problem**: Postgres에서 ClickHouse까지 Kafka, Debezium Connect 등 여러 스테이지를 비동기로 거치기 때문에, 중간 과정에서 유실되거나 지연된 데이터가 있더라도 시스템 차원에서 이를 모니터링하기 어렵습니다.
* **Analyze**: 실시간으로 양쪽 데이터베이스를 지속적 대조하면 운영 DB 성능에 큰 타격을 주므로, 일별 비집중 시간대를 활용한 감사 추적이 최적 대안입니다.
* **Solution**: 매일 새벽 2시마다 실행되는 **데이터 정합성 사후 대조 감사(Data Reconciliation Audit) DAG**를 Airflow에 구축했습니다.
  - PostgreSQL의 최근 24시간 거래금액 합계 `SUM(total_price)`와,
  - ClickHouse DW에 복제된 stg_orders 테이블에 `FINAL` 제어어를 적용한 `SUM(total_price)` 값을 상호 대조하고,
  - **오차율이 0.01%를 초과하는 즉시 경고성 예외를 뿜어** 파이프라인 누수 여부를 100% 추적할 수 있도록 안전망을 구축했습니다.

---

## 3. 리포지토리 디렉토리 레이아웃 (Repository Layout)

```text
├── desktop/                         # [노드 1 데스크톱] 전용 인프라 컴포넌트
│   ├── source-db/                   # [도메인 1] 원천 데이터베이스 영역
│   │   ├── docker-compose-db.yaml   # Postgres 단독 컴포즈
│   │   └── postgres-init/
│   │       └── init.sql             # Postgres DDL 및 REPLICA IDENTITY 설정
│   └── data-pipeline/               # [데이터 플랫폼] 스트리밍 및 ClickHouse DW 영역
│       ├── docker-compose-pipeline.yaml # Kafka, Connect, ClickHouse 통합 컴포즈
│       ├── clickhouse-init/
│       │   └── init-db.sql          # ClickHouse ReplacingMergeTree DDL 및 TTL 설정
│       └── kafka-connect/
│           ├── Dockerfile           # Debezium + ClickHouse 어댑터 커스텀 이미지 빌드
│           ├── submit-pg-source-v2.json # Debezium PostgreSQL Source 커넥터 설정서
│           └── submit-ch-sink-v2.json   # ClickHouse Sink 커넥터 설정서
└── notebook/                        # [노드 2 노트북] 오케스트레이션 및 데이터 인입 컴포넌트
    ├── DATA_CATALOG.md              # [AI 에이전트용] 마스터 데이터 카탈로그 및 Few-Shot SQL 가이드
    ├── data-generator/              # 트래픽 유발 파이썬 엔진
    │   └── transaction_generator.py 
    ├── kaggle_data/                 # Kaggle Ecommerce DW 원천 데이터 및 전처리 스크립트
    │   └── preprocess_kaggle_data.py
    ├── dbt_clickhouse_dw/           # ClickHouse 전용 dbt 변환 가공 프로젝트
    │   ├── dbt_project.yml          # persist_docs 설정 포함
    │   ├── profiles.yml             # analytics 스키마 격리 적용
    │   └── models/
    │       ├── 01_bronze/           # 원천 1:1 매핑 및 마스킹/파싱 계층 (Bronze Layer)
    │       │   └── ecommerce/       # toDate 날짜 변환 및 SHA256 개인정보 가명화 뷰 정의
    │       ├── 02_silver/           # 비즈니스 정규화ReplacingMergeTree 계층 (Silver Layer)
    │       │   └── schema.yml       # DB 컬럼 코멘트 영구 동기화용 메타데이터 사전
    │       └── 03_gold/             # BI 시각화 및 집계용 (OBT) 계층 (Gold Layer)
    │           └── executive/       # mart_sales_summary (One Big Table) 매출 분석 마트
    └── airflow_3_2/                 # Airflow 3.2.2 샌드박스
        ├── docker-compose.yaml
        └── dags/
            ├── local_ecommerce_pipeline.py  # 15분 크론 / Asset 연쇄 체인 메인 DAG
            └── data_reconciliation_audit.py # Postgres vs ClickHouse 데이터 원장 정합성 검증 DAG
```

---

## 4. AI 에이전트 질의 최적화 & 데이터 카탈로그 표준

본 프로젝트는 향후 LLM 기반 AI 에이전트가 자연어로 데이터베이스에 질의하고 정확하게 조인을 수행하여 데이터를 추출할 수 있도록 강력한 데이터 시맨틱 표준을 확립했습니다.

1. **DB 레벨 메타데이터 영구 주입 (`persist_docs`)**:
   - `models/02_silver/schema.yml`에 한글 칼럼 설명과 관계 사전을 SSOT(단일 진실 공급원)으로 통합 관리합니다.
   - dbt 빌드 시 ClickHouse의 `system.columns` 코멘트 메타데이터에 이 설명들이 영구적으로 자동 동기화 주입되도록 구현하여, AI 에이전트가 DB 연결 즉시 풍부한 한글 컬럼 사전을 참조하도록 구성했습니다.
2. **가명화 컬럼 조인성 보장**:
   - 개인정보 식별자인 `customer_id`는 전 계층에서 동일한 Salt 알고리즘과 대문자 **`SHA256`** 함수를 결합해 가명화 처리하여, 암호화 상태에서도 실버/골드 계층 간의 정확한 조인(`JOIN`)을 지원합니다.
3. **ClickHouse FINAL 조회 구문 우회 표준**:
   - ReplacingMergeTree 테이블의 비동기 병합 문제를 해소하기 위해 **`(SELECT * FROM table FINAL) as alias`** 서브쿼리 구조로 SQL을 최적화하여 구문 오류(Missing Columns) 및 파싱 복잡성을 차단했습니다.
- AI 에이전트 상세 가이드 및 Few-Shot SQL: [DATA_CATALOG.md](file:///d:/01.DEV/EPSP/notebook/DATA_CATALOG.md) 참조

---

## 5. 시작 가이드 (Quick Start)

### 🛠️ 전제 조건 (WSL2 리소스 제한 설정)
Windows 호스트 환경(특히 노트북 노드)에서 Docker Desktop 및 WSL2를 구동할 때, 가상 메모리 프로세스(`vmmemWSL`)의 과도한 리소스 점유와 시스템 프리징을 방지하기 위해 사용자 프로필 경로(`%USERPROFILE%`)에 `.wslconfig` 설정을 강제 설정할 것을 권장합니다.

* **파일 경로**: `C:\Users\<사용자명>\.wslconfig`
```ini
[wsl]
memory=6GB
processors=4
pageReporting=true
```

---

### 🚀 Phase 1: 단일 로컬 환경 검증 (Monolithic MVP)
네트워크 연결 복잡성을 최소화하고 전체 파이프라인 동작을 1대의 로컬 머신에서 먼저 검증합니다.

1. **인프라 컨테이너 구동 (데스크톱)**:
   ```bash
   cd desktop
   docker-compose -f docker-compose-infra.yaml up -d
   ```
2. **트래픽 생성기 실행**:
   `TAILSCALE_DESKTOP_IP` 환경 변수를 지정하지 않으면 기본값인 `localhost`로 Postgres DB에 연결을 시도합니다.
   ```bash
   cd notebook/data-generator
   pip install psycopg2-binary faker
   python transaction_generator.py
   ```
3. **Airflow 및 dbt 구동**:
   각 가공 엔진의 호스트 주소를 `localhost`로 세팅하여 배치 및 마트 가공이 정합성 있게 동작하는지 모니터링합니다.

---

### 🚀 Phase 2: Tailscale 하이브리드 분산화 (Hybrid Expansion)
물리 기기 2대를 네트워크로 격리한 후 실시간 분산 수집을 가동합니다.

1. **Tailscale 가상 사설망 연결**:
   - 두 노드(데스크톱, 노트북)에 Tailscale 클라이언트를 설치하고 로그인하여 동일 네트워크 대역(`100.X.X.X`)으로 묶습니다.
2. **데스크톱 인프라 구동**:
   - `.env`에 `TAILSCALE_DESKTOP_IP=100.X.X.X` (데스크톱 IP)를 지정한 뒤 인프라를 구동하여 Kafka 외부 리스너에 바인딩합니다.
3. **노트북 트래픽 전송**:
   - 노트북에서 `TAILSCALE_DESKTOP_IP`를 환경 변수로 내보낸 후 주문 생성기를 실행하여 사설망을 통과해 데스크톱으로 전송되도록 유도합니다.
   ```bash
   export TAILSCALE_DESKTOP_IP=100.X.X.X
   python transaction_generator.py
   ```
