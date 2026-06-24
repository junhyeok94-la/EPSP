# EPSP Project Spec & Architecture

본 문서는 EPSP(E-Commerce Pipeline with Segmented Platforms)의 마스터 스펙이다. 기존 백업은 `PROJECT_SPEC_AND_ARCHITECTURE.md.bak-20260624`에 보관되어 있으며, 상세 스펙 및 개발 표준 가이드는 토큰 절약과 작업 효율을 위해 분리하여 관리한다.

## 📌 분할된 상세 스펙 문서 리스트 (Specifications)
AI 에이전트 및 개발자는 실제 세부 구현이나 개발 시 아래의 특화 스펙 파일을 반드시 우선 참조한다.
1. **[DDL 계약 명세서 (ddl_contracts.md)](file:///d:/01.DEV/EPSP/specs/ddl_contracts.md)**: PostgreSQL 원천 DB DDL 및 ClickHouse ReplacingMergeTree landing DDL.
2. **[dbt 개발 표준 가이드 (dbt_standards.md)](file:///d:/01.DEV/EPSP/specs/dbt_standards.md)**: dbt 디렉토리 규칙, 훅, 표준 SQL 헤더 주석 템플릿.
3. **[Airflow 개발 표준 가이드 (airflow_standards.md)](file:///d:/01.DEV/EPSP/specs/airflow_standards.md)**: Airflow 3.x Task SDK 가이드, Connection 명세 CLI 및 런타임 제약.
4. **[인프라 장애 트러블슈팅 가이드 (troubleshooting.md)](file:///d:/01.DEV/EPSP/specs/troubleshooting.md)**: rdkafka DNS, ConfigException compact, configs partition 개수 에러 등.
5. **[에이전트 제약 규칙서 (.agent_rules.yaml)](file:///d:/01.DEV/EPSP/.agent_rules.yaml)**: 기계 가독형 압축 제약조건 YAML 파일.

---

## 1. 프로젝트 목표

### 1.1 학습 및 포트폴리오 목표
- PostgreSQL 운영 DB에서 발생한 데이터를 Kafka/Debezium CDC로 수집한다.
- ClickHouse에 실시간/준실시간 분석용 원천 테이블을 적재한다.
- dbt로 Bronze, Silver, Gold 계층의 DW 모델을 만든다.
- Airflow 3.2.2로 15분 마이크로 배치와 데이터 정합성 감사 DAG를 운영한다.
- dbt manifest, schema metadata, data catalog, Neo4j Graph RAG를 이용해 AI 에이전트가 이해 가능한 semantic metadata layer를 구축한다.

### 1.2 AX 후속 프로젝트 목표
AX 프로젝트로 이어지려면 DW는 AI 에이전트가 안전하게 탐색하고 질의할 수 있는 지식 기반이어야 한다. 따라서 EPSP의 DW는 다음 조건을 만족해야 한다.
- 테이블, 컬럼, 관계, 데이터 타입, grain, refresh policy, owner, sensitivity, quality rule을 기계가 읽을 수 있는 형태로 보존한다.
- dbt `schema.yml`과 `sources.yml`을 데이터 카탈로그의 SSOT로 삼는다.
- ClickHouse `system.tables`, `system.columns`, dbt `manifest.json`, Neo4j graph metadata가 서로 모순되지 않게 관리한다.
- AI 에이전트가 Text-to-SQL을 만들 때 사용해야 할 join path, FINAL 사용 규칙, masking rule, time zone rule, 금지된 anti-pattern을 명시한다.
- 운영 데이터와 분석 데이터의 차이, CDC 지연, ReplacingMergeTree merge latency 같은 물리적 제약을 에이전트 프롬프트와 카탈로그에 함께 노출한다.

---

## 2. 현재 리포지토리 기준 아키텍처

현재 구현의 본류는 Olist Kaggle 이커머스 데이터셋을 기반으로 한 PostgreSQL -> Kafka/Debezium -> ClickHouse -> dbt -> Airflow -> Graph RAG 흐름이다.

```text
D:\01.DEV\EPSP
├── desktop
│   ├── source-db
│   │   ├── docker-compose-db.yaml
│   │   └── postgres-init/init.sql
│   └── data-pipeline
│       ├── docker-compose-pipeline.yaml
│       ├── clickhouse-init/init-db.sql
│       └── kafka-connect
│           ├── Dockerfile
│           ├── register_connectors.py
│           ├── submit-pg-source.json
│           ├── submit-pg-source-v2.json
│           ├── submit-ch-sink.json
│           └── submit-ch-sink-v2.json
├── notebook
│   ├── DATA_CATALOG.md
│   ├── kaggle_data
│   │   ├── preprocess_kaggle_data.py
│   │   └── load_kaggle_to_clickhouse.py
│   ├── data-generator
│   │   ├── transaction_generator.py
│   │   └── test_transaction_generator.py
│   ├── dbt_clickhouse_dw
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   ├── dbt_to_neo4j.py
│   │   ├── graph_rag_qa.py
│   │   ├── README_GRAPH_RAG.md
│   │   └── models
│   │       ├── 01_bronze
│   │       ├── 02_silver
│   │       └── 03_gold
│   └── airflow_3_2
│       ├── Dockerfile
│       ├── docker-compose.yaml
│       ├── plugins/dbt_common_config.py
│       └── dags
│           ├── local_ecommerce_pipeline.py
			└── data_reconciliation_audit.py
└── README.md
```

### 2.1 물리 노드 구상
- 데스크톱 노드: PostgreSQL, Kafka, Kafka Connect, ClickHouse, Flink, Kafka UI를 담당한다.
- 노트북 노드: Airflow, dbt, 데이터 생성/적재 스크립트, Graph RAG 실험을 담당한다.
- Tailscale VPN: 두 물리 노드를 `100.x.x.x` 대역으로 묶어 Kafka external listener, Postgres, ClickHouse 접근 경로를 제공한다.
- 단일 로컬 검증: 처음에는 모든 서비스를 `localhost` 기준으로 띄우고, 이후 `TAILSCALE_DESKTOP_IP` 환경 변수로 분산 노드 구성을 확장한다.

### 2.2 서비스 포트

| 서비스 | 위치 | 포트 | 용도 |
| --- | --- | --- | --- |
| PostgreSQL source DB | `desktop/source-db` | `15432:5432` | Olist 원천 운영 DB |
| Kafka internal | `desktop/data-pipeline` | `9092` | Docker network 내부 브로커 |
| Kafka external | `desktop/data-pipeline` | `9094` | 호스트/Tailscale 접근 브로커 |
| Kafka Connect | `desktop/data-pipeline` | `8083` | Debezium/ClickHouse connector REST |
| ClickHouse HTTP | `desktop/data-pipeline` | `8123` | dbt/Airflow/스크립트 접근 |
| ClickHouse native | `desktop/data-pipeline` | `9000` | Native client 접근 |
| Flink Dashboard | `desktop/data-pipeline` | `8081` | 스트리밍 처리 모니터링 |
| Kafka UI | `desktop/data-pipeline` | `8089` | Kafka/connector 모니터링 |
| Airflow API/Web | `notebook/airflow_3_2` | `8080` | DAG 운영 UI |
| Neo4j Browser | 별도 Docker | `7474` | Graph RAG 메타데이터 탐색 |
| Neo4j Bolt | 별도 Docker | `7687` | Graph RAG 적재/질의 |

---

## 3. 현재 구현 상태와 중요한 갭

현재 `notebook/data-generator/transaction_generator.py`는 `users`, `products`, `orders`, `payments` 테이블을 기대한다. 하지만 `desktop/source-db/postgres-init/init.sql`은 Olist 테이블(`olist_customers`, `olist_orders` 등)만 만든다.

따라서 다음 중 하나를 선택하기 전까지 `transaction_generator.py`는 본류 파이프라인의 실행 기준으로 보면 안 된다.
1. Olist 스키마 기반으로 generator를 다시 작성한다.
2. `users/products/orders/payments` OLTP 스키마를 별도 실시간 이벤트 시뮬레이션 도메인으로 추가한다.
3. generator는 Kafka clickstream/Flink 실험 전용으로 분리하고, Olist DW와는 다른 스펙 문서로 관리한다.

현재 스펙의 기본 결정은 **Olist Kaggle DW를 canonical domain으로 유지**하는 것이다.

---

## 4. 데이터 계층 설계 (High-level Summary)
각 계층별 구체적인 스키마 구조 및 DDL은 **[ddl_contracts.md](file:///d:/01.DEV/EPSP/specs/ddl_contracts.md)** 문서를 참조한다.

* **4.1 Source: PostgreSQL Olist 운영 DB**: Olist CSV를 적재하는 운영 원천 DB.
* **4.2 Landing/Raw: ClickHouse `default.stg_olist_*`**: Kafka Connect가 수집하는 CDC Landing 영역. ReplacingMergeTree(ts_ms) 및 TTL 7일이 필수적이다.
* **4.3 Bronze: dbt `01_bronze`**: landing table을 KST 시간대 통일 및 가명화를 수행하여 정제 View로 노출.
* **4.4 Silver: dbt `02_silver`**: 비즈니스 정규화 차원/팩트 모델(`dim_customers`, `fact_orders` 등)이 구현되는 실질적인 엔터프라이즈 중심 영역.
* **4.5 Gold: dbt `03_gold`**: BI/AX 소비 요약 마트 계층(`mart_daily_sales_wide` 등).

---

## 5. AX 대응 메타데이터 설계 표준

### 5.1 dbt metadata를 SSOT로 삼는다
`dbt_project.yml`에 `persist_docs` 설정이 반영되어 있으므로, 컬럼 레벨 메타데이터가 ClickHouse에 동기화된다. 
AI 에이전트의 Text-to-SQL 성능 고도화를 위해 각 모델별 `schema.yml` 내에 `meta` 속성(grain, owner, sensitivity, priority 등)을 필수로 정의해야 한다.

### 5.2 AI 에이전트용 카탈로그 산출물
에이전트가 참조해야 하는 카탈로그는 세 단계로 관리한다.
1. Human-readable catalog: `notebook/DATA_CATALOG.md`
2. dbt machine metadata: `target/manifest.json`, `target/catalog.json`
3. Graph metadata: Neo4j에 적재한 Model, Source, Column, DEPENDS_ON 관계

### 5.3 Graph RAG 설계 표준
- `dbt_to_neo4j.py`를 통해 manifest 정보를 Neo4j에 주입한다.
- Column 노드의 key는 모델별 namespace를 포함한 복합 식별자(`model_unique_id + column_name`)를 사용하여 이름 충돌을 피해야 한다.
- Text-to-SQL 동작 시, LLM이 생성한 쿼리를 ClickHouse에 전달하기 전 `EXPLAIN` 혹은 `LIMIT` 등을 이용해 검증 단계를 반드시 거치도록 설계한다.

### 5.4 AI 질의 작성 규칙
- Gold 모델을 최우선 사용하고, 상세 분석은 Silver를 사용한다. (Landing 테이블 직접 조회 금지)
- ReplacingMergeTree 정합성을 위해 최신 질의 시 `FINAL` 키워드를 제한적으로 활용한다.
- 날짜/시간 인덱스 필터를 최우선으로 사용하여 Full-scan을 차단한다.

---

## 6. 실행 방법

### 6.1 사전 조건
- Docker Desktop / Docker Compose v2
- Python 3.10+ / dbt-core & dbt-clickhouse
- Windows 호스트 환경에서는 메모리 고갈 방지를 위해 `%USERPROFILE%\.wslconfig`에 아래 설정을 권장한다.
  ```ini
  [wsl]
  memory=6GB
  processors=4
  pageReporting=true
  ```

### 6.2 Docker network 생성
```powershell
docker network create ecommerce-data-network
```

### 6.3 Kaggle CSV 전처리 및 DB 구동
```powershell
# CSV 전처리
python notebook\kaggle_data\preprocess_kaggle_data.py

# PostgreSQL 구동 (desktop/source-db 디렉토리)
docker compose -f docker-compose-db.yaml up -d
```

### 6.4 Kafka / Connect / ClickHouse / Flink 실행
```powershell
cd desktop\data-pipeline
# 로컬 기준 실행
$env:TAILSCALE_DESKTOP_IP="localhost"
docker compose -f docker-compose-pipeline.yaml up -d --build
```

### 6.5 Kafka Connect connector 등록
```powershell
python desktop\data-pipeline\kafka-connect\register_connectors.py
```

### 6.6 dbt 빌드 및 실행
```powershell
cd notebook\dbt_clickhouse_dw
$env:CLICKHOUSE_HOST="localhost"
dbt debug
dbt compile  # 필수 사전 검증
dbt run
dbt test
dbt docs generate
```

### 6.7 Airflow 실행
```powershell
cd notebook\airflow_3_2
docker compose build
docker compose up airflow-init
docker compose up -d
```

### 6.8 Graph RAG 실행
```powershell
# Neo4j 구동
docker run -d --name neo4j-graph-rag -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:5.12.0

# dbt 메타데이터 Neo4j 적재 (dbt compile 우선 수행)
$env:NEO4J_URI="bolt://localhost:7687"
$env:NEO4J_USER="neo4j"
$env:NEO4J_PASSWORD="password"
python dbt_to_neo4j.py
```

---

## 7. 테스트 및 검증 전략

### 7.1 빠른 정적 검증
```powershell
git status --short
rg "docker-compose-infra|users|products|orders|payments|fact_returns|dim_campaign" .
```

### 7.2 Python 단위 테스트
```powershell
python -m unittest notebook\data-generator\test_transaction_generator.py
```

### 7.3 dbt 검증 (dbt_standards.md 참조)
실제 데이터 반영 전에 `dbt compile`을 통해 Jinja 문법 오류 및 순환 의존성을 검증한다.

### 7.4 Airflow 검증 및 API 모니터링 (airflow_standards.md 참조)
- 컨테이너 내부 실행 검증:
  ```powershell
  docker exec -i --user airflow airflow_3_2-airflow-scheduler-1 python /opt/airflow/dags/local_ecommerce_pipeline.py
  ```
- REST API 기반 임포트 에러 실시간 감시:
  ```powershell
  Invoke-RestMethod -Uri "http://localhost:8080/api/v1/importErrors" -Headers @{Authorization="Basic YWRtaW46YWRtaW4="}
  ```

---

## 8. 에이전트 작업 지침

* **상세 가이드 연동**: 에이전트는 작업을 시작하기 전 루트 디렉토리의 **[.agent_rules.yaml](file:///d:/01.DEV/EPSP/.agent_rules.yaml)**을 우선적으로 탐색하여 제약 조건과 DDL, dbt, Airflow 표준이 정의된 개별 스펙 파일을 사전에 read해야 한다.
* **코딩 스타일 강제**:
  - dbt: **[dbt_standards.md](file:///d:/01.DEV/EPSP/specs/dbt_standards.md)**에 명시된 SQL 구조화 템플릿과 명명법을 지킨다.
  - Airflow: **[airflow_standards.md](file:///d:/01.DEV/EPSP/specs/airflow_standards.md)**에 명시된 Task SDK 네임스페이스와 커넥션 방식을 강제한다.
* **장애 복구 조치**: 인프라 오류 발생 시 **[troubleshooting.md](file:///d:/01.DEV/EPSP/specs/troubleshooting.md)**에 기술된 compact 정책 조치 스크립트를 사용하여 디버깅한다.

---

## 9. 우선순위 높은 다음 작업
1. `notebook/dbt_clickhouse_dw/models`에 실제 `schema.yml`을 추가한다.
2. `DATA_CATALOG.md`의 구현 완료 테이블과 planned 테이블을 분리한다.
3. `transaction_generator.py`를 Olist canonical domain과 분리하거나 Olist 스키마에 맞게 재작성한다.
4. `register_connectors.py`가 v1/v2 connector 설정을 명확히 선택하도록 개선한다.
5. Airflow audit DAG의 ClickHouse database/schema 이름과 오차율 계산 방식을 실제 dbt output에 맞춘다.
6. Graph RAG의 Column node key 충돌을 제거한다.
7. README의 Quick Start에서 존재하지 않는 `docker-compose-infra.yaml` 참조를 실제 compose 파일 기준으로 수정한다.

---

## 10. 현재 스펙 결론
- EPSP의 canonical DW 도메인은 현재 Olist Kaggle 이커머스 데이터다.
- AX 후속을 위해서는 dbt metadata와 Graph RAG를 프로젝트의 부가 기능이 아니라 DW 설계의 핵심 산출물로 다뤄야 한다.
- 에이전트가 안정적으로 작업하려면 `schema.yml`, `DATA_CATALOG.md`, `manifest.json`, Neo4j metadata가 같은 사실을 말하도록 지속 관리해야 한다.
