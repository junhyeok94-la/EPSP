# EPSP (E-Commerce Pipeline with Segmented Platforms)

> **"Kafka와 ClickHouse 기반 실시간 데이터 파이프라인 및 DW 설계 기술을 확보하기 위한 데이터 엔지니어링 성장 여정"**

본 프로젝트의 궁극적인 목적은 데이터 엔지니어로서 실시간 데이터 수집(CDC, Kafka), 분산 적재, 그리고 OLAP 데이터 웨어하우스(ClickHouse)와 데이터 모델링(dbt)의 전체 라이프사이클을 직접 설계하고 튜닝하여 기술적 깊이를 확보하는 것입니다. 집의 데스크톱 PC(데이터 엔진)와 공유오피스의 노트북(제어 및 인입)을 가상 사설망(Tailscale VPN)으로 묶은 하이브리드 아키텍처는, 현재의 개발 여건 하에서 비용($0)을 최소화하면서 실제 프로덕션 수준의 분산 네트워크 환경을 모방하기 위해 설계된 실용적인 아키텍처적 솔루션입니다.

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

## 2. 데이터 엔지니어링 아키텍처 및 핵심 성능 설계 (DE Design & Architecture)

본 프로젝트는 고성능 실시간 스트리밍 적재와 대용량 OLAP 마트 분석 성능을 극대화하기 위해 다음과 같은 데이터 엔지니어링 설계 제약 및 표준화 원칙을 강제 반영했습니다.

### 2.1 Kafka & CDC 실시간 파티셔닝 전략 (FIFO 순서 보장)

Kafka 브로커는 동일 파티션 내부에서만 메시지의 시간적 선입선출(FIFO)을 보장합니다. 비즈니스 정합성을 위해 다음과 같이 파티션 키를 이원화하여 분배했습니다.

#### 2.1.1 Kafka 분산 분배와 MurmurHash2 메커니즘
Kafka는 프로듀서가 레코드를 전송할 때 파티션 키의 해시값(기본 `MurmurHash2` 알고리즘)을 기반으로 파티션을 결정합니다.
$$\text{Partition} = |\text{MurmurHash2}(\text{Key})| \pmod{\text{Number of Partitions}}$$
이 수식에 의거하여 동일한 파티션 키를 갖는 데이터는 무조건 동일한 브로커 노드의 동일 파티션 파일(`.log`)에 순서대로 쓰이게 됩니다.

#### 2.1.2 비즈니스 도메인별 파티션 키(Partition Key) 할당 규칙
* **유저 행동 및 실시간 흐름 (`user_id` 기반 파티셔닝)**:
  - **대상 토픽**: `clickstream_events`, `ecommerce_orders_stream`
  - **설계 의도**: 사용자의 여정(조회 -> 장바구니 -> 주문 -> 취소)은 **사용자 개별 타임라인** 내에서 절대적인 시간 순서가 보장되어야 합니다. 만약 주문 취소(`CANCELLED`) 이벤트가 주문 생성(`CREATED`)보다 파이프라인 지연 등으로 인해 스트림 프로세서(Flink)에 먼저 도달하면 "존재하지 않는 주문에 대한 취소 요청" 에러를 내며 상태(State)가 깨집니다. 따라서 사용자 단위의 실시간 분석 및 추천 파이트라인은 반드시 `user_id`를 파티션 키로 사용하여 순서를 고정합니다.
* **트랜잭션 수집 및 정산 (`order_id` 기반 파티셔닝)**:
  - **대상 토픽**: `ecommerce.public.orders` (Debezium CDC 수집 토픽)
  - **설계 의도**: 개별 주문서의 상태 변경 트래킹(`CREATED` -> `PAID` -> `SHIPPED`)은 주문 번호 단위의 정렬이 중요합니다. 서로 독립적인 주문 트랜잭션 간에는 병렬 처리를 극대화하여 처리량(Throughput)을 늘려야 하므로, 운영 DB의 Primary Key인 `order_id`를 파티션 키로 사용합니다. Debezium CDC 커넥터는 PostgreSQL WAL 복제 시 테이블의 PK를 자동으로 Kafka 메시지의 Key로 설정하여 이를 보장합니다.

#### 2.1.3 안티패턴 및 Flink Stateful Join 성능 제어
* **안티패턴 경고**: 클릭스트림에 `product_id`를 파티션 키로 지정할 경우, 한 사용자가 조회하는 다양한 상품 이벤트가 서로 다른 카프카 파티션으로 찢어집니다. 이 경우 Flink가 특정 유저의 최근 5분간 상품 조회 목록을 윈도우 조인하려고 할 때 데이터 순서가 완전히 뒤엉키며, 네트워크 셔플링(Shuffle) 부하가 기하급수적으로 증가합니다.
* **Flink Re-keying & Stateful Window Join**: 
  서로 다른 파티션 키를 가져 순서 보장이 분리된 두 스트림(예: `order_id`가 키인 주문 스트림과 `delivery_id`가 키인 배송 스트림)을 결합해야 하는 비즈니스 도메인의 경우 Flink 수준에서 공통 조인 키를 기준으로 `keyBy(order_id)` 연산을 선언하여 메모리 상에서 실시간 재분배(Repartitioning)합니다. 이후 lead-time을 보정하기 위해 Flink 내부 메모리 상태(State)에 선도착 이벤트를 버퍼링하고, 시간 윈도우(예: 30분) 범위 내에서 결합을 완수한 후 결과 레이어에 적재하도록 표준을 규정했습니다.

---

### 2.2 인프라 자원 제어 및 오케스트레이션 이원화

제한된 로컬 하드웨어 환경(노트북 + 데스크톱)에서 시스템 중단 없이 안정적으로 데이터 웨어하우스를 구동하기 위해 하드웨어 리소스와 스케줄링 흐름을 물리적으로 격리했습니다.

#### 2.2.1 WSL2 커널 리소스 세부 튜닝 (`.wslconfig`)
Windows 노트북 노드에서 Docker Desktop 및 WSL2를 구동할 때, 가상 메모리 프로세스(`vmmemWSL`)의 과도한 메모리 점유 및 호스트 OS 프리징을 예방하기 위해 사용자 프로필 디렉토리(`%USERPROFILE%`)에 `.wslconfig` 설정을 강제 적용했습니다.
* **`memory=6GB`**: WSL 가상 머신에 할당할 최대 메모리 크기입니다. Airflow와 dbt 연산 시 호스트 Windows OS의 메모리 고갈을 막기 위한 최적의 타협선입니다.
* **`processors=4`**: 가상 머신이 사용할 최대 논리 프로세서(CPU Core) 개수입니다.
* **`pageReporting=true`**: WSL2 가상 머신 내부에서 더 이상 사용하지 않는 가비지 메모리를 호스트 Windows OS에 실시간으로 즉시 반환하도록 강제 유도하여 메모리 누수를 원천 차단합니다.

#### 2.2.2 배치-이벤트 하이브리드 오케스트레이션 설계
실시간 스트리밍 파일 인입 주기마다 에어플로우 스케줄러가 이벤트 드리븐 형태로 작동하면, 메타데이터 DB(PostgreSQL)의 커넥션이 폭발하고 스케줄러가 먹통이 되는 병목 현상이 발생합니다.
* **수집-계산 이원화**: 실시간 적재(Kafka/CDC)는 L4 스레드 레벨에서 무중단으로 밀어 넣도록 단독 구동하고, OLAP 마트 가공 및 dbt 변환은 **15분 주기 마이크로 배치 크론 스케줄링**으로 격리하여 물리 하드웨어의 CPU/메모리 스파이크를 방어했습니다.
* **Airflow 3.x Asset (Data-Aware scheduling) 연쇄**:
  최종 ClickHouse 매출 마트가 완성되는 시점을 에어플로우 자산(Asset)으로 등록합니다:
  ```python
  from airflow.sdk import Asset
  CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/fact_orders_hourly")
  ```
  dbt 변환이 완료되는 배치 DAG([DAG 1])의 최종 태스크 아웃렛으로 `CLICKHOUSE_ORDER_GOLD_ASSET`을 설정하여, 성공 시점에만 이벤트를 방출합니다. 후속 비정기 파이프라인(예: 재고 부족 알림 [DAG 2])은 해당 Asset 이벤트를 구독하여 유기적으로 연쇄 트리거되도록 구성하여, 스케줄러 폴링 리소스를 $0$로 차단했습니다.

---

### 2.3 ClickHouse OLAP 성능 최적화 및 데이터 정합성 설계

ClickHouse는 분산 컬럼 지향 데이터베이스로 대규모 단일 스캔에 최적화되어 있지만, 관계형 DB와 같은 잦은 `UPDATE/DELETE`나 무분별한 물리 파티셔닝에 매우 취약합니다. 이를 방지하기 위한 엔지니어링 설계를 반영했습니다.

#### 2.3.1 ReplacingMergeTree 최적화 및 Read-side FINAL 우회
Debezium CDC 로그의 삽입/수정 변경 내역을 수용하기 위해, 동일 PK 기준 최신 타임스탬프(`ts_ms`) 데이터만 남기고 중복을 지우는 `ReplacingMergeTree(ts_ms)` 엔진을 채택했습니다.
* **문제점**: `ReplacingMergeTree`는 백그라운드 머지 스레드가 동작할 때만 데이터 최신화를 수행하므로, 쿼리 실행 시점에 중복 데이터가 남아있어 정합성이 깨질 수 있습니다. 이를 막기 위해 조회 시마다 `FINAL` 키워드를 사용하면 ClickHouse가 싱글 스레드 병합 처리를 강제하여 CPU 점유율이 100%로 치솟고 쿼리 속도가 10배 이상 느려집니다.
* **dbt 최적화 설계**: 
  - dbt 증분(Incremental) 모델 설계 시 `is_incremental()` 블록 내부에서 `ts_ms` 최신 정렬과 `op != 'd'`(삭제 플래그) 필터를 조합해 변경된 블록만 증분 계산(`delete+insert` 또는 `append`)하도록 격리했습니다.
  - 마트 조회 쿼리 등 최신성이 100% 필요한 시점에만 `FINAL`을 부분적으로 서브쿼리 내에 적용하거나, 최신 상태를 뽑아내는 `argMax(value, ts_ms)` 집계 함수로 대체 구현하여 쿼리 효율을 고속화했습니다.

#### 2.3.2 ClickHouse 파티셔닝 안티패턴 방지 (Too many parts)
ClickHouse는 테이블을 파티셔닝(`PARTITION BY`)할 때마다 디스크 상에 별도의 물리 폴더(Part)를 생성하고 백그라운드 스레드가 이를 병합합니다.
* **경고**: 만약 카디널리티가 수만~수백만에 달하는 `user_id`나 `product_id`를 파티션 키로 지정하면, 백그라운드 머지 스레드가 감당할 수 없을 정도로 많은 디렉토리와 인덱스 파일이 디스크에 생성됩니다. 결국 파일 디스크립터 고갈 및 **"Too many parts"** 예외와 함께 DB 서버가 다운됩니다.
* **표준 가이드라인**:
  - **물리 파티셔닝 키 (`PARTITION BY`)**: 카디널리티가 현저히 낮은 **시간 범위**(`toYYYYMM(created_at)`)나 대분류 도메인 코드로 설정하여 물리적인 디스크 분할과 Data Pruning(스캔 범위 제약)에 대응합니다.
  - **희소 인덱스 정렬 키 (`ORDER BY`)**: 고속 필터링 및 조인을 위해 실제 검색 조건이 되는 **`(product_id, created_at)`** 또는 **`(user_id, created_at)`** 조합을 희소 인덱스 정렬 키로 지정하여 쿼리를 튜닝했습니다.

#### 2.3.3 CDC 메타데이터 타임존 단일화 (KST 보정)
Debezium CDC 수집 파이프라인에서 생성되는 이벤트 수집 시간인 `ts_ms`는 카프카 커넥트 런타임에 의해 **UTC(세계 표준시)** 기준으로 기록됩니다. 이를 원천 DB 및 로컬 시각(KST, UTC+9)과 동기화하지 않고 분석 마트에 직접 사용하면 9시간의 시차가 생겨 일별 매출 통계 등 모든 시계열 데이터가 왜곡되는 원인이 됩니다.
* **해결 방안**: dbt `01_bronze` 계층 뷰 모델 작성 시, `ts_ms` 컬럼에 대해 ClickHouse 내장 함수인 **`toTimeZone(ts_ms, 'Asia/Seoul') as ts_ms`** 처리를 강제했습니다. 이로써 Downstream 모델들(Silver/Gold)이 추가적인 타임존 처리 연산 없이 일관되게 로컬 KST 타임라인을 안전하게 조회하고 분석할 수 있도록 데이터 정합성을 단일화했습니다.

#### 2.3.4 연결 복원력(Resilience) 및 사후 정합성 대조 모니터링
* **시뮬레이터 연결 복원력**: 파이썬 트래픽 시뮬레이터에 연결 복원력(Connection Resilience)을 갖춘 재시도 메커니즘(`psycopg2` 커넥션 루프 내 재시도 정책: 최대 5회, 백오프 3초)을 구현하여 사설망 일시 단절 시의 데이터 손실을 원천 방어했습니다.
* **데이터 정합성 사후 감사 (Data Reconciliation Audit)**: 
  매벽 새벽 2시에 작동하는 에어플로우 감사 DAG가 원천 PostgreSQL DB의 `SUM(total_price)`와 ClickHouse DW `analytics_silver.fact_orders`에 `FINAL` 제어어를 적용한 `SUM(total_price)` 값을 대조합니다. 오차가 0.01% 이상 발생할 경우 즉시 예외를 발생시키고 Slack 등 시스템 경보를 발생시키도록 모니터링 안전망을 설계했습니다.

---

## 3. 5대 핵심 도전 과제와 기술적 극복 과정 (Problem - Analyze - Solution)

### 📌 Challenge 1: 인프라 운영 비용의 한계 ($0 Infra 제약)
* **Problem**: AWS RDS, Confluent Cloud, ClickHouse Cloud 등 프로덕션급 클라우드 데이터 스택을 구축하는 데 월 수백 달러의 비용이 발생하며, 단순 단일 VM 내부 Docker-Compose 구성은 실제 물리적인 분산 환경(레이턴시, 패킷 유실 등)을 대변하지 못합니다.
* **Analyze**: 실무에 바로 활용할 수 있는 데이터 엔지니어링 기술 스택(Kafka, ClickHouse) 역량을 체득하고자 하였으나, 보유 중인 가용한 로컬 컴퓨팅 리소스(데스크톱 PC + 개인 노트북)를 물리 노드로 활용해야 하는 여건이었습니다. 이에 따라 공유오피스의 방화벽과 가정용 인터넷의 유동 IP 한계를 극복하고 두 노드를 가상 LAN 대역으로 안전하게 묶을 수 있는 대안이 요구되었습니다.
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

## 4. 리포지토리 디렉토리 레이아웃 (Repository Layout)

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

## 5. AI 에이전트 질의 최적화 & 데이터 카탈로그 표준

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

## 6. 각 서비스별 포트 및 웹 UI 맵핑 정보 (Port Map)

학습 및 파이프라인 흐름 추적을 위해 노드별로 기동되는 각 서비스의 포트 및 웹 UI 접속 정보는 다음과 같습니다.

| 노드 구분 | 서비스명 | 주요 용도 | 사용 포트 | 웹 UI 주소 |
| :--- | :--- | :--- | :--- | :--- |
| **노드 1 (데스크톱)** | **Kafka UI** | 카프카 브로커 및 커넥터 모니터링 | `8089` | `http://localhost:8089` |
| **노드 1 (데스크톱)** | **Flink Dashboard** | 실시간 스트림 처리 모니터링 | `8081` | `http://localhost:8081` |
| **노드 1 (데스크톱)** | **ClickHouse Server** | 실시간 DW (OLAP HTTP) | `8123` | - |
| **노드 1 (데스크톱)** | **PostgreSQL 15** | 원천 트랜잭션 운영 DB | `5433` (외부 포워딩) | - |
| **노드 1 (데스크톱)** | **Apache Kafka** | 메시지 브로커 (외부 바인딩) | `9094` | - |
| **노드 2 (노트북)** | **Apache Airflow** | 마이크로 배치 및 감사 DAG 제어 | `8080` (기본값) | 로컬 Airflow UI |

---

## 7. 시작 가이드 (Quick Start)

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
