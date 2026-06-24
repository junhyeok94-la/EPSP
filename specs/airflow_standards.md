# Airflow 3.2+ 개발 표준 가이드

본 문서는 Airflow 3.0+ (AIP-72) 및 Task SDK 아키텍처를 기반으로 오케스트레이션 DAG를 개발하고 인프라 접속을 정의할 때의 통합 개발 가이드라인이다.

---

## 1. 핵심 개발 그라운드 룰 (Core Ground Rules)

1. **Task SDK 공식 네임스페이스 (`airflow.sdk`) 사용 의무화**
   - 기존 Airflow 2.x의 내부 모듈 임포트(예: `airflow.models.dag.DAG`, `airflow.decorators.task`)는 더 이상 사용하지 않는다.
   - 모든 DAG 구성 요소 및 유틸리티는 `from airflow.sdk import ...` 경로로 통일한다.
     - *대표 Import 대상*: `dag`, `task`, `Asset`, `Connection`, `Variable`, `get_current_context`
2. **메타데이터 DB 직접 접근 엄격 금지 (Execution API 강제)**
   - 워커의 태스크 코드 내에서 메타데이터 데이터베이스에 직접 커넥션을 맺거나 내부 모델(예: `DagRun`, `TaskInstance`)을 직접 임포트해 쿼리할 수 없다.
   - 데이터베이스와의 통신은 에어플로우 내부 REST API 및 Execution API를 거쳐야 하므로, 태스크 상태나 콘텍스트 정보 조회 시에는 반드시 `get_current_context()` 메소드를 통해 접근한다.
3. **태그(Tags) 메타데이터 강제화**
   - Airflow 3.x부터는 UI 및 오케스트레이션 관리 목적의 태깅이 표준화되어 권장된다.
   - 모든 DAG 선언부(`@dag` 데코레이터)에는 시스템 분류를 명시하는 `tags` 파라미터를 필수로 작성해야 한다. (예: `tags=["ecommerce", "cdc", "clickhouse"]`)
4. **TaskFlow API 및 데코레이터 우선 표준**
   - 전통적인 Operator 클래스를 명시적으로 선언하여 연결하는 방식(예: `PythonOperator`) 대신, 가독성과 데이터 흐름 추적이 용이한 `@dag`, `@task`, `@task_group` 데코레이터를 이용한 TaskFlow API 작성을 기본 원칙으로 한다.
5. **Asset(구 Dataset) 기반 스케줄링 적용**
   - 2.x 버전에서 사용되던 `Dataset` 명칭은 3.x 버전부터 `Asset`으로 일괄 개편되었다.
   - 스케줄링 간 이벤트 기반 연쇄 흐름을 설계할 때는 `from airflow.sdk import Asset`을 선언하여 데이터 의존성 체인을 구성한다.

### 1.1 표준 코딩 모범 사례 (Best Practice)
```python
from datetime import datetime
from airflow.sdk import dag, task, Asset, get_current_context

# 1. 3.x 규격의 Asset 정의 (일관된 URI 스키마 사용)
CLICKHOUSE_GOLD_ASSET = Asset(uri="clickhouse://default/mart_daily_sales_wide")

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

---

## 2. 공통 로직 캡슐화 및 결합도 제어 가이드

1. **공통 로직의 `plugins` 관리 원칙**
   - DAG 간에 재사용되는 공통 기능은 `notebook/airflow_3_2/plugins` 디렉토리 하위에 모듈/패키지 형태로 작성하여 관리한다.
   - 플러그인에 정의된 모듈은 Airflow 3.x 환경에서 임포트할 때 표준 파이썬 경로로 접근이 가능해야 한다.
   - **캡슐화(Encapsulation)**를 통해 공통 함수의 내부 세부 구현을 숨기고, DAG 비즈니스 로직에는 간결한 인터페이스만 노출시켜 재사용성과 가독성을 극대화한다.
2. **강결합 방지를 위한 유연성 대처 원칙 (Loose Coupling)**
   - **강결합(Tight Coupling) 리스크 방지**: 만약 공통 로직이 특정 DAG의 비즈니스 도메인과 밀접하게 연동된다면, 이를 무리하게 공통화하여 플러그인에 캡슐화하는 것은 피해야 한다. 이는 해당 모듈을 참조하는 다른 DAG들의 동시 변경을 강제(사이드 이펙트 유발)하기 때문이다.
   - **유연한 대처 규칙**:
     - *도메인 특화 쿼리 및 변환 로직*: 개별 DAG 또는 태스크 내부에 직접 인라인(Inline)으로 작성하거나, 해당 DAG 파일이 속한 로컬 디렉토리 내부에서만 사용될 헬퍼 모듈로 격리하여 결합을 차단한다.
     - *의존성 주입(Dependency Injection) 지향*: 공통 코드가 필요하지만 특정 비즈니스 파라미터나 쿼리가 달라져야 하는 경우, 로직 내에 하드코딩하지 않고 매개변수(Parameter)나 콜백 함수(Callback) 형태로 외부에서 주입받도록 구성하여 결합도를 낮춘다.

---

## 3. Airflow Connection 및 외부 인프라 연동 설정 표준 (Connections Specification)

에어플로우가 각 이기종 데이터 소스 및 타깃 DW와 통신하기 위해 정의하는 Connection(연결) 정보에 대한 공식 스펙이다.

### 3.1 원천 PostgreSQL 연결 (`postgres_desktop`)
- **Conn ID**: `postgres_desktop`
- **Conn Type**: `Postgres`
- **연결 매개변수 설정**:
  - *로컬 샌드박스 내부 테스트 시*:
    - **Host**: `ecommerce-postgres` (동일 도커 브리지 네트워크 상의 PostgreSQL 컨테이너명)
    - **Port**: `5432`
  - *노트북(노드 2)에서 데스크톱(노드 1) 원격 연결 시*:
    - **Host**: `${TAILSCALE_DESKTOP_IP}` (데스크톱의 Tailscale 가상 IP)
    - **Port**: `15432` (외부 포워딩 포트)
  - **Database**: `postgres`
  - **Login**: `postgres`
  - **Password**: `postgres`
- **원클릭 등록 CLI 예시**:
  ```bash
  docker exec -i --user airflow airflow_3_2-airflow-scheduler-1 \
    airflow connections add postgres_desktop \
    --conn-type postgres \
    --conn-host ecommerce-postgres \
    --conn-login postgres \
    --conn-password postgres \
    --conn-port 5432 \
    --conn-schema postgres
  ```

### 3.2 대상 ClickHouse 연결 (`clickhouse_desktop`)
- **Conn ID**: `clickhouse_desktop`
- **Conn Type**: `Generic` (또는 ClickHouse 전용 Provider 사용 시 `clickhouse`)
- **연결 매개변수 설정**:
  - *로컬 샌드박스 내부 테스트 시*:
    - **Host**: `ecommerce-clickhouse` (ClickHouse 컨테이너명)
    - **Port**: `8123` (HTTP interface)
  - *노트북(노드 2)에서 데스크톱(노드 1) 원격 연결 시*:
    - **Host**: `${TAILSCALE_DESKTOP_IP}`
    - **Port**: `8123`
  - **Database**: `default`
  - **Login**: `default`
  - **Password**: (공란 또는 지정된 패스워드)
- **원클릭 등록 CLI 예시**:
  ```bash
  docker exec -i --user airflow airflow_3_2-airflow-scheduler-1 \
    airflow connections add clickhouse_desktop \
    --conn-type generic \
    --conn-host ecommerce-clickhouse \
    --conn-login default \
    --conn-port 8123 \
    --conn-schema default
  ```

---

## 4. 실행 및 런타임 제약

* **컨테이너 내부 실행**: DAG 검증 및 스케줄러 인지 테스트는 호스트 Python 환경이 아닌 Docker 컨테이너 내부에 진입해 수행한다.
* **사용자 컨텍스트 제약**: 실행 시 `--user airflow` 옵션을 생략하면 root 사용자로 진입하게 되어 패키지(dbt, cosmos, common_config) 로드 오류가 발생하므로, **반드시 `airflow` 사용자로 컨텍스트를 고정**한다.

---

## 5. 대규모 dbt 모델 세분화 및 실행 의존성 제어 표준

* **업무 도메인 단위의 모델 폴더 세분화**:
  * dbt 모델의 수가 증가함에 따라 단일 `DbtTaskGroup` 렌더링에 따른 스케줄러 부하를 방지하기 위해, `models/` 폴더 하위를 도메인 단위(예: `models/02_silver/sales`, `models/02_silver/logistics` 등)로 세분화하여 관리한다.
  * Airflow에서는 세분화된 디렉토리를 물리적으로 분리된 **독립적인 서브 DAG(또는 도메인 특화 DAG)**로 정의하여 개별 Cosmos `DbtTaskGroup`으로 감싸 렌더링 부하를 제어한다.
* **Asset 기반의 이벤트 구동형(Event-driven) 의존성 설계 (강제 권장)**:
  * 서브 DAG 간 실행 흐름 제어 시, 상위 컨트롤 DAG에서 `TriggerDagRunOperator`를 통해 실행 순서를 강결합(`>>`)하는 구조를 탈피한다.
  * 대신 **Airflow 3.x의 Asset(Dataset) 발행 및 구독 아키텍처**를 활용하여 데이터 업데이트 상태 변화에 맞춰 각 DAG가 연쇄 트리거되도록 구성한다.
  * *Asset 기반 연쇄 스케줄링 예시*:
    ```python
    from datetime import datetime
    from airflow.sdk import dag, task, Asset
    from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
    from cosmos.selectors import Selector

    # 1. 갱신 상태를 전달할 매개체(Asset) 정의
    SILVER_SALES_ASSET = Asset(uri="clickhouse://default/silver_sales_status_updated")
    GOLD_MART_ASSET = Asset(uri="clickhouse://default/mart_daily_sales_wide")

    # 2. 선행 DAG (Silver 세부 도메인 갱신 후 Asset 발행)
    @dag(
        dag_id="dbt_silver_sales_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="*/15 * * * *",
        catchup=False,
        tags=["dbt", "silver", "sales"]
    )
    def silver_sales_dag():
        # Cosmos DbtTaskGroup을 사용하여 sales 폴더만 렌더링
        sales_group = DbtTaskGroup(
            group_id="dbt_sales",
            project_config=ProjectConfig("/opt/airflow/dbt_clickhouse_dw"),
            profile_config=ProfileConfig(...),
            render_config=RenderConfig(
                select=["path:models/02_silver/sales"]
            )
        )
        
        # dbt 완료 후 후속 상태 갱신을 의미하는 Asset 트리거
        @task(task_id="publish_sales_asset", outlets=[SILVER_SALES_ASSET])
        def trigger_asset():
            print("Sales models processing done. Publishing Asset updates.")

        sales_group >> trigger_asset()

    silver_sales_dag()


    # 3. 후행 DAG (Asset을 구독하여 자동 구동)
    @dag(
        dag_id="dbt_gold_sales_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule=[SILVER_SALES_ASSET],  # 앞선 DAG가 Asset을 발행하면 자동으로 트리거됨
        catchup=False,
        tags=["dbt", "gold", "sales"]
    )
    def gold_sales_dag():
        gold_group = DbtTaskGroup(
            group_id="dbt_gold",
            project_config=ProjectConfig("/opt/airflow/dbt_clickhouse_dw"),
            profile_config=ProfileConfig(...),
            render_config=RenderConfig(
                # 정합성 유지를 위해 Modifier(+)를 추가하여 결합 실행
                select=["+path:models/03_gold/sales"]
            )
        )
        
    gold_sales_dag()
    ```

