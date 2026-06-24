# dbt Analytics Engineering 개발 표준 가이드

본 문서는 ClickHouse 데이터 웨어하우스(DW)를 dbt-core 및 dbt-clickhouse를 이용해 구축할 때 준수해야 하는 개발 표준 규격이다. AI 에이전트 및 데이터 엔지니어가 SQL 모델을 설계하거나 리팩토링할 때 그라운드 트루스로 동작한다.

---

## 1. 디렉토리 레이아웃 및 명명 규칙

- 모델 파일은 반드시 `models/[스키마명]/[업무도메인별]/[모델명].sql` 경로에 위치해야 한다.
  - 예: `models/01_bronze/ecommerce/stg_orders.sql`, `models/02_silver/users/dim_users.sql`
- 모델명은 반드시 생성되는 대상 테이블명과 1:1로 일치해야 한다. (예: 파일명이 `stg_orders.sql`이면, 테이블명도 `stg_orders`여야 함)
- 소문자와 스네이크 케이스(snake_case)만 사용한다.

---

## 2. 코드 작성 구조 및 순서 원칙

모든 dbt 모델의 내부 코드는 반드시 다음 순서대로 작성되어야 한다.

1. **[1] Header Comments (헤더 주석)**: 최상단에 모델의 명칭, 목적, 변경 이력을 표준 양식으로 작성.
2. **[2] Hooks (훅 설정)**: 모델 실행 전후에 중복 체킹이나 로깅 등이 필요할 경우 `pre_hook`, `post_hook` 선언. (불필요한 경우 생략 가능하나 위치는 헤더 다음)
3. **[3] Config (설정)**: ClickHouse 특성(`materialized`, `engine`, `order_by` 등)을 선언.
4. **[4] SQL Query (본문)**: 비즈니스 로직 작성.

### 2.1 표준 템플릿 예시
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

---

## 3. ClickHouse 전용 dbt 모델링 규칙

* **모델 목적과 grain의 명확성**: 각 dbt 모델에는 목적, grain(한 줄의 의미), upstream/downstream 의존성 주석을 필수 명시한다.
* **`schema.yml` 연동**: 모든 모델은 컬럼 설명(`description`)과 무결성 테스트(`unique`, `not_null`, `relationships`)가 명시된 `schema.yml`을 함께 추가하고 관리한다.
* **물리 성능 최적화**: ClickHouse 테이블 엔진 생성 시 `order_by`, `partition_by`는 실제 데이터 볼륨과 자주 사용되는 쿼리 필터링 조건을 기준으로 수립하며, 고카디널리티 컬럼을 무차별 partition key로 사용하지 않는다.
* **ReplacingMergeTree FINAL**: 중복 제거 및 준실시간 데이터 정합성이 반드시 요구되는 경우에만 `FINAL` 제어어를 제한적으로 쿼리에 활용한다.
* **Bronze 변환 원칙**: 원천 Debezium 타임스탬프는 Bronze 단에서 KST 시간대 통일 및 가명화(`customer_id` SHA-256 해싱 가명화 등)를 일괄 처리한다.
* **Silver/Gold 명명**: 비즈니스 관점에서 직관적이고 AI 에이전트가 이해하기 쉬운 도메인 컬럼명을 채용한다.

---

## 4. 상태 의존성(State Dependency) 및 리니지 싱크 표준

* **의존성 양성화 (depends_on)**:
  * 물리적인 SQL 조인(JOIN)이나 참조(`ref`)가 없더라도, 비즈니스 처리 절차상 혹은 포스트 훅(post_hook) 실행 완료 등에 따른 런타임 선후 관계가 확실히 보장되어야 하는 경우, 모델 상단에 `depends_on` 지시어를 명시하여 dbt 정적 리니지에 강제로 편입시킨다.
  * *예시 코드*:
    ```sql
    -- models/gold/mart_daily_sales_wide.sql
    -- {# depends_on ref('stg_payments_post_hook_trigger') #}
    
    select ...
    ```
* **AI 에이전트 연동을 위한 `schema.yml` 내 `meta` 정의 규칙**:
  * 테이블의 런타임 제약조건, 갱신 주기, 담당 부서 등은 AI 에이전트가 카탈로그 및 Graph RAG(Neo4j)를 통해 해석할 수 있어야 하므로, `meta` 속성을 정의하여 구조화된 정보를 주입한다.
  * *schema.yml 작성 가이드라인*:
    ```yaml
    models:
      - name: mart_daily_sales_wide
        description: "일별 매출 요약 와이드 마트"
        meta:
          owner: "Data Platform Team"
          refresh_policy: "Runs after model 'stg_payments' post_hook triggers status updates"
          runtime_constraints:
            - "Depends on Tailscale tunnel stabilization"
            - "Run frequency: Every 15 minutes micro-batch"
          agent_rules:
            - "Do not queries this table without timezone filter (KST standard)"
        columns:
          - name: total_sales
            description: "당일 총 매출액"
            meta:
              sensitivity: "confidential"
    ```

