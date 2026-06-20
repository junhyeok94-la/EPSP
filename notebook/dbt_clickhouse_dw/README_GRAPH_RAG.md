# DBT Graph RAG (Neo4j + LangChain) 구축 및 실행 가이드

본 가이드는 dbt 프로젝트의 메타데이터(`manifest.json`)를 Neo4j 그래프 데이터베이스에 적재하고, LangChain을 결합하여 데이터 카탈로그 계보 및 ClickHouse 전용 Text-to-SQL을 자동으로 수행하는 **Graph RAG**를 로컬에서 실행하고 테스트하기 위한 안내서입니다.

---

## 1. 전제 조건 및 환경 준비

### 1) Python 필수 패키지 설치
이 PoC 기능을 실행하기 위해서는 아래의 파이썬 패키지들이 필요합니다.
```bash
pip install neo4j langchain langchain-community langchain-openai
```

### 2) Neo4j 데이터베이스 기동 (Docker 방식)
로컬 PC(데스크톱 또는 노트북)에 Docker가 설치되어 있는 경우, 다음 명령어로 Neo4j 컨테이너를 가동할 수 있습니다.

```bash
docker run -d \
  --name neo4j-graph-rag \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:5.12.0
```
- **Bolt 주소**: `bolt://localhost:7687` (기본 포트)
- **웹 콘솔 주소**: `http://localhost:7474` (브라우저를 통해 그래프 시각화 확인 가능)
- **ID/PW**: `neo4j` / `password`

---

## 2. dbt 메타데이터 생성 및 Neo4j 적재

### 1) dbt manifest.json 컴파일 생성
dbt 모델 변경 사항 및 의존 관계 정보를 담은 최신 메타데이터 빌드를 수행합니다.
```bash
# dbt 프로젝트 루트 폴더 (notebook/dbt_clickhouse_dw) 이동 후 실행
dbt compile
```
이후 `target/manifest.json` 파일이 정상적으로 갱신되었는지 확인합니다.

### 2) Neo4j 메타데이터 마이그레이션 실행
`dbt_to_neo4j.py` 스크립트를 가동하여 dbt의 테이블, 컬럼, 계보(Lineage) 정보를 그래프 DB로 내보냅니다.
```bash
# 연결 정보 환경 변수 설정 (기본값인 경우 생략 가능)
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password"

# 스크립트 실행
python notebook/dbt_clickhouse_dw/dbt_to_neo4j.py
```
**성공 출력 예시**:
```text
1. manifest.json 로딩 중... (d:\01.DEV\EPSP\notebook\dbt_clickhouse_dw\target\manifest.json)
2. Neo4j 데이터베이스 연결 중... (bolt://localhost:7687)
Neo4j 기존 dbt 관련 데이터를 초기화합니다...
dbt Model 및 Column 정보를 Neo4j에 적재 중...
dbt Source 및 Column 정보를 Neo4j에 적재 중...
dbt Lineage(의존 관계) 정보를 Neo4j에 생성 중...
🎉 dbt 메타데이터가 Neo4j에 성공적으로 로드되었습니다!
```

---

## 3. LangChain Graph RAG 실행 및 SQL 검증

### 1) OpenAI API Key 설정
지능형 쿼리 매핑 및 SQL 변환을 수행할 LLM 연동을 위해 OpenAI API Key를 환경 변수로 내보냅니다.
```bash
export OPENAI_API_KEY="sk-..."
```

### 2) Graph RAG 실행
자연어 질문을 통해 Neo4j에서 메타데이터(테이블 및 조인 정보)를 실시간 추출하고, 최종 ClickHouse 쿼리를 조립하여 가져오는지 검증합니다.
```bash
python notebook/dbt_clickhouse_dw/graph_rag_qa.py
```

**동작 메커니즘**:
1. **질문**: `"2026년 1분기 기준, 카테고리별 총 실 결제금액(net_amount)과 평균 배송지연일수를 조회해줘."`
2. **Graph RAG 탐색**: 질문의 엔티티(`orders`, `product`, `calendar`)를 Neo4j에서 찾아 연관 모델인 `fact_orders`, `dim_product`, `dim_calendar` 및 그들의 관계와 모든 컬럼 타입을 Context로 인출합니다.
3. **LLM 프롬프트 결합**: 가져온 테이블 명세 및 `ClickHouse FINAL 그라운드 룰`을 프롬프트에 주입하여, 정확하고 정밀한 ClickHouse 쿼리를 생성합니다.
4. **결과 출력**:
   ```sql
   SELECT 
       p.category as product_category,
       SUM(o.net_amount) as total_net_amount,
       AVG(o.delivery_delay_days) as avg_delivery_delay_days
   FROM default.fact_orders FINAL as o
   INNER JOIN default.dim_product FINAL as p 
       ON o.product_id = p.product_id
   INNER JOIN default.dim_calendar FINAL as cal 
       ON o.date_id = cal.date_id
   WHERE cal.year = 2026 
     AND cal.quarter = 1
   GROUP BY product_category
   ORDER BY total_net_amount DESC;
   ```
