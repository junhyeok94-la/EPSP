import os
from langchain_community.graphs import Neo4jGraph
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI

# 1. Neo4j 및 OpenAI API 키 설정 (환경 변수 읽기)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# LLM 초기화 (여기서는 ChatOpenAI를 기본값으로 사용하되, 다른 모델도 사용 가능하도록 구성)
# 필요시 환경변수 OPENAI_API_KEY를 사전에 세팅해야 합니다.
openai_api_key = os.getenv("OPENAI_API_KEY", "your-api-key-here")

def get_graph_connection():
    try:
        graph = Neo4jGraph(
            url=NEO4J_URI,
            username=NEO4J_USER,
            password=NEO4J_PASSWORD
        )
        # 스키마 정보를 갱신 및 확인
        graph.refresh_schema()
        return graph
    except Exception as e:
        print(f"Neo4j 연결 실패: {e}")
        return None

def retrieve_metadata_from_graph(graph, keyword_list):
    """
    질문에 포함된 키워드와 매칭되는 모델(Model) 및 컬럼(Column), 의존관계(Lineage) 메타데이터를
    Neo4j 그래프에서 조회하여 컨텍스트 문자열로 만듭니다.
    """
    # 1. 키워드에 매칭되는 모델 및 해당 모델의 모든 컬럼 조회
    query = """
    MATCH (m:Model)
    WHERE any(k IN $keywords WHERE toLower(m.name) CONTAINS toLower(k) OR toLower(m.description) CONTAINS toLower(k))
    OPTIONAL MATCH (m)-[:HAS_COLUMN]->(c:Column)
    RETURN m.name AS model_name, 
           m.description AS model_desc, 
           m.materialized AS materialized,
           collect({name: c.name, type: c.type, desc: c.description}) AS columns
    LIMIT 10
    """
    
    results = graph.query(query, {"keywords": keyword_list})
    
    # 2. 관련 모델들의 의존관계(Lineage) 정보 조회
    lineage_query = """
    MATCH (m1:Model)-[r:DEPENDS_ON]->(m2)
    WHERE m1.name IN $model_names OR m2.name IN $model_names
    RETURN m1.name AS child_model, m2.name AS parent_model
    LIMIT 10
    """
    
    model_names = [res["model_name"] for res in results]
    lineage_results = graph.query(lineage_query, {"model_names": model_names}) if model_names else []

    # 3. 텍스트 컨텍스트로 변환
    context_parts = []
    context_parts.append("### [DBT Schema & Metadata Context from Neo4j]")
    
    for res in results:
        model_info = (
            f"Table/View: {res['model_name']}\n"
            f"  - Description: {res['model_desc']}\n"
            f"  - Materialized: {res['materialized']}\n"
            f"  - Columns:\n"
        )
        for col in res["columns"]:
            if col['name']:
                model_info += f"    * {col['name']} ({col['type']}): {col['desc']}\n"
        context_parts.append(model_info)
        
    if lineage_results:
        context_parts.append("### [Table Dependencies & Lineage]")
        for lin in lineage_results:
            context_parts.append(f"  - {lin['child_model']} depends on {lin['parent_model']}")
            
    return "\n".join(context_parts)

# ClickHouse 그라운드 룰을 주입한 Text-to-SQL 프롬프트
TEXT_TO_SQL_PROMPT = """너는 ClickHouse 및 dbt 전문가 데이터 분석 에이전트야.
제공된 Neo4j 그래프 기반 dbt 메타데이터 컨텍스트와 ClickHouse 작성 룰을 바탕으로 사용자의 자연어 질문을 최적의 ClickHouse SQL 쿼리로 변환해줘.

[ClickHouse SQL 작성 규칙 (필수 준수)]
1. ReplacingMergeTree 엔진이 적용된 실버(Silver) 및 골드(Gold) 레이어 테이블을 조회할 때는 중복 행을 제거하고 최신 상태를 얻기 위해 반드시 테이블 이름 바로 뒤에 'FINAL' 키워드를 기입해야 해.
   (예: FROM default.fact_orders FINAL as o)
2. 날짜 조인이나 필터 시, 'date_id'는 8자리 정수형(YYYYMMDD) 형식을 주로 사용해.
3. KST 시간(UTC+9) 정합성을 위해 뷰 계층에서 toTimeZone(ts_ms, 'Asia/Seoul') 처리가 되어 있을 수 있어.
4. 테이블 명세를 참고하여 컬럼 타입과 이름을 정확히 매칭해줘. 없는 컬럼을 가상으로 만들어내선 안돼.

[dbt 메타데이터 컨텍스트 (Neo4j 검색 결과)]
{context}

[사용자 질문]
{question}

최종 ClickHouse SQL 쿼리만 마크다운 코드 블록(```sql ... ```)으로 출력하고, 쿼리에 대한 간단한 설명(조인 조건, 사용된 테이블 등)을 덧붙여줘.
"""

def generate_clickhouse_sql(graph, llm, question, keywords):
    # 1. Neo4j Graph DB로부터 관련 스키마 컨텍스트 추출 (Graph RAG)
    print(f"\n[1] Neo4j 그래프에서 키워드 {keywords} 관련 메타데이터를 검색 중...")
    context = retrieve_metadata_from_graph(graph, keywords)
    
    # 2. LLM 체인을 통해 ClickHouse 쿼리 생성
    print("[2] LLM에게 메타데이터 컨텍스트와 함께 ClickHouse SQL 생성을 요청 중...")
    prompt = ChatPromptTemplate.from_template(TEXT_TO_SQL_PROMPT)
    chain = prompt | llm | StrOutputParser()
    
    response = chain.invoke({
        "context": context,
        "question": question
    })
    return response

def main():
    print("--- LangChain + Neo4j Graph RAG Text-to-SQL PoC ---")
    
    # Neo4j 연결
    graph = get_graph_connection()
    if not graph:
        return
        
    # LLM 초기화
    if openai_api_key == "your-api-key-here" and not os.getenv("OPENAI_API_KEY"):
        print("경고: OPENAI_API_KEY 환경변수가 설정되지 않았습니다. API 키를 설정해야 정상 작동합니다.")
        # 로컬 테스트용 Mock 또는 임시 LLM
        return
        
    llm = ChatOpenAI(model="gpt-4o", temperature=0)

    # 테스트 시나리오 질문
    test_question = "2026년 1분기 기준, 카테고리별 총 실 결제금액(net_amount)과 평균 배송지연일수를 조회해줘."
    keywords = ["orders", "product", "calendar"]  # 질문에서 추출한 핵심 엔티티 키워드들

    print(f"\n자연어 질문: '{test_question}'")
    response = generate_clickhouse_sql(graph, llm, test_question, keywords)
    
    print("\n[최종 AI 응답]")
    print(response)

if __name__ == "__main__":
    main()
