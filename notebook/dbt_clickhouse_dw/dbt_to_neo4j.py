import os
import json
from neo4j import GraphDatabase

# Neo4j 연결 정보 (환경 변수 또는 기본값)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# dbt manifest.json 파일 경로
MANIFEST_PATH = os.path.join(os.path.dirname(__file__), "target", "manifest.json")

def load_manifest(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"dbt manifest.json 파일을 찾을 수 없습니다: {path}. 'dbt compile' 또는 'dbt docs generate'를 먼저 실행하세요.")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def run_cypher(tx, query, parameters=None):
    tx.run(query, parameters)

def clear_database(driver):
    with driver.session() as session:
        print("Neo4j 기존 dbt 관련 데이터를 초기화합니다...")
        # dbt 관련 노드 및 관계 삭제
        session.execute_write(run_cypher, "MATCH (n) WHERE n:Model OR n:Source OR n:Column DETACH DELETE n")

def ingest_metadata(driver, manifest):
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    with driver.session() as session:
        # 1. Model 노드 및 관련 Column 노드 생성
        print("dbt Model 및 Column 정보를 Neo4j에 적재 중...")
        for node_id, node_info in nodes.items():
            # 모델(Model) 타입만 처리 (seed, snapshot 등도 포함 가능하지만 여기선 model에 집중)
            if node_info.get("resource_type") == "model":
                model_name = node_info.get("name")
                materialized = node_info.get("config", {}).get("materialized", "unknown")
                engine = node_info.get("config", {}).get("engine", "unknown")
                db_name = node_info.get("database", "default")
                schema_name = node_info.get("schema", "default")
                description = node_info.get("description", "")

                # Model 노드 병합 생성
                model_query = """
                MERGE (m:Model {unique_id: $unique_id})
                SET m.name = $name,
                    m.materialized = $materialized,
                    m.engine = $engine,
                    m.database = $database,
                    m.schema = $schema,
                    m.description = $description
                """
                session.execute_write(run_cypher, model_query, {
                    "unique_id": node_id,
                    "name": model_name,
                    "materialized": materialized,
                    "engine": engine,
                    "database": db_name,
                    "schema": schema_name,
                    "description": description
                })

                # Column 노드 및 HAS_COLUMN 관계 생성
                columns = node_info.get("columns", {})
                for col_name, col_info in columns.items():
                    col_type = col_info.get("data_type") or col_info.get("type", "unknown")
                    col_desc = col_info.get("description", "")
                    
                    column_query = """
                    MERGE (c:Column {name: $col_name})
                    ON CREATE SET c.type = $col_type, c.description = $col_desc
                    ON MATCH SET c.type = $col_type, c.description = CASE WHEN c.description = "" THEN $col_desc ELSE c.description END
                    WITH c
                    MATCH (m:Model {unique_id: $unique_id})
                    MERGE (m)-[:HAS_COLUMN]->(c)
                    """
                    session.execute_write(run_cypher, column_query, {
                        "col_name": col_name,
                        "col_type": col_type,
                        "col_desc": col_desc,
                        "unique_id": node_id
                    })

        # 2. Source 노드 및 관련 Column 노드 생성
        print("dbt Source 및 Column 정보를 Neo4j에 적재 중...")
        for source_id, source_info in sources.items():
            if source_info.get("resource_type") == "source":
                source_name = source_info.get("name")
                db_name = source_info.get("database", "default")
                schema_name = source_info.get("schema", "default")
                description = source_info.get("description", "")

                source_query = """
                MERGE (s:Source {unique_id: $unique_id})
                SET s.name = $name,
                    s.database = $database,
                    s.schema = $schema,
                    s.description = $description
                """
                session.execute_write(run_cypher, source_query, {
                    "unique_id": source_id,
                    "name": source_name,
                    "database": db_name,
                    "schema": schema_name,
                    "description": description
                })

                # Source Column 노드 및 HAS_COLUMN 관계 생성
                columns = source_info.get("columns", {})
                for col_name, col_info in columns.items():
                    col_type = col_info.get("data_type") or col_info.get("type", "unknown")
                    col_desc = col_info.get("description", "")
                    
                    column_query = """
                    MERGE (c:Column {name: $col_name})
                    ON CREATE SET c.type = $col_type, c.description = $col_desc
                    ON MATCH SET c.type = $col_type, c.description = CASE WHEN c.description = "" THEN $col_desc ELSE c.description END
                    WITH c
                    MATCH (s:Source {unique_id: $unique_id})
                    MERGE (s)-[:HAS_COLUMN]->(c)
                    """
                    session.execute_write(run_cypher, column_query, {
                        "col_name": col_name,
                        "col_type": col_type,
                        "col_desc": col_desc,
                        "unique_id": source_id
                    })

        # 3. Model -> Model / Source 간의 DEPENDS_ON 의존 관계 생성
        print("dbt Lineage(의존 관계) 정보를 Neo4j에 생성 중...")
        for node_id, node_info in nodes.items():
            if node_info.get("resource_type") == "model":
                depends_on_nodes = node_info.get("depends_on", {}).get("nodes", [])
                for parent_id in depends_on_nodes:
                    # parent_id가 model이거나 source인 경우만 연결
                    if parent_id.startswith("model."):
                        lineage_query = """
                        MATCH (child:Model {unique_id: $child_id})
                        MATCH (parent:Model {unique_id: $parent_id})
                        MERGE (child)-[:DEPENDS_ON]->(parent)
                        """
                        session.execute_write(run_cypher, lineage_query, {
                            "child_id": node_id,
                            "parent_id": parent_id
                        })
                    elif parent_id.startswith("source."):
                        lineage_query = """
                        MATCH (child:Model {unique_id: $child_id})
                        MATCH (parent:Source {unique_id: $parent_id})
                        MERGE (child)-[:DEPENDS_ON]->(parent)
                        """
                        session.execute_write(run_cypher, lineage_query, {
                            "child_id": node_id,
                            "parent_id": parent_id
                        })

def main():
    print(f"1. manifest.json 로딩 중... ({MANIFEST_PATH})")
    try:
        manifest = load_manifest(MANIFEST_PATH)
    except Exception as e:
        print(f"오류: {e}")
        return

    print(f"2. Neo4j 데이터베이스 연결 중... ({NEO4J_URI})")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
    except Exception as e:
        print(f"Neo4j 연결 실패: {e}")
        print("로컬에 Neo4j가 실행 중이거나 올바른 연결 정보 환경 변수(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)가 설정되었는지 확인하세요.")
        return

    try:
        clear_database(driver)
        ingest_metadata(driver, manifest)
        print("🎉 dbt 메타데이터가 Neo4j에 성공적으로 로드되었습니다!")
    finally:
        driver.close()

if __name__ == "__main__":
    main()
