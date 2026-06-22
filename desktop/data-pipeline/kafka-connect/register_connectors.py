import urllib.request
import json
import time
import os

CONNECT_URL = "http://localhost:8083/connectors"

def make_request(url, method="GET", data=None, headers=None):
    if headers is None:
        headers = {}
    
    req_data = None
    if data is not None:
        req_data = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    
    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body
    except Exception as e:
        print(f"Request failed: {e}")
        return None, str(e)

def register_connectors():
    print("Checking existing connectors...")
    status, active_connectors = make_request(CONNECT_URL)
    if status != 200:
        print(f"Failed to fetch connectors from {CONNECT_URL}. Status: {status}, Response: {active_connectors}")
        return
    
    print(f"Currently active connectors: {active_connectors}")
    
    target_connectors = ["postgres-source-connector", "clickhouse-sink-connector"]
    for conn in target_connectors:
        if conn in active_connectors:
            print(f"Deleting old connector: {conn}")
            del_status, del_resp = make_request(f"{CONNECT_URL}/{conn}", method="DELETE")
            print(f"Delete response status: {del_status}")
            time.sleep(1)
            
    # JSON 파일 경로
    base_dir = r"d:\01.DEV\EPSP\desktop\data-pipeline\kafka-connect"
    pg_file = os.path.join(base_dir, "submit-pg-source.json")
    ch_file = os.path.join(base_dir, "submit-ch-sink.json")
    
    # 1. PostgreSQL Source Connector 등록
    if os.path.exists(pg_file):
        print("Registering Postgres Source Connector...")
        with open(pg_file, "r", encoding="utf-8") as f:
            pg_config = json.load(f)
        status, resp = make_request(CONNECT_URL, method="POST", data=pg_config)
        print(f"Postgres Source Connector registration status: {status}, Response: {resp}")
    else:
        print(f"Error: {pg_file} not found.")
        
    time.sleep(2)
    
    # 2. ClickHouse Sink Connector 등록
    if os.path.exists(ch_file):
        print("Registering ClickHouse Sink Connector...")
        with open(ch_file, "r", encoding="utf-8") as f:
            ch_config = json.load(f)
        status, resp = make_request(CONNECT_URL, method="POST", data=ch_config)
        print(f"ClickHouse Sink Connector registration status: {status}, Response: {resp}")
    else:
        print(f"Error: {ch_file} not found.")
        
    # 커넥터 상태 모니터링 (3회 반복하며 RUNNING 확인)
    print("\nWaiting for connectors to initialize...")
    for i in range(3):
        time.sleep(5)
        print(f"\nChecking status (Check {i+1}/3)...")
        for conn in target_connectors:
            status, resp = make_request(f"{CONNECT_URL}/{conn}/status")
            if status == 200:
                connector_state = resp.get("connector", {}).get("state", "UNKNOWN")
                tasks = resp.get("tasks", [])
                task_states = [t.get("state", "UNKNOWN") for t in tasks]
                print(f" - Connector '{conn}': {connector_state} | Tasks: {task_states}")
            else:
                print(f" - Connector '{conn}': FAILED to query status ({status})")

if __name__ == "__main__":
    register_connectors()
