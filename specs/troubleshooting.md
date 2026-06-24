# 실시간 스트리밍 인프라 장애 트러블슈팅 가이드

본 문서는 실시간 데이터 파이프라인(Kafka, Connect, ClickHouse, Flink)의 테스트 및 운영 과정에서 마주하기 쉬운 대표적 인프라 에러와 실무 해결 시나리오이다.

---

## 1. rdkafka 연결 실패 및 호스트 해결 오류 (`Failed to resolve 'kafka:9092'`)

* **현상**: 호스트 PC(노트북)에서 기동한 Python 트래픽 생성기가 카프카 브로커 `localhost:9092` 연결 시도 후 `Failed to resolve 'kafka:9092'` 에러로 전송 실패.
* **원인**: 카프카 브로커에 접속 성공 시 카프카가 반환하는 내부 Advertised Listener 정보인 `kafka:9092`를 호스트 컴퓨터가 네트워크 DNS단에서 직접 찾아가지 못해 발생 (카프카 리스너 설계의 물리적 격리 제약).
* **조치**: 외부 기기 및 호스트 환경에서 카프카 브로커에 접근할 때는 반드시 `KAFKA_LISTENERS`에 맵핑된 **EXTERNAL 포트인 `9094`**로 카프카 브로커 경로를 지정하여 통신해야 합니다. (`KAFKA_BROKER = localhost:9094` 또는 `TAILSCALE_IP:9094`).

---

## 2. 카프카 커넥트 offset/config 토픽 cleanup.policy 검증 오류 (`ConfigException`)

* **현상**: 카프카 브로커 재기동 이후 카프카 커넥트(Connect) 컨테이너가 즉사(Exit)하며 로그에 `offset.storage.topic is required to have cleanup.policy=compact` 등의 ConfigException 발생.
* **원인**: 카프카 브로커의 `KAFKA_NUM_PARTITIONS` 변경 등으로 인해 내부 메타데이터 적재 토픽들이 자동 생성될 때, 카프카 기본값인 `cleanup.policy=delete`로 오설정 생성되어 커넥트의 멱등성 검증 루프를 통과하지 못한 현상.
* **조치**: 카프카 관리 CLI 도구를 활용해 아래와 같이 커넥터 내부 보관용 토픽의 클린업 정책을 `compact`로 변경하십시오.
  ```bash
  docker exec ecommerce-kafka kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name ecommerce_connect_offsets --alter --add-config cleanup.policy=compact
  docker exec ecommerce-kafka kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name ecommerce_connect_configs --alter --add-config cleanup.policy=compact
  docker exec ecommerce-kafka kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name ecommerce_connect_statuses --alter --add-config cleanup.policy=compact
  ```

---

## 3. config.storage.topic 파티션 개수(1개 초과) 불일치 오류

* **현상**: 커넥트 로그에 `config.storage.topic is required to have a single partition ... but found 3 partitions` 에러와 함께 기동 실패.
* **원인**: 브로커의 전역 파티션 기본값(`KAFKA_NUM_PARTITIONS=3`)에 의해 Connect 설정 보관 토픽(`ecommerce_connect_configs`)이 3개 파티션으로 자동 생성됨. 카프카 커넥트는 설정 정보의 일관성을 위해 이 토픽이 반드시 단 1개의 파티션으로 구성될 것을 요구함. (파티션 축소는 카프카 스펙상 불가).
* **조치**: 커넥트 컨테이너를 일시 중지시킨 후, 기존 토픽을 강제 삭제하고 파티션 개수 1개 규격으로 재생성하여 극복합니다.
  ```bash
  # 1. 커넥트 컨테이너 일시 중지
  docker stop ecommerce-connect
  # 2. 기존 다중 파티션 configs 토픽 삭제
  docker exec ecommerce-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic ecommerce_connect_configs
  # 3. 1개 파티션 및 compact 정책으로 configs 토픽 강제 재생성
  docker exec ecommerce-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic ecommerce_connect_configs --partitions 1 --replication-factor 1 --config cleanup.policy=compact
  # 4. 커넥트 재시작
  docker start ecommerce-connect
  ```
