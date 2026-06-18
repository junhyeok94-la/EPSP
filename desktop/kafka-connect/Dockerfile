FROM debezium/connect:2.4

USER root

# curl 및 unzip 설치
RUN microdnf update -y && \
    microdnf install -y unzip && \
    microdnf clean all

# ClickHouse Kafka Connect Sink Connector 다운로드 및 설치
RUN mkdir -p /kafka/connect/clickhouse-kafka-connect && \
    curl -sfL https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v1.3.9/clickhouse-kafka-connect-v1.3.9.zip -o /tmp/clickhouse-kafka-connect.zip && \
    unzip /tmp/clickhouse-kafka-connect.zip -d /kafka/connect/clickhouse-kafka-connect/ && \
    rm /tmp/clickhouse-kafka-connect.zip

USER kafka
