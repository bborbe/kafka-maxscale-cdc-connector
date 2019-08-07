# Kafka Maxscale CDC Connector

Stream Maxscale CDC events to Kafka.

## Run

```bash
go run main.go \
-cdc-host=127.0.0.1 \
-cdc-port=4001 \
-cdc-user=cdcuser \
-cdc-password=cdc \
-cdc-database=test \
-cdc-table=names \
-cdc-uuid=e3835094-4824-4f9d-ba97-5e2ec217733b \
-kafka-brokers=kafka:9092 \
-kafka-topic=cdc-test-names \
-datadir=/tmp \
-initial-delay=1s \
-v=2
```

## Sample SQL

```sql
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
CREATE TABLE test.names(id INT auto_increment PRIMARY KEY, name VARCHAR(20));
INSERT INTO test.names (name) VALUES ('Hello');
INSERT INTO test.names (name) VALUES ('World');
```

## Consome topic

```bash
go get github.com/Shopify/sarama/tools/kafka-console-consumer
kafka-console-consumer -topic=cdc-test-names -brokers=kafka:9092
```       

## Docker Compose

Sample for run Mariadb, Maxscale, Kafka, Zookeeper and the Connector inside Docker. 

```bash
docker-compose up \
--force-recreate \
--build \
--detach
```         

```bash
docker exec -ti mariadb mysql --password=123
```
