# event-consumer-demo

`event-consumer-demo` 负责异步事件处理。

职责：

1. 消费 `Kafka` 中的会话事件
2. 按 `snapshot_object_key` 读取 `MinIO`
3. 将事件和快照补充信息写入 `ClickHouse`
4. 输出 Prometheus 指标和结构化日志

## 关键环境变量

- `KAFKA_BROKERS`
- `KAFKA_TOPIC`
- `KAFKA_GROUP_ID`
- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET`
- `CLICKHOUSE_ENDPOINT`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_TABLE`

## 默认端口

- HTTP：`18083`
- Metrics：`12115`

## 本地运行

```powershell
go mod tidy
$env:KAFKA_BROKERS="127.0.0.1:9092"
$env:KAFKA_TOPIC="user-session-events"
$env:MINIO_ENDPOINT="127.0.0.1:9000"
$env:MINIO_ACCESS_KEY="minioadmin"
$env:MINIO_SECRET_KEY="minioadmin123"
$env:MINIO_BUCKET="login-snapshots"
$env:CLICKHOUSE_ENDPOINT="http://127.0.0.1:8123"
go run .
```
