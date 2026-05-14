region      = "global"
datacenters = ["dc1"]
namespace   = "default"

image = "event-consumer-demo:dev"

consul_service_tags = ["prometheus.enabled=true"]
discovery_service_tags = []

count  = 1
cpu    = 100
memory = 128

kafka_brokers      = "127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092"
kafka_topic        = "user-session-events"
kafka_group_id     = "event-consumer-demo"

minio_endpoint     = "127.0.0.1:9000"
minio_access_key   = "minioadmin"
minio_secret_key   = "minioadmin123"
minio_bucket       = "login-snapshots"
minio_use_ssl      = "false"

clickhouse_endpoint = "http://127.0.0.1:18123"
clickhouse_user     = "lab"
clickhouse_password = "lab123"
clickhouse_database = "app"
clickhouse_table    = "session_events"

host_volume = "logs"
