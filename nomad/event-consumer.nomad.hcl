job "event-consumer-demo" {
  region      = var.region
  datacenters = var.datacenters
  namespace   = var.namespace
  type        = "service"

  group "event-consumer-demo" {
    count = var.count

    volume "logs" {
      type   = "host"
      source = var.host_volume
    }

    network {
      port "http" {}
      port "metrics" {}
    }

    service {
      name         = "event-consumer-demo-http"
      tags         = var.discovery_service_tags
      port         = "http"
      address_mode = "host"
      check {
        name     = "event-consumer-demo HTTP Check"
        type     = "http"
        path     = "/healthz"
        interval = "10s"
        timeout  = "2s"
      }
    }

    service {
      name         = "event-consumer-demo-prom"
      tags         = concat(["prometheus"], var.consul_service_tags)
      port         = "metrics"
      address_mode = "host"
      check {
        name     = "event-consumer-demo Metrics Check"
        type     = "http"
        path     = "/metrics"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "event-consumer-demo" {
      driver = "docker"
      user   = "root"

      volume_mount {
        volume      = "logs"
        destination = "/app/logs"
      }

      config {
        image        = var.image
        network_mode = "host"
        force_pull   = false
      }

      env {
        TZ                   = "Asia/Shanghai"
        SERVICE_NAME         = "event-consumer-demo"
        APP_PORT             = "${NOMAD_PORT_http}"
        METRICS_PORT         = "${NOMAD_PORT_metrics}"
        INSTANCE_ID          = "${NOMAD_ALLOC_ID}"
        APP_LOG_PATH         = "/app/logs/event-consumer-demo/${NOMAD_ALLOC_ID}.log"
        KAFKA_BROKERS        = var.kafka_brokers
        KAFKA_TOPIC          = var.kafka_topic
        KAFKA_GROUP_ID       = var.kafka_group_id
        MINIO_ENDPOINT       = var.minio_endpoint
        MINIO_ACCESS_KEY     = var.minio_access_key
        MINIO_SECRET_KEY     = var.minio_secret_key
        MINIO_BUCKET         = var.minio_bucket
        MINIO_USE_SSL        = var.minio_use_ssl
        CLICKHOUSE_ENDPOINT  = var.clickhouse_endpoint
        CLICKHOUSE_USER      = var.clickhouse_user
        CLICKHOUSE_PASSWORD  = var.clickhouse_password
        CLICKHOUSE_DATABASE  = var.clickhouse_database
        CLICKHOUSE_TABLE     = var.clickhouse_table
      }

      resources {
        cpu    = var.cpu
        memory = var.memory
      }
    }
  }
}

variable "region" {
  type = string
}

variable "datacenters" {
  type = list(string)
}

variable "namespace" {
  type    = string
  default = "default"
}

variable "image" {
  type = string
}

variable "consul_service_tags" {
  type    = list(string)
  default = []
}

variable "discovery_service_tags" {
  type    = list(string)
  default = []
}

variable "count" {
  type    = number
  default = 1
}

variable "cpu" {
  type    = number
  default = 100
}

variable "memory" {
  type    = number
  default = 128
}

variable "kafka_brokers" {
  type = string
}

variable "kafka_topic" {
  type    = string
  default = "user-session-events"
}

variable "kafka_group_id" {
  type    = string
  default = "event-consumer-demo"
}

variable "minio_endpoint" {
  type = string
}

variable "minio_access_key" {
  type = string
}

variable "minio_secret_key" {
  type = string
}

variable "minio_bucket" {
  type    = string
  default = "login-snapshots"
}

variable "minio_use_ssl" {
  type    = string
  default = "false"
}

variable "clickhouse_endpoint" {
  type = string
}

variable "clickhouse_user" {
  type    = string
  default = "default"
}

variable "clickhouse_password" {
  type    = string
  default = ""
}

variable "clickhouse_database" {
  type    = string
  default = "app"
}

variable "clickhouse_table" {
  type    = string
  default = "session_events"
}

variable "host_volume" {
  type    = string
  default = "logs"
}
