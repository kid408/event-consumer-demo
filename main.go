package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"event-consumer-demo/internal/sessionrpc"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_event_consumer_http_requests_total",
			Help: "Total HTTP requests received by the event consumer demo service.",
		},
		[]string{"path", "method", "code"},
	)

	httpRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "go_event_consumer_http_request_duration_seconds",
			Help:    "HTTP request duration of the event consumer demo service in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5},
		},
		[]string{"path", "method", "code"},
	)

	processUp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_event_consumer_process_up",
			Help: "Whether the event consumer demo process is considered up.",
		},
	)

	eventsConsumedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_event_consumer_events_consumed_total",
			Help: "Total Kafka events consumed by the event consumer demo.",
		},
		[]string{"action", "result"},
	)

	clickhouseInsertTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_event_consumer_clickhouse_insert_total",
			Help: "Total ClickHouse insert attempts made by the event consumer demo.",
		},
		[]string{"result"},
	)

	minioFetchTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_event_consumer_minio_fetch_total",
			Help: "Total MinIO fetch attempts made by the event consumer demo.",
		},
		[]string{"result"},
	)

	eventProcessDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "go_event_consumer_event_process_duration_seconds",
			Help:    "Duration of Kafka event processing in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5},
		},
		[]string{"action", "result"},
	)

	kafkaLagGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_event_consumer_kafka_lag",
			Help: "Best effort Kafka lag gauge for the event consumer demo.",
		},
	)
)

type config struct {
	serviceName        string
	instanceID         string
	appPort            string
	metricsPort        string
	logPath            string
	kafkaBrokers       []string
	kafkaTopic         string
	kafkaGroupID       string
	minioEndpoint      string
	minioAccessKey     string
	minioSecretKey     string
	minioBucket        string
	minioUseSSL        bool
	clickhouseEndpoint string
	clickhouseUser     string
	clickhousePassword string
	clickhouseDatabase string
	clickhouseTable    string
	consumerTimeout    time.Duration
}

type app struct {
	config       config
	startedAt    time.Time
	logger       *log.Logger
	httpClient   *http.Client
	minioClient  *minio.Client
	kafkaReader  *kafka.Reader
	requestCount atomic.Uint64
}

type eventRecord struct {
	EventTime         time.Time `json:"event_time"`
	ConsumedAt        time.Time `json:"consumed_at"`
	EventID           string    `json:"event_id"`
	SessionID         string    `json:"session_id"`
	ClientID          string    `json:"client_id"`
	UserID            uint64    `json:"user_id"`
	DeviceID          string    `json:"device_id"`
	Action            string    `json:"action"`
	Payload           string    `json:"payload"`
	GatewayID         string    `json:"gateway_id"`
	WorkerID          string    `json:"worker_id"`
	SnapshotObjectKey string    `json:"snapshot_object_key"`
	SnapshotExists    bool      `json:"snapshot_exists"`
	SnapshotSizeBytes int64     `json:"snapshot_size_bytes"`
	SnapshotPayload   string    `json:"snapshot_payload"`
	KafkaPartition    int       `json:"kafka_partition"`
	KafkaOffset       int64     `json:"kafka_offset"`
	KafkaTimestamp    time.Time `json:"kafka_timestamp"`
}

type clickhouseEventRecord struct {
	EventTime         string `json:"event_time"`
	ConsumedAt        string `json:"consumed_at"`
	EventID           string `json:"event_id"`
	SessionID         string `json:"session_id"`
	ClientID          string `json:"client_id"`
	UserID            uint64 `json:"user_id"`
	DeviceID          string `json:"device_id"`
	Action            string `json:"action"`
	Payload           string `json:"payload"`
	GatewayID         string `json:"gateway_id"`
	WorkerID          string `json:"worker_id"`
	SnapshotObjectKey string `json:"snapshot_object_key"`
	SnapshotExists    bool   `json:"snapshot_exists"`
	SnapshotSizeBytes int64  `json:"snapshot_size_bytes"`
	SnapshotPayload   string `json:"snapshot_payload"`
	KafkaPartition    int    `json:"kafka_partition"`
	KafkaOffset       int64  `json:"kafka_offset"`
	KafkaTimestamp    string `json:"kafka_timestamp"`
}

type logEntry struct {
	Level            string `json:"level"`
	Event            string `json:"event"`
	Service          string `json:"service"`
	InstanceID       string `json:"instance_id"`
	Path             string `json:"path,omitempty"`
	Method           string `json:"method,omitempty"`
	Status           int    `json:"status,omitempty"`
	Action           string `json:"action,omitempty"`
	EventID          string `json:"event_id,omitempty"`
	SessionID        string `json:"session_id,omitempty"`
	ClientID         string `json:"client_id,omitempty"`
	Detail           string `json:"detail,omitempty"`
	KafkaPartition   int    `json:"kafka_partition,omitempty"`
	KafkaOffset      int64  `json:"kafka_offset,omitempty"`
	ProcessedRecords int64  `json:"processed_records,omitempty"`
	Timestamp        string `json:"ts"`
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func main() {
	prometheus.MustRegister(
		httpRequestsTotal,
		httpRequestDurationSeconds,
		processUp,
		eventsConsumedTotal,
		clickhouseInsertTotal,
		minioFetchTotal,
		eventProcessDurationSeconds,
		kafkaLagGauge,
	)

	cfg := loadConfig()
	logger, logFile, err := newLogger(cfg.logPath)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logFile.Close()

	application := &app{
		config:    cfg,
		startedAt: time.Now(),
		logger:    logger,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	if cfg.minioEndpoint != "" {
		client, clientErr := minio.New(cfg.minioEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.minioAccessKey, cfg.minioSecretKey, ""),
			Secure: cfg.minioUseSSL,
		})
		if clientErr != nil {
			log.Fatalf("init minio client failed: %v", clientErr)
		}
		application.minioClient = client
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               cfg.kafkaBrokers,
		Topic:                 cfg.kafkaTopic,
		GroupID:               cfg.kafkaGroupID,
		MinBytes:              1,
		MaxBytes:              10e6,
		CommitInterval:        0,
		StartOffset:           kafka.FirstOffset,
		QueueCapacity:         100,
		WatchPartitionChanges: true,
	})
	application.kafkaReader = reader
	defer reader.Close()

	if err := application.ensureClickHouseSchema(context.Background()); err != nil {
		log.Fatalf("ensure clickhouse schema failed: %v", err)
	}

	processUp.Set(1)

	appMux := http.NewServeMux()
	appMux.HandleFunc("/", application.handleRoot)
	appMux.HandleFunc("/healthz", application.handleHealth)
	appMux.HandleFunc("/health", application.handleHealth)
	appMux.HandleFunc("/status", application.handleStatus)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:              ":" + cfg.appPort,
		Handler:           application.withMetrics(appMux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsServer := &http.Server{
		Addr:              ":" + cfg.metricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go application.consumeLoop(rootCtx)

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "http_server_starting"})
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "metrics_server_starting"})
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	processUp.Set(0)
	application.writeLog(logEntry{Level: "info", Event: "shutdown_signal_received"})

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = metricsServer.Shutdown(shutdownCtx)
}

func (a *app) consumeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		message, err := a.kafkaReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			a.writeLog(logEntry{
				Level:  "error",
				Event:  "kafka_fetch_failed",
				Detail: err.Error(),
			})
			time.Sleep(2 * time.Second)
			continue
		}

		startedAt := time.Now()
		result := "success"

		var event sessionrpc.SessionEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			result = "error"
			eventsConsumedTotal.WithLabelValues("unmarshal", result).Inc()
			eventProcessDurationSeconds.WithLabelValues("unmarshal", result).Observe(time.Since(startedAt).Seconds())
			a.writeLog(logEntry{
				Level:          "error",
				Event:          "kafka_event_unmarshal_failed",
				Detail:         err.Error(),
				KafkaPartition: message.Partition,
				KafkaOffset:    message.Offset,
			})
			_ = a.kafkaReader.CommitMessages(ctx, message)
			continue
		}

		record, err := a.enrichRecord(ctx, message, &event)
		if err != nil {
			result = "error"
			eventsConsumedTotal.WithLabelValues(event.Action, result).Inc()
			eventProcessDurationSeconds.WithLabelValues(event.Action, result).Observe(time.Since(startedAt).Seconds())
			a.writeLog(logEntry{
				Level:          "error",
				Event:          "event_enrich_failed",
				Action:         event.Action,
				EventID:        event.EventID,
				SessionID:      event.SessionID,
				ClientID:       event.ClientID,
				Detail:         err.Error(),
				KafkaPartition: message.Partition,
				KafkaOffset:    message.Offset,
			})
			_ = a.kafkaReader.CommitMessages(ctx, message)
			continue
		}

		if err := a.insertRecord(ctx, record); err != nil {
			result = "error"
			eventsConsumedTotal.WithLabelValues(event.Action, result).Inc()
			clickhouseInsertTotal.WithLabelValues(result).Inc()
			eventProcessDurationSeconds.WithLabelValues(event.Action, result).Observe(time.Since(startedAt).Seconds())
			a.writeLog(logEntry{
				Level:          "error",
				Event:          "clickhouse_insert_failed",
				Action:         event.Action,
				EventID:        event.EventID,
				SessionID:      event.SessionID,
				ClientID:       event.ClientID,
				Detail:         err.Error(),
				KafkaPartition: message.Partition,
				KafkaOffset:    message.Offset,
			})
			_ = a.kafkaReader.CommitMessages(ctx, message)
			continue
		}

		eventsConsumedTotal.WithLabelValues(event.Action, result).Inc()
		clickhouseInsertTotal.WithLabelValues("success").Inc()
		eventProcessDurationSeconds.WithLabelValues(event.Action, result).Observe(time.Since(startedAt).Seconds())
		kafkaLagGauge.Set(float64(a.kafkaReader.Lag()))

		a.writeLog(logEntry{
			Level:            "info",
			Event:            "event_processed",
			Action:           event.Action,
			EventID:          event.EventID,
			SessionID:        event.SessionID,
			ClientID:         event.ClientID,
			KafkaPartition:   message.Partition,
			KafkaOffset:      message.Offset,
			ProcessedRecords: 1,
		})

		if err := a.kafkaReader.CommitMessages(ctx, message); err != nil {
			a.writeLog(logEntry{
				Level:          "error",
				Event:          "kafka_commit_failed",
				Action:         event.Action,
				EventID:        event.EventID,
				SessionID:      event.SessionID,
				ClientID:       event.ClientID,
				Detail:         err.Error(),
				KafkaPartition: message.Partition,
				KafkaOffset:    message.Offset,
			})
		}
	}
}

func (a *app) enrichRecord(ctx context.Context, message kafka.Message, event *sessionrpc.SessionEvent) (*eventRecord, error) {
	record := &eventRecord{
		EventTime:         parseEventTime(event.ProcessedAt, event.SentAt),
		ConsumedAt:        time.Now(),
		EventID:           event.EventID,
		SessionID:         event.SessionID,
		ClientID:          event.ClientID,
		UserID:            event.UserID,
		DeviceID:          event.DeviceID,
		Action:            event.Action,
		Payload:           event.Payload,
		GatewayID:         event.GatewayID,
		WorkerID:          event.WorkerID,
		SnapshotObjectKey: event.SnapshotObjectKey,
		KafkaPartition:    message.Partition,
		KafkaOffset:       message.Offset,
		KafkaTimestamp:    message.Time,
	}

	if strings.TrimSpace(event.SnapshotObjectKey) == "" || a.minioClient == nil {
		return record, nil
	}

	objectInfo, data, err := a.fetchSnapshot(ctx, event.SnapshotObjectKey)
	if err != nil {
		minioFetchTotal.WithLabelValues("error").Inc()
		return record, err
	}

	minioFetchTotal.WithLabelValues("success").Inc()
	record.SnapshotExists = true
	record.SnapshotSizeBytes = objectInfo.Size
	record.SnapshotPayload = string(data)
	return record, nil
}

func (a *app) fetchSnapshot(ctx context.Context, objectKey string) (*minio.ObjectInfo, []byte, error) {
	info, err := a.minioClient.StatObject(ctx, a.config.minioBucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return nil, nil, err
	}

	object, err := a.minioClient.GetObject(ctx, a.config.minioBucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, nil, err
	}
	defer object.Close()

	data, err := io.ReadAll(object)
	if err != nil {
		return nil, nil, err
	}
	return &info, data, nil
}

func (a *app) ensureClickHouseSchema(ctx context.Context) error {
	if strings.TrimSpace(a.config.clickhouseEndpoint) == "" {
		return nil
	}

	if err := a.clickhouseQuery(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", a.config.clickhouseDatabase)); err != nil {
		return err
	}

	tableSQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s
(
    event_time DateTime64(3, 'Asia/Shanghai'),
    consumed_at DateTime64(3, 'Asia/Shanghai'),
    event_id String,
    session_id String,
    client_id String,
    user_id UInt64,
    device_id String,
    action LowCardinality(String),
    payload String,
    gateway_id String,
    worker_id String,
    snapshot_object_key String,
    snapshot_exists UInt8,
    snapshot_size_bytes Int64,
    snapshot_payload String,
    kafka_partition Int32,
    kafka_offset Int64,
    kafka_timestamp DateTime64(3, 'Asia/Shanghai')
)
ENGINE = MergeTree
ORDER BY (event_time, session_id, event_id)
`, a.config.clickhouseDatabase, a.config.clickhouseTable)

	return a.clickhouseQuery(ctx, tableSQL)
}

func (a *app) insertRecord(ctx context.Context, record *eventRecord) error {
	if strings.TrimSpace(a.config.clickhouseEndpoint) == "" {
		return nil
	}

	row := clickhouseEventRecord{
		EventTime:         formatClickHouseDateTime64(record.EventTime),
		ConsumedAt:        formatClickHouseDateTime64(record.ConsumedAt),
		EventID:           record.EventID,
		SessionID:         record.SessionID,
		ClientID:          record.ClientID,
		UserID:            record.UserID,
		DeviceID:          record.DeviceID,
		Action:            record.Action,
		Payload:           record.Payload,
		GatewayID:         record.GatewayID,
		WorkerID:          record.WorkerID,
		SnapshotObjectKey: record.SnapshotObjectKey,
		SnapshotExists:    record.SnapshotExists,
		SnapshotSizeBytes: record.SnapshotSizeBytes,
		SnapshotPayload:   record.SnapshotPayload,
		KafkaPartition:    record.KafkaPartition,
		KafkaOffset:       record.KafkaOffset,
		KafkaTimestamp:    formatClickHouseDateTime64(record.KafkaTimestamp),
	}

	body, err := json.Marshal(row)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", a.config.clickhouseDatabase, a.config.clickhouseTable)
	return a.clickhouseQueryWithBody(ctx, query, bytes.NewReader(body))
}

func (a *app) clickhouseQuery(ctx context.Context, query string) error {
	return a.clickhouseQueryWithBody(ctx, query, nil)
}

func (a *app) clickhouseQueryWithBody(ctx context.Context, query string, body io.Reader) error {
	if strings.TrimSpace(a.config.clickhouseEndpoint) == "" {
		return nil
	}

	endpoint := strings.TrimRight(a.config.clickhouseEndpoint, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return err
	}
	req.URL.RawQuery = "query=" + urlQueryEscape(query)
	req.Header.Set("Content-Type", "application/json")
	if a.config.clickhouseUser != "" {
		req.SetBasicAuth(a.config.clickhouseUser, a.config.clickhousePassword)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("clickhouse returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (a *app) handleRoot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":         a.config.serviceName,
		"instanceId":      a.config.instanceID,
		"kafkaTopic":      a.config.kafkaTopic,
		"kafkaGroupID":    a.config.kafkaGroupID,
		"clickhouseTable": a.config.clickhouseTable,
		"requestCount":    a.requestCount.Add(1),
		"uptimeSec":       int64(time.Since(a.startedAt).Seconds()),
		"time":            time.Now().Format(time.RFC3339),
	})
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"service":    a.config.serviceName,
		"instanceId": a.config.instanceID,
		"time":       time.Now().Format(time.RFC3339),
	})
}

func (a *app) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":            a.config.serviceName,
		"instanceId":         a.config.instanceID,
		"kafkaTopic":         a.config.kafkaTopic,
		"kafkaGroupID":       a.config.kafkaGroupID,
		"clickhouseTable":    a.config.clickhouseTable,
		"clickhouseEndpoint": a.config.clickhouseEndpoint,
		"minioEndpoint":      a.config.minioEndpoint,
		"uptimeSec":          int64(time.Since(a.startedAt).Seconds()),
		"time":               time.Now().Format(time.RFC3339),
	})
}

func (a *app) withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(recorder, r)

		codeLabel := strconv.Itoa(recorder.statusCode)
		httpRequestsTotal.WithLabelValues(r.URL.Path, r.Method, codeLabel).Inc()
		httpRequestDurationSeconds.WithLabelValues(r.URL.Path, r.Method, codeLabel).Observe(time.Since(startedAt).Seconds())
		a.writeLog(logEntry{
			Level:  "info",
			Event:  "http_request_processed",
			Path:   r.URL.Path,
			Method: r.Method,
			Status: recorder.statusCode,
		})
	})
}

func loadConfig() config {
	instanceID := envOrDefault("INSTANCE_ID", envOrDefault("NOMAD_ALLOC_ID", hostnameOrDefault()))

	return config{
		serviceName:        envOrDefault("SERVICE_NAME", "event-consumer-demo"),
		instanceID:         instanceID,
		appPort:            envOrDefault("APP_PORT", "18083"),
		metricsPort:        envOrDefault("METRICS_PORT", "12115"),
		logPath:            envOrDefault("APP_LOG_PATH", "/app/logs/event-consumer-demo.log"),
		kafkaBrokers:       envCSV("KAFKA_BROKERS"),
		kafkaTopic:         envOrDefault("KAFKA_TOPIC", "user-session-events"),
		kafkaGroupID:       envOrDefault("KAFKA_GROUP_ID", "event-consumer-demo"),
		minioEndpoint:      envOrDefault("MINIO_ENDPOINT", ""),
		minioAccessKey:     envOrDefault("MINIO_ACCESS_KEY", ""),
		minioSecretKey:     envOrDefault("MINIO_SECRET_KEY", ""),
		minioBucket:        envOrDefault("MINIO_BUCKET", "login-snapshots"),
		minioUseSSL:        envBoolOrDefault("MINIO_USE_SSL", false),
		clickhouseEndpoint: envOrDefault("CLICKHOUSE_ENDPOINT", ""),
		clickhouseUser:     envOrDefault("CLICKHOUSE_USER", "default"),
		clickhousePassword: envOrDefault("CLICKHOUSE_PASSWORD", ""),
		clickhouseDatabase: envOrDefault("CLICKHOUSE_DATABASE", "app"),
		clickhouseTable:    envOrDefault("CLICKHOUSE_TABLE", "session_events"),
		consumerTimeout:    envDurationMillisOrDefault("CONSUMER_TIMEOUT_MS", 2000),
	}
}

func newLogger(logPath string) (*log.Logger, *os.File, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, nil, err
	}
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return log.New(io.MultiWriter(os.Stdout, file), "", 0), file, nil
}

func (a *app) writeLog(entry logEntry) {
	entry.Service = a.config.serviceName
	entry.InstanceID = a.config.instanceID
	entry.Timestamp = time.Now().Format(time.RFC3339)
	body, err := json.Marshal(entry)
	if err != nil {
		a.logger.Printf(`{"level":"error","event":"log_marshal_failed","service":"%s","instance_id":"%s","detail":%q,"ts":"%s"}`,
			a.config.serviceName,
			a.config.instanceID,
			err.Error(),
			time.Now().Format(time.RFC3339),
		)
		return
	}
	a.logger.Println(string(body))
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func parseEventTime(processedAt string, sentAt string) time.Time {
	for _, raw := range []string{processedAt, sentAt} {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		if ts, err := time.Parse(time.RFC3339, raw); err == nil {
			return ts
		}
	}
	return time.Now()
}

func formatClickHouseDateTime64(value time.Time) string {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return value.Format("2006-01-02 15:04:05.000")
	}
	return value.In(location).Format("2006-01-02 15:04:05.000")
}

func urlQueryEscape(value string) string {
	return url.QueryEscape(value)
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDurationMillisOrDefault(key string, fallback int) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Millisecond
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return time.Duration(fallback) * time.Millisecond
	}
	return time.Duration(value) * time.Millisecond
}

func envIntOrDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envBoolOrDefault(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envCSV(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func hostnameOrDefault() string {
	name, err := os.Hostname()
	if err != nil || strings.TrimSpace(name) == "" {
		return "unknown-host"
	}
	return name
}
