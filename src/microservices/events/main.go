package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	Port    string
	Brokers []string
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() (Config, error) {
	brokersCSV := getEnv("KAFKA_BROKERS", "kafka:9092")
	brokers := make([]string, 0)
	for _, b := range strings.Split(brokersCSV, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS is empty")
	}
	return Config{
		Port:    getEnv("PORT", "8082"),
		Brokers: brokers,
	}, nil
}

// Доступные типы событий и соответствующие Kafka-топики
var topics = map[string]string{
	"user":    "user-events",
	"payment": "payment-events",
	"movie":   "movie-events",
}

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	log.Printf("events-service starting on :%s, brokers=%v", cfg.Port, cfg.Brokers)

	// Создаём Kafka writers по топикам
	writers := make(map[string]*kafka.Writer, len(topics))
	for kind, topic := range topics {
		writers[kind] = &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Topic:                  topic,
			RequiredAcks:           kafka.RequireAll,
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: true,
		}
	}
	defer func() {
		for _, w := range writers {
			_ = w.Close()
		}
	}()

	// Контекст приложения и graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Стартуем консюмеры по каждому топику (для наглядности в логах)
	for kind, topic := range topics {
		go consumeLoop(ctx, cfg.Brokers, topic, "events-service-"+kind)
	}

	mux := http.NewServeMux()

	// Health (исправлено: добавлен ведущий слэш и /api префикс)
	mux.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":true}`))
	})

	// Register both /api/events and /events for учебной совместимости
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "missing event kind", http.StatusBadRequest)
	})
	mux.HandleFunc("/api/events/", func(w http.ResponseWriter, r *http.Request) {
		eventsHandler(w, r, writers)
	})

	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "missing event kind", http.StatusBadRequest)
	})
	mux.HandleFunc("/events/", func(w http.ResponseWriter, r *http.Request) {
		eventsHandler(w, r, writers)
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		ctxShut, cancelShut := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShut()
		_ = srv.Shutdown(ctxShut)
	}()

	log.Printf("http listening on :%s", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func eventsHandler(w http.ResponseWriter, r *http.Request, writers map[string]*kafka.Writer) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	kind, ok := extractKind(r.URL.Path)
	if !ok {
		http.Error(w, "missing or unknown event kind", http.StatusBadRequest)
		return
	}
	topic, ok := topics[kind]
	if !ok {
		http.Error(w, "unknown event kind", http.StatusBadRequest)
		return
	}

	// читаем payload как raw JSON (может быть пустым)
	var raw json.RawMessage
	if r.Body != nil {
		defer r.Body.Close()
		b, err := ioReadAllLimit(r.Body, 1<<20) // 1 MiB
		if err != nil {
			if err.Error() == "body too large" {
				http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		if len(strings.TrimSpace(string(b))) > 0 {
			raw = json.RawMessage(b)
		} else {
			raw = json.RawMessage("{}")
		}
	} else {
		raw = json.RawMessage("{}")
	}

	e := Event{
		ID:        uuid.NewString(),
		Type:      kind,
		Timestamp: time.Now().UTC(),
		Payload:   raw,
	}
	val, _ := json.Marshal(e)

	ctxReq, cancelReq := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancelReq()

	if err := writers[kind].WriteMessages(ctxReq, kafka.Message{
		Key:   []byte(e.ID),
		Value: val,
	}); err != nil {
		log.Printf("produce error: topic=%s id=%s err=%v", topic, e.ID, err)
		http.Error(w, "kafka write failed", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"success","topic":"%s","id":"%s"}`, topic, e.ID)))
}

// extractKind разбирает путь и вытаскивает {kind} для /api/events/{kind} и /events/{kind}
func extractKind(path string) (string, bool) {
	p := strings.Trim(path, "/")
	seg := strings.Split(p, "/")
	if len(seg) < 2 {
		return "", false
	}
	if seg[0] == "api" {
		// ожидаем api/events/{kind}
		if len(seg) < 3 || seg[1] != "events" {
			return "", false
		}
		return strings.ToLower(seg[2]), true
	}
	if seg[0] == "events" {
		if len(seg) < 2 {
			return "", false
		}
		return strings.ToLower(seg[1]), true
	}
	return "", false
}

// helpers
func ioReadAllLimit(r io.Reader, limit int64) ([]byte, error) {
	var b strings.Builder
	buf := make([]byte, 4096)
	var n int64
	for {
		c, err := r.Read(buf)
		if c > 0 {
			n += int64(c)
			if n > limit {
				return []byte{}, errors.New("body too large")
			}
			b.Write(buf[:c])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return []byte{}, err
		}
	}
	return []byte(b.String()), nil
}

func consumeLoop(ctx context.Context, brokers []string, topic, group string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     group,
		Topic:       topic,
		StartOffset: kafka.LastOffset, // читать новые сообщения
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()
	log.Printf("consumer started: topic=%s group=%s", topic, group)
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			log.Printf("consumer error: topic=%s err=%v", topic, err)
			return
		}
		var e Event
		if err := json.Unmarshal(m.Value, &e); err != nil {
			log.Printf("consume decode error: topic=%s err=%v", topic, err)
			continue
		}
		log.Printf("consumed: topic=%s key=%s id=%s type=%s ts=%s payload=%s", topic, string(m.Key), e.ID, e.Type, e.Timestamp.Format(time.RFC3339), safePreview(e.Payload, 256))
	}
}

func safePreview(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}
