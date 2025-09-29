package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	Port                 string
	MonolithURL          string
	MoviesServiceURL     string
	EventsServiceURL     string
	RouteMoviesToService bool
	ClientTimeout        time.Duration
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func parseBool(k string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(k)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func mustParseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("bad URL %q: %v", raw, err)
	}
	return u
}

func loadConfig() Config {
	return Config{
		Port:                 getenv("PORT", "8000"),
		MonolithURL:          getenv("MONOLITH_URL", "http://localhost:8080"),
		MoviesServiceURL:     getenv("MOVIES_SERVICE_URL", "http://localhost:8081"),
		EventsServiceURL:     getenv("EVENTS_SERVICE_URL", "http://localhost:8082"),
		RouteMoviesToService: parseBool("ROUTE_MOVIES_TO_SERVICE", false),
		ClientTimeout:        5 * time.Second,
	}
}

type ctxKey string

const (
	ctxKeyReqID ctxKey = "req_id"
	ctxKeyRoute ctxKey = "route"
)

func genReqID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// крайне маловероятно
		ts := time.Now().UnixNano()
		return hex.EncodeToString([]byte{
			byte(ts >> 56), byte(ts >> 48), byte(ts >> 40), byte(ts >> 32),
			byte(ts >> 24), byte(ts >> 16), byte(ts >> 8), byte(ts),
		})
	}
	return hex.EncodeToString(b)
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = genReqID()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKeyReqID, id)))
	})
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		reqID, _ := r.Context().Value(ctxKeyReqID).(string)
		route, _ := r.Context().Value(ctxKeyRoute).(string)
		if route == "" {
			route = "-"
		}
		log.Printf("ts=%s method=%s path=%s status=%d dur_ms=%d req_id=%s route=%s bytes=%d",
			start.Format(time.RFC3339), r.Method, r.URL.Path, sw.status, time.Since(start).Milliseconds(), reqID, route, sw.bytes)
	})
}

// hop-by-hop заголовки, которые нельзя проксировать дальше
var hopByHop = map[string]struct{}{
	"Connection":          {},
	"Proxy-Connection":    {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		if _, skip := hopByHop[k]; skip {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func singleHopProxy(client *http.Client, w http.ResponseWriter, r *http.Request, base *url.URL, outPath string, routeLabel string) {
	ctx := r.Context()
	reqURL := *base // копия
	reqURL.Path = outPath
	reqURL.RawQuery = r.URL.RawQuery

	// Тело запроса (если есть) мы просто протаскиваем дальше
	req, err := http.NewRequestWithContext(ctx, r.Method, reqURL.String(), r.Body)
	if err != nil {
		http.Error(w, "build upstream request error", http.StatusBadGateway)
		return
	}
	copyHeaders(req.Header, r.Header)
	// X-Forwarded-* метки
	req.Header.Set("X-Forwarded-Host", r.Host)
	req.Header.Set("X-Forwarded-Proto", "http")

	// Проставим роут в контекст для логов
	r = r.WithContext(context.WithValue(ctx, ctxKeyRoute, routeLabel))
	w.Header().Set("X-Backend", routeLabel)

	resp, err := client.Do(req)
	if err != nil {
		// различать таймаут необязательно для учебного проекта
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		if _, skip := hopByHop[k]; skip {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func buildMux(cfg Config, client *http.Client) http.Handler {
	monolith := mustParseURL(cfg.MonolithURL)
	movies := mustParseURL(cfg.MoviesServiceURL)
	events := mustParseURL(cfg.EventsServiceURL)

	mux := http.NewServeMux()

	// health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"true"}`))
	})

	// /api/movies -> либо монолит (/api/movies...), либо сервис (/movies...)
	mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		// оставляем любые методы как есть
		inPath := r.URL.Path
		suffix := strings.TrimPrefix(inPath, "/api/movies") // включая возможный хвост /...
		if cfg.RouteMoviesToService {
			out := "/movies" + suffix
			singleHopProxy(client, w, r, movies, out, "movies")
			return
		}
		out := "/api/movies" + suffix
		singleHopProxy(client, w, r, monolith, out, "monolith")
	})
	mux.HandleFunc("/api/movies/", func(w http.ResponseWriter, r *http.Request) {
		inPath := r.URL.Path
		suffix := strings.TrimPrefix(inPath, "/api/movies")
		if cfg.RouteMoviesToService {
			out := "/movies" + suffix
			singleHopProxy(client, w, r, mustParseURL(cfg.MoviesServiceURL), out, "movies")
			return
		}
		out := "/api/movies" + suffix
		singleHopProxy(client, w, r, mustParseURL(cfg.MonolithURL), out, "monolith")
	})

	// /api/events -> всегда на events-service, путь сохраняем как есть
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		singleHopProxy(client, w, r, events, r.URL.Path, "events")
	})
	mux.HandleFunc("/api/events/", func(w http.ResponseWriter, r *http.Request) {
		singleHopProxy(client, w, r, events, r.URL.Path, "events")
	})

	// /api/users -> всегда на монолит, путь сохраняем как есть
	mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		singleHopProxy(client, w, r, monolith, r.URL.Path, "monolith")
	})
	mux.HandleFunc("/api/users/", func(w http.ResponseWriter, r *http.Request) {
		singleHopProxy(client, w, r, monolith, r.URL.Path, "monolith")
	})

	// дефолт — 404 как раньше
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	return withRequestID(withLogging(mux))
}

func main() {
	cfg := loadConfig()
	log.Printf("proxy-service :%s route_movies_to_service=%v monolith=%s movies=%s events=%s",
		cfg.Port, cfg.RouteMoviesToService, cfg.MonolithURL, cfg.MoviesServiceURL, cfg.EventsServiceURL,
	)

	client := &http.Client{
		Timeout: cfg.ClientTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	root := buildMux(cfg, client)
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           root,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-done
		log.Println("shutdown signal received")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server shutdown error: %v", err)
		} else {
			log.Println("server stopped gracefully")
		}
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}
