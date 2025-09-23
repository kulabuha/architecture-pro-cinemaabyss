package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	mrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
	ClientTimeout          time.Duration
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func parseBoolEnv(k string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
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

func parseIntBounded(k string, def, min, max int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	if i < min {
		return min
	}
	if i > max {
		return max
	}
	return i
}

func loadConfig() (Config, error) {
	cfg := Config{
		Port:                   getEnv("PORT", "8000"),
		MonolithURL:            getEnv("MONOLITH_URL", "http://localhost:8080"),
		MoviesServiceURL:       getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"),
		EventsServiceURL:       getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"),
		GradualMigration:       parseBoolEnv("GRADUAL_MIGRATION", false),
		MoviesMigrationPercent: parseIntBounded("MOVIES_MIGRATION_PERCENT", 0, 0, 100),
		ClientTimeout:          3 * time.Second,
	}
	// validate base URLs
	if _, err := url.ParseRequestURI(cfg.MonolithURL); err != nil {
		return cfg, errors.New("invalid MONOLITH_URL")
	}
	if _, err := url.ParseRequestURI(cfg.MoviesServiceURL); err != nil {
		return cfg, errors.New("invalid MOVIES_SERVICE_URL")
	}
	return cfg, nil
}

// request-scoped context key
type ctxKey string

const (
	ctxKeyReqID ctxKey = "req_id"
	ctxKeyRoute ctxKey = "route"
)

// generateRequestID returns a 16-byte hex string.
func generateRequestID() string {
	b := make([]byte, 16)
	if _, err := crand.Read(b); err != nil {
		for i := range b {
			b[i] = byte(mrand.Intn(256))
		}
	}
	return hex.EncodeToString(b)
}

// responseWriter wrapper to capture status and bytes written

type statusWriter struct {
	w          http.ResponseWriter
	statusCode int
	bytes      int
}

func (sw *statusWriter) Header() http.Header { return sw.w.Header() }
func (sw *statusWriter) Write(b []byte) (int, error) {
	n, err := sw.w.Write(b)
	sw.bytes += n
	return n, err
}
func (sw *statusWriter) WriteHeader(statusCode int) {
	sw.statusCode = statusCode
	sw.w.WriteHeader(statusCode)
}
func newStatusWriter(w http.ResponseWriter) *statusWriter {
	return &statusWriter{w: w, statusCode: http.StatusOK}
}

// middleware: inject/request Request-ID and logging
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = generateRequestID()
		}
		w.Header().Set("X-Request-ID", reqID)
		r = r.WithContext(context.WithValue(r.Context(), ctxKeyReqID, reqID))
		next.ServeHTTP(w, r)
	})
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := newStatusWriter(w)
		next.ServeHTTP(sw, r)
		reqID, _ := r.Context().Value(ctxKeyReqID).(string)
		route, _ := r.Context().Value(ctxKeyRoute).(string)
		if route == "" {
			route = "-"
		}
		log.Printf("ts=%s method=%s path=%s status=%d dur_ms=%d req_id=%s route=%s bytes=%d",
			start.Format(time.RFC3339), r.Method, r.URL.Path, sw.statusCode, time.Since(start).Milliseconds(), reqID, route, sw.bytes,
		)
	})
}

// upstream chooser for movies: returns baseURL and route label
func chooseMoviesUpstream(cfg Config) (base string, route string) {
	if cfg.GradualMigration && cfg.MoviesMigrationPercent > 0 {
		if mrand.Intn(100) < cfg.MoviesMigrationPercent {
			return cfg.MoviesServiceURL, "movies"
		}
	}
	return cfg.MonolithURL, "monolith"
}

// minimal header allowlist to propagate
var passHeaders = []string{"Authorization", "X-Request-ID", "X-User-ID"}

// proxyTryPaths tries several upstream paths, falling back on 404/405
func proxyTryPaths(client *http.Client, w http.ResponseWriter, r *http.Request, upstreamBase string, paths []string) {
	for i, p := range paths {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)

		u, err := url.Parse(upstreamBase)
		if err != nil {
			cancel()
			http.Error(w, "bad upstream base", http.StatusBadGateway)
			return
		}
		joined, err := url.JoinPath(u.Path, p)
		if err != nil {
			cancel()
			http.Error(w, "bad upstream path", http.StatusBadGateway)
			return
		}
		u.Path = joined
		u.RawQuery = r.URL.RawQuery
		log.Printf("proxy -> %s", u.String())

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			cancel()
			http.Error(w, "cannot build upstream request", http.StatusBadGateway)
			return
		}
		for _, h := range passHeaders {
			if v := r.Header.Get(h); v != "" {
				req.Header.Set(h, v)
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded") {
				http.Error(w, "upstream timeout", http.StatusGatewayTimeout)
				return
			}
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}

		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
			_ = resp.Body.Close()
			cancel()
			if i < len(paths)-1 {
				continue
			}
			w.WriteHeader(resp.StatusCode)
			return
		}

		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		if cc := resp.Header.Get("Cache-Control"); cc != "" {
			w.Header().Set("Cache-Control", cc)
		}
		if etag := resp.Header.Get("ETag"); etag != "" {
			w.Header().Set("ETag", etag)
		}
		if lm := resp.Header.Get("Last-Modified"); lm != "" {
			w.Header().Set("Last-Modified", lm)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		resp.Body.Close()
		cancel()
		return
	}
}

// buildMux constructs the HTTP handler tree so we can unit-test routing easily.
func buildMux(cfg Config, client *http.Client) http.Handler {
	mux := http.NewServeMux()

	// /health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// movies handler (supports both /api/movies and /api/movies/..)
	moviesHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		base, route := chooseMoviesUpstream(cfg)
		r = r.WithContext(context.WithValue(r.Context(), ctxKeyRoute, route))
		w.Header().Set("X-Backend", route)

		paths := []string{"/api/movies", "/movies"}
		if route == "movies" {
			paths = []string{"/movies", "/api/movies"}
		}
		proxyTryPaths(client, w, r, base, paths)
	})
	mux.Handle("/api/movies", moviesHandler)
	mux.Handle("/api/movies/", moviesHandler)

	// users (always monolith for now)
	usersHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), ctxKeyRoute, "monolith"))
		w.Header().Set("X-Backend", "monolith")
		// pass through as-is
		u, _ := url.Parse(cfg.MonolithURL)
		joined, _ := url.JoinPath(u.Path, "/api/users")
		u.Path = joined
		u.RawQuery = r.URL.RawQuery
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
		for _, h := range passHeaders {
			if v := r.Header.Get(h); v != "" {
				req.Header.Set(h, v)
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})
	mux.Handle("/api/users", usersHandler)
	mux.Handle("/api/users/", usersHandler)

	// custom NotFound with log
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("NotFound path=%s", r.URL.Path)
		http.NotFound(w, r)
	})

	return withRequestID(withLogging(mux))
}

func main() {
	// seed RNG for gradual migration
	mrand.Seed(time.Now().UnixNano())

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	log.Printf("starting proxy-service on :%s (gradual=%v percent=%d) monolith=%s movies=%s",
		cfg.Port, cfg.GradualMigration, cfg.MoviesMigrationPercent, cfg.MonolithURL, cfg.MoviesServiceURL,
	)

	client := &http.Client{Timeout: cfg.ClientTimeout, Transport: &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 100, IdleConnTimeout: 90 * time.Second}}

	root := buildMux(cfg, client)

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: root, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}

	// graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-done
		log.Println("shutdown signal received")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("server shutdown error: %v", err)
		} else {
			log.Println("server stopped gracefully")
		}
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}
