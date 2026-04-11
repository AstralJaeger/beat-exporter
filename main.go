package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/AstralJaeger/beat-exporter/collector"
	"github.com/AstralJaeger/beat-exporter/internal/service"
	"github.com/AstralJaeger/beat-exporter/internal/version"
)

const serviceName = "beat_exporter"
const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 30 * time.Second
	httpWriteTimeout      = 30 * time.Second
	httpIdleTimeout       = 120 * time.Second
)

var errStopDuringDiscovery = errors.New("stop signal received during beat discovery")

func main() {
	os.Exit(run())
}

func run() int {
	var (
		listenAddress = flag.String("web.listen-address", ":9479", "Address to listen on for web interface and telemetry.")
		tlsCertFile   = flag.String("tls.certfile", "", "TLS certificate file (enables HTTPS).")
		tlsKeyFile    = flag.String("tls.keyfile", "", "TLS key file (enables HTTPS).")
		metricsPath   = flag.String("web.telemetry-path", "/metrics", "Path under which to expose Prometheus metrics.")
		beatURI       = flag.String("beat.uri", "http://localhost:5066", "HTTP API address of the beat.")
		beatTimeout   = flag.Duration("beat.timeout", 10*time.Second, "Timeout for requests to the beat stats endpoint.")
		showVersion   = flag.Bool("version", false, "Print version information and exit.")
		systemBeat    = flag.Bool("beat.system", false, "Expose system-level stats (load, CPU cores).")
		exporterType  = flag.String("exporter.type", envOrDefault("BEAT_EXPORTER_TYPE", "prometheus"),
			"Exporter backend: 'prometheus' (pull) or 'otlp' (push). Defaults from BEAT_EXPORTER_TYPE; explicit flag wins.")
	)
	flag.Parse()

	if *showVersion {
		fmt.Print(version.Print(serviceName))
		return 0
	}

	setupLogging()

	slog.Info("Starting beat-exporter",
		"version", version.Version,
		"revision", version.Revision,
		"branch", version.Branch,
		"buildDate", version.BuildDate,
		"exporterType", *exporterType,
	)

	beatURL, err := url.Parse(*beatURI)
	if err != nil {
		slog.Error("Invalid beat.uri", "err", err)
		return 1
	}

	httpClient := &http.Client{Timeout: *beatTimeout}

	if beatURL.Scheme == "unix" {
		httpClient, beatURL = collector.NewHTTPClientWithUnixSocket(beatURL, *beatTimeout)
	}

	stopCh := make(chan bool, 4)

	if setupErr := service.SetupServiceListener(stopCh, serviceName); setupErr != nil {
		slog.Warn("Could not set up service listener", "err", setupErr)
	}

	slog.Info("Discovering beat type", "url", beatURL.String())
	beatInfo, err := discoverBeat(httpClient, *beatURL, stopCh)
	if err != nil {
		if errors.Is(err, errStopDuringDiscovery) {
			slog.Info("Stop signal received during beat discovery")
			return 0
		}
		slog.Error("Failed to discover beat", "err", err)
		return 1
	}

	// Build OTel MeterProvider
	ctx := context.Background()
	otelServiceName := envOrDefault("OTEL_SERVICE_NAME", serviceName)
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(otelServiceName),
		semconv.ServiceVersion(version.Version),
	)

	var mp *sdkmetric.MeterProvider
	var mux *http.ServeMux

	switch strings.ToLower(*exporterType) {
	case "otlp":
		mp, err = buildOTLPProvider(ctx, res)
		if err != nil {
			slog.Error("Failed to create OTLP MeterProvider", "err", err)
			return 1
		}
		slog.Info("OTLP metrics push enabled")

	default: // "prometheus"
		registry := promclient.NewRegistry()
		mp, err = buildPrometheusProvider(registry, res)
		if err != nil {
			slog.Error("Failed to create Prometheus MeterProvider", "err", err)
			return 1
		}
		mux = http.NewServeMux()
		mux.Handle(*metricsPath, promhttp.HandlerFor(registry, promhttp.HandlerOpts{
			ErrorLog:      slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
			ErrorHandling: promhttp.ContinueOnError,
		}))
		mux.HandleFunc("/", indexHandler(*metricsPath))
		slog.Info("Prometheus metrics endpoint enabled", "path", *metricsPath)
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mp.Shutdown(shutdownCtx); err != nil {
			slog.Error("Error shutting down MeterProvider", "err", err)
		}
	}()

	meter := mp.Meter(serviceName)
	if err := registerBuildInfoMetric(meter); err != nil {
		slog.Error("Failed to register build info metric", "err", err)
		return 1
	}

	if _, err := collector.NewBeatCollector(meter, httpClient, beatURL, beatInfo, *systemBeat); err != nil {
		slog.Error("Failed to register beat collector", "err", err)
		return 1
	}

	slog.Info("Exporter started",
		"beat", beatInfo.Beat,
		"listenAddress", *listenAddress,
		"serviceName", otelServiceName,
	)

	var server *http.Server
	if mux != nil {
		server = &http.Server{
			Addr:              *listenAddress,
			Handler:           mux,
			ReadHeaderTimeout: httpReadHeaderTimeout,
			ReadTimeout:       httpReadTimeout,
			WriteTimeout:      httpWriteTimeout,
			IdleTimeout:       httpIdleTimeout,
		}
		go func() {
			var serverErr error
			if *tlsCertFile != "" && *tlsKeyFile != "" {
				serverErr = server.ListenAndServeTLS(*tlsCertFile, *tlsKeyFile)
			} else {
				serverErr = server.ListenAndServe()
			}
			if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
				slog.Error("HTTP server error", "err", serverErr)
			}
			stopCh <- true
		}()
	} else {
		slog.Info("OTLP mode active: HTTP listener disabled")
	}

	<-stopCh
	if server != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Error shutting down HTTP server", "err", err)
		}
	}
	slog.Info("Shutting down beat-exporter")
	return 0
}

// discoverBeat polls the beat endpoint until it responds, then returns the BeatInfo.
// It returns errStopDuringDiscovery if a stop signal arrives first.
func discoverBeat(client *http.Client, beatURL url.URL, stopCh <-chan bool) (*collector.BeatInfo, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			info, err := collector.LoadBeatInfo(client, beatURL)
			if err != nil {
				slog.Warn("Beat not yet reachable, retrying in 1s", "err", err)
				continue
			}
			return info, nil
		case <-stopCh:
			return nil, errStopDuringDiscovery
		}
	}
}

func buildPrometheusProvider(registry *promclient.Registry, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	exporter, err := prometheusexporter.New(
		prometheusexporter.WithRegisterer(registry),
		prometheusexporter.WithoutScopeInfo(),
		prometheusexporter.WithoutTargetInfo(),
	)
	if err != nil {
		return nil, err
	}
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	), nil
}

func buildOTLPProvider(ctx context.Context, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(envDurationOrDefault("OTEL_METRIC_EXPORT_INTERVAL", 60*time.Second)),
		)),
		sdkmetric.WithResource(res),
	), nil
}

// indexHandler returns a simple HTML landing page for the Prometheus mode.
func indexHandler(metricsPath string) http.HandlerFunc {
	body := fmt.Sprintf(strings.TrimSpace(`
<html>
<head><title>Beat Exporter</title></head>
<body>
<h1>Beat Exporter</h1>
<p><a href='%s'>Metrics</a></p>
</body>
</html>`), metricsPath)
	b := []byte(body)
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(b)
	}
}

func setupLogging() {
	level := slog.LevelInfo
	if strings.ToLower(os.Getenv("LOG_LEVEL")) == "debug" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationOrDefault(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func registerBuildInfoMetric(meter otelmetric.Meter) error {
	buildInfoGauge, err := meter.Float64ObservableGauge(
		"beat_exporter_build_info",
		otelmetric.WithDescription("Build information about beat_exporter"),
	)
	if err != nil {
		return err
	}
	buildInfoAttrs := otelmetric.WithAttributes(
		attribute.String("version", version.Version),
		attribute.String("revision", version.Revision),
		attribute.String("branch", version.Branch),
		attribute.String("goversion", runtime.Version()),
		attribute.String("build_user", version.BuildUser),
		attribute.String("build_date", version.BuildDate),
	)

	_, err = meter.RegisterCallback(func(_ context.Context, obs otelmetric.Observer) error {
		obs.ObserveFloat64(
			buildInfoGauge,
			1,
			buildInfoAttrs,
		)
		return nil
	}, buildInfoGauge)
	return err
}
