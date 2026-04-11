# Beat Exporter

[![CI](https://github.com/AstralJaeger/beat-exporter/actions/workflows/ci.yml/badge.svg)](https://github.com/AstralJaeger/beat-exporter/actions/workflows/ci.yml)
[![Release](https://github.com/AstralJaeger/beat-exporter/actions/workflows/release.yml/badge.svg)](https://github.com/AstralJaeger/beat-exporter/actions/workflows/release.yml)
[![Latest Release](https://img.shields.io/github/v/release/AstralJaeger/beat-exporter?sort=semver)](https://github.com/AstralJaeger/beat-exporter/releases/latest)
[![Go Version](https://img.shields.io/github/go-mod/go-version/AstralJaeger/beat-exporter)](go.mod)
[![Docker Pulls](https://ghcr-badge.egpl.dev/astraljaeger/beat-exporter/latest_tag?trim=major&label=ghcr)](https://github.com/AstralJaeger/beat-exporter/pkgs/container/beat-exporter)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A lightweight, zero-dependency metrics exporter for [Elastic Beats](https://www.elastic.co/beats/) built with [OpenTelemetry](https://opentelemetry.io/). It scrapes the built-in HTTP stats endpoint of any beat and exposes metrics either as a **Prometheus scrape target** or as an **OTLP push** to any OpenTelemetry-compatible backend.

## Supported Beats

| Beat        | Status          |
|-------------|-----------------|
| filebeat    | ✅ Full          |
| metricbeat  | ✅ Full          |
| auditbeat   | ⚠️ Partial       |
| packetbeat  | ⚠️ Partial       |

> Any beat that exposes the standard libbeat `/stats` endpoint will work partially out of the box — only beat-specific metric sections require dedicated support.

---

## Quick Start

### 1. Enable the HTTP stats endpoint in your beat config

```yaml
http:
  enabled: true
  host: localhost
  port: 5066
```

### 2. Run the exporter

**Binary:**
```bash
./beat-exporter --beat.uri=http://localhost:5066
```

**Docker:**
```bash
docker run --rm -p 9479:9479 \
  -e BEAT_EXPORTER_TYPE=prometheus \
  ghcr.io/astraljaeger/beat-exporter:latest \
  --beat.uri=http://host.docker.internal:5066
```

### 3. Point Prometheus at `http://localhost:9479/metrics`

---

## Docker Compose Example

```yaml
version: "3.8"
services:
  filebeat:
    image: docker.elastic.co/beats/filebeat:8.x.x
    volumes:
      - ./filebeat.yml:/usr/share/filebeat/filebeat.yml:ro
    # filebeat.yml must contain: http.enabled: true; http.host: 0.0.0.0; http.port: 5066

  beat-exporter:
    image: ghcr.io/astraljaeger/beat-exporter:latest
    ports:
      - "9479:9479"
    environment:
      BEAT_EXPORTER_TYPE: prometheus
    command:
      - --beat.uri=http://filebeat:5066

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    # prometheus.yml scrape_configs should target beat-exporter:9479/metrics
```

---

## Exporter Modes

### Prometheus (pull)

The default mode. The exporter listens on `:9479/metrics` and waits for Prometheus to scrape it.

```
BEAT_EXPORTER_TYPE=prometheus   # default
```

### OTLP (push)

Set `BEAT_EXPORTER_TYPE=otlp` and configure the standard [OTLP environment variables](https://opentelemetry.io/docs/specs/otel/protocol/exporter/):

```bash
BEAT_EXPORTER_TYPE=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer <token>
OTEL_METRIC_EXPORT_INTERVAL=60s   # default: 60s
OTEL_SERVICE_NAME=beat-exporter   # default: beat_exporter
```

In OTLP mode, the exporter does **not** start the Prometheus HTTP listener.

---

## Configuration Reference

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--beat.uri` | `http://localhost:5066` | HTTP API address of the beat |
| `--beat.timeout` | `10s` | Timeout for requests to the beat |
| `--beat.system` | `false` | Expose system-level stats (CPU cores, load averages) |
| `--web.listen-address` | `:9479` | Address for the Prometheus metrics endpoint |
| `--web.telemetry-path` | `/metrics` | Path to expose metrics |
| `--tls.certfile` | `""` | TLS certificate file (enables HTTPS) |
| `--tls.keyfile` | `""` | TLS key file (enables HTTPS) |
| `--exporter.type` | `prometheus` | Exporter backend: `prometheus` or `otlp` |
| `--version` | | Print version and exit |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `BEAT_EXPORTER_TYPE` | Default value for `--exporter.type` (`prometheus` or `otlp`); explicit CLI flag overrides it |
| `LOG_LEVEL` | Set log level (`debug` or `info`, default: `info`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint (used with `BEAT_EXPORTER_TYPE=otlp`) |
| `OTEL_EXPORTER_OTLP_HEADERS` | OTLP headers (e.g., auth tokens) |
| `OTEL_METRIC_EXPORT_INTERVAL` | Push interval for OTLP mode (default: `60s`) |
| `OTEL_SERVICE_NAME` | Service name reported in OTel resources |

### Unix Socket Support

```bash
./beat-exporter --beat.uri=unix:///var/run/beat.sock
```

---

## Building

### Local

```bash
go build \
  -ldflags "-X github.com/AstralJaeger/beat-exporter/internal/version.Version=$(git describe --tags --always)" \
  -o beat-exporter .
```

### Cross-compile

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o beat-exporter-linux-arm64 .
```

### Docker (multi-arch)

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg VERSION=$(git describe --tags --always) \
  -t ghcr.io/astraljaeger/beat-exporter:dev \
  --push .
```

---

## Container Image

Multi-arch images (`linux/amd64`, `linux/arm64`) are published to GitHub Container Registry on each release tag.

- **Base image:** [`gcr.io/distroless/static-debian12:nonroot`](https://github.com/GoogleContainerTools/distroless) — rootless, shell-free, minimal attack surface
- **No root:** runs as the `nonroot` user (UID 65532)
- **No shell:** distroless image contains only the binary and its dependencies

```bash
docker pull ghcr.io/astraljaeger/beat-exporter:latest
```

---

## Contributing

1. Fork the repository
2. Create a branch: `git checkout -b feat/my-feature`
3. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/)
4. Push and open a Pull Request

Please open an [Issue](https://github.com/AstralJaeger/beat-exporter/issues) before starting large changes.

---

## License

[MIT](LICENSE) — see `LICENSE` for details.
