# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM golang:1.26-alpine AS builder

WORKDIR /build

# Download dependencies first (cached layer)
COPY go.mod go.sum ./
RUN go mod download

# Build the binary
COPY . .
ARG VERSION=dev
ARG REVISION=unknown
ARG BRANCH=unknown
ARG BUILD_DATE=unknown
ARG BUILD_USER=docker

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w \
      -X github.com/AstralJaeger/beat-exporter/internal/version.Version=${VERSION} \
      -X github.com/AstralJaeger/beat-exporter/internal/version.Revision=${REVISION} \
      -X github.com/AstralJaeger/beat-exporter/internal/version.Branch=${BRANCH} \
      -X github.com/AstralJaeger/beat-exporter/internal/version.BuildDate=${BUILD_DATE} \
      -X github.com/AstralJaeger/beat-exporter/internal/version.BuildUser=${BUILD_USER}" \
    -tags 'netgo static_build' \
    -a -o beat-exporter .

# ---- Runtime stage ----
# distroless/static-debian12:nonroot provides a minimal, rootless, shell-free image.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/beat-exporter /bin/beat-exporter

EXPOSE 9479

ENTRYPOINT ["/bin/beat-exporter"]
