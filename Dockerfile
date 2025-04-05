# syntax=docker/dockerfile:1

FROM golang:1.19 as builder

WORKDIR /app
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer

# Copy the go source
COPY . .

RUN go mod download

# Enable OpenTelemetry support by linking with the Go instrumentation library
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Build the binary with OpenTelemetry support
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags=netgo,osusergo,static_build -a -o derelay .


# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static-debian12

WORKDIR /
COPY --from=builder /app/derelay .

# Set OpenTelemetry environment variables
ENV OTEL_SERVICE_NAME="derelay" \
    OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4317" \
    OTEL_LOG_LEVEL="info"


ENTRYPOINT ["/derelay"]