FROM golang:1.26.8-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/consumer-1 ./cmd/consumer-1
COPY internal/platform ./internal/platform
COPY internal/consumer-1 ./internal/consumer-1
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bnpb-consumer-1 ./cmd/consumer-1
RUN mkdir -p /data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/bnpb-consumer-1 /bnpb-consumer-1
COPY --from=build --chown=65532:65532 /data /data
USER 65532:65532
VOLUME ["/data"]
ENTRYPOINT ["/bnpb-consumer-1"]
