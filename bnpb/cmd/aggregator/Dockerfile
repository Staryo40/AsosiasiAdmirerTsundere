FROM golang:1.26.8-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/aggregator ./cmd/aggregator
COPY internal/platform ./internal/platform
COPY internal/aggregator ./internal/aggregator
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bnpb-aggregator ./cmd/aggregator

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/bnpb-aggregator /bnpb-aggregator
USER 65532:65532
ENTRYPOINT ["/bnpb-aggregator"]
