FROM golang:1.26.8-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod ./
COPY cmd/consumer-3 ./cmd/consumer-3
COPY internal/platform ./internal/platform
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bnpb-consumer-3 ./cmd/consumer-3

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/bnpb-consumer-3 /bnpb-consumer-3
USER 65532:65532
ENTRYPOINT ["/bnpb-consumer-3"]
