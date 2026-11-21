FROM golang:1.26.8-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/satellite ./cmd/satellite
COPY internal/platform ./internal/platform
COPY internal/satellite ./internal/satellite
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bnpb-satellite ./cmd/satellite
RUN mkdir -p /data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/bnpb-satellite /bnpb-satellite
COPY --from=build --chown=65532:65532 /data /data
USER 65532:65532
VOLUME ["/data"]
ENTRYPOINT ["/bnpb-satellite"]
