FROM golang:1.26.8-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/field-intake ./cmd/field-intake
COPY internal/platform ./internal/platform
COPY internal/field-intake ./internal/field-intake
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bnpb-field-intake ./cmd/field-intake
RUN mkdir -p /data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/bnpb-field-intake /bnpb-field-intake
COPY --from=build --chown=65532:65532 /data /data
USER 65532:65532
VOLUME ["/data"]
ENTRYPOINT ["/bnpb-field-intake"]
