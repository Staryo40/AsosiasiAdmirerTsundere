FROM golang:1.26.8-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ai-summarizer ./cmd/ai-summarizer
COPY internal/platform ./internal/platform
COPY internal/ai-summarizer ./internal/ai-summarizer
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bnpb-ai-summarizer ./cmd/ai-summarizer
RUN mkdir -p /data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/bnpb-ai-summarizer /bnpb-ai-summarizer
COPY --from=build --chown=65532:65532 /data /data
USER 65532:65532
VOLUME ["/data"]
ENTRYPOINT ["/bnpb-ai-summarizer"]
