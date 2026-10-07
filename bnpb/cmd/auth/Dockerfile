FROM golang:1.26.8-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/auth ./cmd/auth
COPY internal/authservice ./internal/authservice
COPY internal/platform ./internal/platform
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bnpb-auth ./cmd/auth

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/bnpb-auth /bnpb-auth
USER 65532:65532
ENTRYPOINT ["/bnpb-auth"]
