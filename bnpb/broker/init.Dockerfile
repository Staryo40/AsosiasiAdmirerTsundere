FROM alpine:3.20
RUN apk add --no-cache curl jq
COPY broker/provision.sh /broker/provision.sh
USER 65534:65534
ENTRYPOINT ["/bin/sh", "/broker/provision.sh"]
