#!/bin/sh
# Idempotent broker provisioning: queue bound policies (Decision 1/2) and one
# RabbitMQ user per publisher/consumer, each scoped to only its own queue
# (KI.5-6). Run once per `docker compose up` against the management HTTP API;
# every call here is a PUT, so re-running just converges to the same state.
set -eu

base="http://rabbitmq:15672/api"
admin="${RABBITMQ_DEFAULT_USER}:${RABBITMQ_DEFAULT_PASS}"

put() {
	path="$1"
	body="$2"
	curl -fsS -u "$admin" -H 'Content-Type: application/json' -X PUT "$base$path" -d "$body" >/dev/null
}

ensure_policy() {
	name="$1"
	pattern="$2"
	definition_json="$3"
	body=$(jq -n --arg p "$pattern" --argjson d "$definition_json" '{pattern: $p, "apply-to": "queues", definition: $d}')
	put "/policies/%2F/$name" "$body"
}

ensure_user() {
	user="$1"
	password="$2"
	body=$(jq -n --arg p "$password" '{password: $p, tags: ""}')
	put "/users/$user" "$body"
}

ensure_permissions() {
	user="$1"
	configure="$2"
	write="$3"
	read="$4"
	body=$(jq -n --arg c "$configure" --arg w "$write" --arg r "$read" '{configure: $c, write: $w, read: $r}')
	put "/permissions/%2F/$user" "$body"
}

# Main queues (hazard.events.<role>): 3h TTL, 10k cap, oldest dropped first,
# queue deleted after 24h with no consumer. See Decision 1.
ensure_policy main-bounds '^hazard\.events\.[a-z0-9-]+$' \
	'{"message-ttl":10800000,"max-length":10000,"overflow":"drop-head","expires":86400000}'

# Failed queues (hazard.events.<role>.failed): 24h TTL, 1k cap. No separate
# expiry; a failed queue is operator-facing and should not quietly vanish.
ensure_policy failed-bounds '^hazard\.events\.[a-z0-9-]+\.failed$' \
	'{"message-ttl":86400000,"max-length":1000,"overflow":"drop-head"}'

# Publisher: may declare and publish into the exchange, never reads anything.
ensure_user "$BNPB_RABBITMQ_PUBLISHER_USER" "$BNPB_RABBITMQ_PUBLISHER_PASSWORD"
ensure_permissions "$BNPB_RABBITMQ_PUBLISHER_USER" '^hazard\.events$' '^hazard\.events$' '^$'

# Each consumer: may declare/bind its own queue and its own failed queue
# (configure), bind to the exchange and publish to its failed queue (write),
# and read only its own queue. The queue name is the role, e.g. "dashboard".
provision_consumer() {
	role="$1"
	password="$2"
	queue="hazard.events.$role"
	queue_pattern=$(printf '%s' "$queue" | sed 's/\./\\./g')
	ensure_user "$role" "$password"
	ensure_permissions "$role" \
		"^${queue_pattern}(\\.failed)?\$" \
		"^(hazard\\.events|${queue_pattern}(\\.failed)?)\$" \
		"^${queue_pattern}\$"
}

provision_consumer "$BNPB_CONSUMER_1_RABBITMQ_USER" "$BNPB_CONSUMER_1_RABBITMQ_PASSWORD"
provision_consumer "$BNPB_CONSUMER_2_RABBITMQ_USER" "$BNPB_CONSUMER_2_RABBITMQ_PASSWORD"
provision_consumer "$BNPB_CONSUMER_3_RABBITMQ_USER" "$BNPB_CONSUMER_3_RABBITMQ_PASSWORD"

echo "broker provisioning complete"
