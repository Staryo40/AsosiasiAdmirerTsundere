#!/bin/sh
# Idempotent broker provisioning: queue bound policies and one RabbitMQ user
# per publisher/consumer, each scoped to only its own queue. Topology changes
# use PUT, so re-running converges to the same state.
set -eu

base="http://rabbitmq:15672/api"
admin="${RABBITMQ_DEFAULT_USER}:${RABBITMQ_DEFAULT_PASS}"
umask 077
scratch=$(mktemp -d /tmp/broker-init.XXXXXX)
trap 'rm -rf "$scratch"' EXIT
trap 'exit 1' HUP INT TERM
correlation_id=$(cat /proc/sys/kernel/random/uuid)

now_ms() { awk '{printf "%.0f\n", $1 * 1000}' /proc/uptime; }
started_ms=$(now_ms)

log() {
	jq -cn --arg event "$1" --arg category "$2" --argjson duration "$3" --argjson attempt "$4" \
		--arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg cid "$correlation_id" \
		--arg status "${http_status:-000}" \
		--arg method "${http_method:-}" --arg path "${request_path:-}" \
		'{service:"broker-init",timestamp:$timestamp,correlation_id:$cid,event:$event,
		  method:$method,path:$path,duration_ms:$duration,attempt:$attempt,status:($status|tonumber),category:$category}'
}

seconds_setting() {
	case "$2" in ''|*[!0-9]*|0|0*) log broker_configuration_failed "$1" 0 0; exit 1;; esac
	if [ "$2" -gt 3600 ]; then log broker_configuration_failed "$1" 0 0; exit 1; fi
}

wait_seconds=${BROKER_WAIT_TIMEOUT_SECONDS:-120}
job_seconds=${BROKER_JOB_TIMEOUT_SECONDS:-180}
seconds_setting BROKER_WAIT_TIMEOUT_SECONDS "$wait_seconds"
seconds_setting BROKER_JOB_TIMEOUT_SECONDS "$job_seconds"
job_deadline=$((started_ms + job_seconds * 1000))
wait_deadline=$((started_ms + wait_seconds * 1000))
[ "$wait_deadline" -le "$job_deadline" ] || wait_deadline=$job_deadline
active_deadline=$wait_deadline

remaining_ms() {
	remaining=$((active_deadline - $(now_ms)))
	if [ "$remaining" -le 0 ]; then
		log broker_timeout deadline "$(( $(now_ms) - started_ms ))" 0
		exit 1
	fi
}

pause() {
	remaining_ms
	sleep "$(awk -v ms="$remaining" 'BEGIN {printf "%.3f", (ms < 1000 ? ms : 1000)/1000}')"
}

request_category() {
	case "$curl_exit" in
		0) ;;
		5|6|7|18|28|52|55|56) category=transport; return;;
		*) category=curl_configuration; return;;
	esac
	case "$http_status" in
		200|201|204) category=success;;
		5??) category=server_error;;
		401|403) category=auth_failed;;
		*) category=invalid_response;;
	esac
}

request() {
	remaining_ms
	http_method=$1
	request_path=$2
	request_timeout=$(awk -v ms="$remaining" 'BEGIN {printf "%.3f", (ms < 5000 ? ms : 5000)/1000}')
	curl_exit=0
	# Discard server bodies and curl diagnostics from logs: either can contain secrets.
	metrics=$(curl -q -s --connect-timeout 2 --max-time "$request_timeout" \
		-u "$admin" -H 'Content-Type: application/json' -H "X-Correlation-ID: $correlation_id" \
		-X "$1" "$base$2" --data-binary "$3" -o "$scratch/response" \
		-w '%{http_code} %{time_total}' 2>/dev/null) || curl_exit=$?
	http_status=${metrics%% *}
	duration_ms=$(awk -v seconds="${metrics#* }" 'BEGIN {printf "%.0f", seconds*1000}')
	request_category
	log broker_request "$category" "$duration_ms" "$attempt"
}

permanent_failure() {
	case "$category" in
		success|transport|server_error) return;;
		auth_failed) log broker_auth_failed credential_or_persisted_user_mismatch "$duration_ms" "$attempt";;
		*) log broker_request_failed "$category" "$duration_ms" "$attempt";;
	esac
	exit 1
}

wait_for() {
	probe_path=$1
	attempt=1
	while :; do
		request GET "$probe_path" ''
		permanent_failure
		if [ "$category" = success ]; then
			[ "$http_status" = 200 ] || { log broker_request_failed invalid_response "$duration_ms" "$attempt"; exit 1; }
			return
		fi
		log broker_wait "$category" "$duration_ms" "$attempt"
		pause
		attempt=$((attempt + 1))
	done
}

wait_for /whoami
if ! jq -e --arg expected "$RABBITMQ_DEFAULT_USER" \
	'.name == $expected and (.tags | type == "array" and index("administrator") != null)' "$scratch/response" >/dev/null 2>&1; then
	log broker_auth_failed invalid_admin_identity "$duration_ms" "$attempt"
	exit 1
fi
wait_for /health/checks/ready-to-serve-clients
if ! jq -e '.status == "ok"' "$scratch/response" >/dev/null 2>&1; then
	log broker_request_failed invalid_readiness "$duration_ms" "$attempt"
	exit 1
fi
log broker_ready success "$(( $(now_ms) - started_ms ))" "$attempt"
active_deadline=$job_deadline

put() {
	put_path=$1
	put_body=$2
	attempt=1
	while :; do
		request PUT "$put_path" "$put_body"
		permanent_failure
		[ "$category" != success ] || return 0
		if [ "$attempt" -ge 3 ]; then log broker_request_failed retry_exhausted "$duration_ms" "$attempt"; exit 1; fi
		pause
		attempt=$((attempt + 1))
	done
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

# Declared here (not left to the Aggregator's own idempotent declare) so a
# consumer can start, bind, and be tested before the Aggregator ever runs.
put "/exchanges/%2F/hazard.events" '{"type":"fanout","durable":true}'

# Main queues (hazard.events.<role>): 3h TTL, 10k cap, oldest dropped first,
# queue deleted after 24h with no consumer.
ensure_policy main-bounds '^hazard\.events\.[a-z0-9-]+$' \
	'{"message-ttl":10800000,"max-length":10000,"overflow":"drop-head","expires":86400000}'

# Failed queues (hazard.events.<role>.failed): 24h TTL, 1k cap. No separate
# expiry; a failed queue is operator-facing and should not quietly vanish.
ensure_policy failed-bounds '^hazard\.events\.[a-z0-9-]+\.failed$' \
	'{"message-ttl":86400000,"max-length":1000,"overflow":"drop-head"}'

# Publisher: may declare and publish into the exchange, never reads anything.
ensure_user "$BNPB_RABBITMQ_PUBLISHER_USER" "$BNPB_RABBITMQ_PUBLISHER_PASSWORD"
ensure_permissions "$BNPB_RABBITMQ_PUBLISHER_USER" '^hazard\.events$' '^hazard\.events$' '^$'

# Each consumer: may declare its own queue and its own failed queue
# (configure); write covers publishing to its failed queue and binding its
# queue to the exchange, which needs write on the queue (already covered)
# plus read on the exchange, so read spans both the exchange and its queue.
provision_consumer() {
	role="$1"
	password="$2"
	queue="hazard.events.$role"
	queue_pattern=$(printf '%s' "$queue" | sed 's/\./\\./g')
	ensure_user "$role" "$password"
	# Publishing to the failed queue goes through the default exchange
	# (routing key = queue name), which RabbitMQ checks as write access to
	# the exchange literally named "amq.default", not the queue name.
	ensure_permissions "$role" \
		"^${queue_pattern}(\\.failed)?\$" \
		"^(hazard\\.events|amq\\.default|${queue_pattern}(\\.failed)?)\$" \
		"^(hazard\\.events|${queue_pattern})\$"
}

provision_consumer "$BNPB_CONSUMER_1_RABBITMQ_USER" "$BNPB_CONSUMER_1_RABBITMQ_PASSWORD"
provision_consumer "$BNPB_CONSUMER_2_RABBITMQ_USER" "$BNPB_CONSUMER_2_RABBITMQ_PASSWORD"
provision_consumer "$BNPB_CONSUMER_3_RABBITMQ_USER" "$BNPB_CONSUMER_3_RABBITMQ_PASSWORD"

remaining_ms
log broker_provisioning_complete success "$(( $(now_ms) - started_ms ))" 0
