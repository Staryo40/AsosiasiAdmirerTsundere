"""P02 acceptance harness; uses only Python standard-library modules."""
from __future__ import annotations

import argparse
import http.client
import json
import math
import os
import platform
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote, urlsplit

from testlib import (assert_status, bearer, bnpb_login, load_dotenv, metadata,
                     pvmbg_login, repository_root, request, require_value,
                     results_directory, timestamp)

SERVICES = ("api", "aggregator", "auth", "bmkg", "pvmbg")
CONFIG_NAMES = {
    "BNPB_MAX_IN_FLIGHT", "BNPB_READ_MAX_IN_FLIGHT", "BNPB_DB_READ_MAX_CONNS", "BNPB_DB_WRITE_MAX_CONNS",
    "BNPB_BMKG_POLL_INTERVAL", "BNPB_PVMBG_POLL_INTERVAL", "BNPB_BMKG_HTTP_TIMEOUT",
    "BNPB_PVMBG_HTTP_TIMEOUT", "BNPB_PVMBG_CYCLE_TIMEOUT", "BNPB_DB_READ_TIMEOUT", "BNPB_DB_WRITE_TIMEOUT",
    "BNPB_AUTH_ACCESS_TOKEN_TTL", "BNPB_RETRY_MAX_BACKOFF", "BNPB_LIST_DEFAULT_LIMIT", "BNPB_LIST_MAX_LIMIT",
    "BNPB_INGEST_MAX_RESPONSE_BYTES", "BMKG_GENERATOR_INTERVAL_SECONDS", "PVMBG_GENERATOR_INTERVAL_SECONDS",
    "PVMBG_RESPONSE_DELAY_MIN_MS", "PVMBG_RESPONSE_DELAY_MAX_MS",
}
GROUPS = ("bmkg", "pvmbg", "combined")
PATHS = {
    "bmkg": "/v1/hazards?source=BMKG&limit=100&view=full",
    "pvmbg": "/v1/hazards?source=PVMBG&limit=100&view=full",
    "combined": "/v1/hazards?limit=100&view=full",
}


def dump(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def docker(*arguments: str) -> str:
    completed = subprocess.run(["docker", *arguments], cwd=repository_root(),
                               capture_output=True, text=True, timeout=30)
    if completed.returncode:
        # Docker config may contain secrets: never include command output in errors.
        raise AssertionError(f"docker {' '.join(arguments[:2])} failed (exit {completed.returncode}).")
    return completed.stdout


def container_state() -> dict:
    result = {}
    for service in SERVICES:
        ids = docker("compose", "ps", "-a", "-q", service).split()
        if len(ids) != 1:
            raise AssertionError(f"Expected exactly one Compose container for {service}.")
        inspection = json.loads(docker("inspect", ids[0]))[0]
        state = inspection["State"]
        result[service] = {
            "id": inspection["Id"], "started_at": state["StartedAt"],
            "restart_count": inspection["RestartCount"], "running": state["Running"],
            "health": state.get("Health", {}).get("Status"),
            "configuration": {name: value for item in inspection["Config"].get("Env", [])
                              for name, _, value in (item.partition("="),) if name in CONFIG_NAMES},
        }
    return result


def unchanged(before: dict, after: dict) -> None:
    for service in SERVICES:
        for field in ("id", "started_at", "restart_count"):
            if before[service][field] != after[service][field]:
                raise AssertionError(f"{service} changed {field} during the test.")
        if not after[service]["running"]:
            raise AssertionError(f"{service} is no longer running.")


class Client:
    """Renew a shared session ahead of expiry; previously issued tokens remain valid.

    Login is outside the measured data path. Avoid refresh rotation during load:
    revoking a token that another worker is using would introduce artificial 401s.
    """

    def __init__(self, api: str):
        self.api = api.rstrip("/")
        self.env = load_dotenv(repository_root() / "bnpb/.env")
        self.lock = threading.Lock()
        self.logins = 0
        self.renew_at = 0.0
        self.access = ""
        self.token()

    def token(self) -> str:
        with self.lock:
            if time.monotonic() >= self.renew_at:
                session = bnpb_login(self.api, require_value(self.env, "BNPB_FIELD_CLIENT_ID"),
                                     require_value(self.env, "BNPB_FIELD_CLIENT_SECRET"))
                ttl = int(session["expires_in"])
                if ttl < 5:
                    raise AssertionError("P02 requires access TTL >=5 seconds.")
                self.access = session["access_token"]
                self.renew_at = time.monotonic() + ttl / 2
                self.logins += 1
            return self.access

    def hazards(self, group: str) -> dict:
        response = request("GET", self.api + PATHS[group], headers=bearer(self.token()), timeout=5)
        self.last_latency_ms = response.duration_ms
        assert_status(response, 200, group + " snapshot")
        envelope = response.json()
        if not isinstance(envelope.get("items"), list) or not isinstance(envelope.get("sources"), dict):
            raise AssertionError("Invalid hazard envelope.")
        return envelope


class Admin:
    def __init__(self, base: str):
        self.base = base.rstrip("/")
        self.env = load_dotenv(repository_root() / "pvmbg/.env")
        self.renew_at = 0.0
        self.access = ""

    def token(self) -> str:
        if time.monotonic() >= self.renew_at:
            session = pvmbg_login(self.base, require_value(self.env, "PVMBG_ADMIN_CLIENT_ID"),
                                   require_value(self.env, "PVMBG_ADMIN_CLIENT_SECRET"))
            self.access = session["access_token"]
            self.renew_at = time.monotonic() + int(session["expires_in"]) / 2
        return self.access

    def set(self, operation: str, body: dict) -> None:
        response = request("POST", self.base + "/admin/" + operation,
                           headers=bearer(self.token()), body=body)
        assert_status(response, 204, "PVMBG admin " + operation)


@contextmanager
def slow_pvmbg(admin: Admin, args: argparse.Namespace, evidence: dict):
    # No GET endpoint exposes runtime delay; restore configured defaults or an
    # explicit operator override. Record this assumption in every result.
    restore_min = args.restore_delay_min
    restore_max = args.restore_delay_max
    if restore_min is None:
        restore_min = int(admin.env.get("PVMBG_RESPONSE_DELAY_MIN_MS", "500"))
    if restore_max is None:
        restore_max = int(admin.env.get("PVMBG_RESPONSE_DELAY_MAX_MS", "3000"))
    evidence["restore_delay"] = {"min_ms": restore_min, "max_ms": restore_max,
                                  "origin": "env/default or explicit CLI override; runtime state has no GET endpoint"}
    if not 0 <= restore_min <= restore_max:
        raise AssertionError("Invalid restoration delay range.")
    health = request("GET", admin.base + "/health")
    assert_status(health, 200, "PVMBG baseline health (outage must be off)")
    token = admin.token()
    try:
        admin.set("response-delay", {"min_ms": 3000, "max_ms": 3000})
        response = request("GET", admin.base + "/volcanic-reports?since=" + quote(timestamp(), safe=""),
                           headers=bearer(token))
        assert_status(response, 200, "PVMBG 3-second delay probe")
        evidence["delay_probe_ms"] = response.duration_ms
        if response.duration_ms < 2950:
            raise AssertionError("PVMBG probe did not exhibit the configured 3-second delay.")
        yield
    finally:
        admin.set("response-delay", {"min_ms": restore_min, "max_ms": restore_max})
        evidence["delay_restored"] = True


def source_summary(envelope: dict) -> dict:
    return {"sources": envelope["sources"], "items_count": len(envelope["items"]),
            "newest": [{"hazard_id": item["hazard_id"], "source": item["source"],
                        "occurred_at": item["occurred_at"]} for item in envelope["items"][:3]]}


def newest(envelope: dict) -> str:
    if not envelope["items"]:
        raise AssertionError("Baseline must contain hazard data; wait for ingestion.")
    return max(item["occurred_at"] for item in envelope["items"])


def has_new(envelope: dict, baseline_stamp: str) -> bool:
    return any(item["occurred_at"] > baseline_stamp for item in envelope["items"])


def percentile(values: list[float], fraction: float):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def metrics(rows: list[dict], seconds: float) -> dict:
    statuses = Counter(row["status"] for row in rows)
    success = [row["latency_ms"] for row in rows if row["status"] == 200]
    all_latencies = [row["latency_ms"] for row in rows]
    unexpected = sum(count for status, count in statuses.items() if status not in (200, 429))
    denominator = len(rows) - statuses[429]
    completed = [row for row in rows if row.get("completed_in_window", True)]
    completed_successes = sum(row["status"] == 200 for row in completed)
    def distribution(values):
        return {"p50_ms": percentile(values, .50), "p95_ms": percentile(values, .95),
                "p99_ms": percentile(values, .99)}
    return {
        "requests": len(rows), "completed_in_window": len(completed),
        "status_counts": dict(statuses), "successful_requests": statuses[200],
        "controlled_429": statuses[429], "unexpected_errors": unexpected,
        "unexpected_error_rate_excluding_429": unexpected / denominator if denominator else None,
        "unexpected_error_rate_all_requests": unexpected / len(rows) if rows else None,
        "rejection_rate": statuses[429] / len(rows) if rows else None,
        "throughput_rps": len(completed) / seconds, "successful_throughput_rps": completed_successes / seconds,
        "successful_latency": distribution(success), "all_latency": distribution(all_latencies),
    }


def load(client: Client, path: Path, workers: int, duration: float, warmup: float) -> dict:
    """Closed-model HTTP/1.1 load with one real keep-alive connection per thread."""
    parsed = urlsplit(client.api)
    if parsed.scheme not in ("http", "https") or parsed.path not in ("", "/"):
        raise AssertionError("API_BASE must be an http(s) origin without a path.")
    lock = threading.Lock()
    sockets: dict[int, dict] = {}
    rows: list[dict] = []
    failures: list[str] = []
    ready = [threading.Event() for _ in range(workers)]
    stop = threading.Event()
    window = [float("inf"), float("inf")]

    base_connection = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection

    class Connection(base_connection):
        def __init__(self, worker: int):
            super().__init__(parsed.hostname, parsed.port, timeout=5)
            self.worker = worker

        def connect(self):
            super().connect()
            with lock:
                sockets[self.worker] = {"worker": self.worker, "local": list(self.sock.getsockname()),
                                        "peer": list(self.sock.getpeername())}

        def close(self):
            with lock:
                sockets.pop(self.worker, None)
            super().close()

    with (path / "requests.jsonl").open("w", encoding="utf-8") as raw:
        def worker(index: int):
            connection = Connection(index)
            group = GROUPS[index % len(GROUPS)]
            try:
                while not stop.is_set():
                    started = time.perf_counter()
                    status = 0
                    error = None
                    try:
                        connection.request("GET", PATHS[group], headers={
                            "Authorization": "Bearer " + client.token(), "Connection": "keep-alive",
                            "X-Correlation-ID": f"p02-worker-{index}",
                        })
                        response = connection.getresponse()
                        status = response.status
                        body = response.read()
                        if status == 200:
                            payload = json.loads(body)
                            if not isinstance(payload.get("items"), list):
                                status = 0
                                error = "invalid_envelope"
                        ready[index].set()
                    except (OSError, http.client.HTTPException, ValueError, AssertionError):
                        error = "transport_or_payload_error"
                        status = 0
                        connection.close()
                    finished = time.perf_counter()
                    if window[0] <= started < window[1]:
                        row = {"worker": index, "group": group, "status": status,
                               "elapsed_seconds": round(started - window[0], 6),
                               "latency_ms": round((finished - started) * 1000, 3),
                               "completed_in_window": finished <= window[1], "error": error}
                        with lock:
                            raw.write(json.dumps(row) + "\n")
                            rows.append(row)
            except Exception as error:
                with lock:
                    failures.append(type(error).__name__)
            finally:
                connection.close()

        threads = [threading.Thread(target=worker, args=(index,), daemon=True) for index in range(workers)]
        for thread in threads:
            thread.start()
        samples = []
        try:
            deadline = time.perf_counter() + 30
            while not all(event.is_set() for event in ready):
                if time.perf_counter() >= deadline or failures:
                    raise AssertionError("Could not initialize every load worker within 30 seconds.")
                time.sleep(.1)
            print(f"Warm-up: {warmup}s; {workers} dedicated HTTP/1.1 connections", flush=True)
            time.sleep(warmup)
            window[0] = time.perf_counter()
            window[1] = window[0] + duration
            next_progress = 0.0
            while time.perf_counter() < window[1]:
                elapsed = time.perf_counter() - window[0]
                with lock:
                    endpoints = list(sockets.values())
                # Count distinct real TCP endpoint pairs, never virtual users.
                unique = {tuple(item["local"] + item["peer"]) for item in endpoints}
                samples.append({"elapsed_seconds": round(elapsed, 6), "socket_count": len(unique),
                                "sockets": endpoints})
                if elapsed >= next_progress:
                    print(f"Load {elapsed:.0f}/{duration:.0f}s: sockets={len(unique)}", flush=True)
                    next_progress += 10
                time.sleep(min(.5, max(0, window[1] - time.perf_counter())))
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=20)
            dump(path / "connections.json", samples)
        if any(thread.is_alive() for thread in threads):
            raise AssertionError("Load workers did not terminate within their timeout.")
    completed = [row for row in rows if row["completed_in_window"]]
    span = samples[-1]["elapsed_seconds"] - samples[0]["elapsed_seconds"] if len(samples) > 1 else 0
    return {
        "tool": "Python standard library http.client", "python_version": platform.python_version(),
        "workers": workers, "model": "closed, one sequential request per persistent HTTP/1.1 connection; no think time",
        "request_mix_workers": dict(Counter(GROUPS[index % 3] for index in range(workers))),
        "warmup_seconds": warmup, "sustained_seconds": duration, "timeout_seconds": 5,
        "socket_sampling_interval_seconds": .5, "minimum_observed_sockets": min((s["socket_count"] for s in samples), default=0),
        "socket_observation_span_seconds": span, "worker_failures": failures,
        "metrics": metrics(rows, duration),
        "by_group": {group: metrics([r for r in rows if r["group"] == group], duration) for group in GROUPS},
        "late_completions_excluded_from_throughput": len(rows) - len(completed),
    }


def assert_load(result: dict, *, latency: bool, sustained: bool) -> None:
    if result["worker_failures"]:
        raise AssertionError("A load worker crashed.")
    metric = result["metrics"]
    rate = metric["unexpected_error_rate_excluding_429"]
    if rate is None or rate >= .01 or not metric["successful_requests"]:
        raise AssertionError("Unexpected error rate must be <1%, with successful data responses.")
    for group, group_metrics in result["by_group"].items():
        if not group_metrics["successful_requests"]:
            raise AssertionError(f"No successful data response for {group}.")
    if latency:
        p95 = result["by_group"]["bmkg"]["successful_latency"]["p95_ms"]
        if p95 is None or p95 >= 300:
            raise AssertionError(f"BMKG-only successful p95={p95}ms; required <300ms.")
    if sustained and (result["workers"] < 50 or result["sustained_seconds"] < 60
                      or result["minimum_observed_sockets"] < 50
                      or result["socket_observation_span_seconds"] < 60):
        raise AssertionError("Required >=50 observed TCP sockets throughout a >=60-second sampled interval.")


def test_load(test_id: str, args, path: Path, evidence: dict) -> None:
    client = Client(args.api)
    admin = Admin(args.pvmbg)
    before = container_state()
    evidence["containers_before"] = before
    baseline = client.hazards("bmkg")
    baseline_stamp = newest(baseline)
    evidence["baseline_bmkg"] = source_summary(baseline)
    with slow_pvmbg(admin, args, evidence):
        # This includes a completed volcanic poll with normal 3s upstream delay.
        time.sleep(7)
        evidence["load"] = load(client, path, args.workers, args.duration, args.warmup)
        after_bmkg = client.hazards("bmkg")
        evidence["after_bmkg"] = source_summary(after_bmkg)
        evidence["new_bmkg_event"] = has_new(after_bmkg, baseline_stamp)
        if not evidence["new_bmkg_event"]:
            raise AssertionError("No new BMKG event ingested while PVMBG was slow.")
    evidence["containers_after"] = container_state()
    unchanged(before, evidence["containers_after"])
    evidence["session_logins"] = client.logins
    assert_load(evidence["load"], latency=test_id == "T03", sustained=test_id == "T04")


def test_outage(args, path: Path, evidence: dict) -> None:
    client = Client(args.api)
    admin = Admin(args.pvmbg)
    assert_status(request("GET", admin.base + "/health"), 200, "Baseline PVMBG health")
    before = container_state()
    evidence["containers_before"] = before
    baseline = {group: client.hazards(group) for group in ("bmkg", "pvmbg")}
    stamps = {group: newest(envelope) for group, envelope in baseline.items()}
    if any(envelope["sources"][source]["status"] != "healthy"
           for group, source in (("bmkg", "BMKG"), ("pvmbg", "PVMBG"))
           for envelope in (baseline[group],)):
        raise AssertionError("Both sources must be healthy before the outage.")
    evidence["baseline"] = {group: source_summary(envelope) for group, envelope in baseline.items()}
    observations = []
    degraded = False
    new_bmkg = False
    try:
        admin.set("outage", {"enabled": True})
        assert_status(request("GET", admin.base + "/health"), 503, "Injected PVMBG outage")
        start = time.monotonic()
        while time.monotonic() - start < args.outage_seconds:
            bmkg = client.hazards("bmkg")
            bmkg_latency_ms = client.last_latency_ms
            # Cached data is present at baseline, so require a useful HTTP 200.
            volcanic = client.hazards("pvmbg")
            volcanic_latency_ms = client.last_latency_ms
            state = volcanic["sources"]["PVMBG"]
            if state["status"] in ("stale", "unavailable"):
                if state["status"] == "stale" and not state.get("stale_since"):
                    raise AssertionError("PVMBG stale response lacks stale_since.")
                if not volcanic["items"]:
                    raise AssertionError("Cached volcanic data disappeared during outage.")
                degraded = True
            if bmkg["sources"]["BMKG"]["status"] != "healthy":
                raise AssertionError("BMKG freshness degraded during the PVMBG outage.")
            new_bmkg = new_bmkg or has_new(bmkg, stamps["bmkg"])
            observations.append({"elapsed_seconds": round(time.monotonic() - start, 2),
                                 "bmkg_latency_ms": bmkg_latency_ms, "pvmbg_latency_ms": volcanic_latency_ms,
                                 "bmkg": source_summary(bmkg), "pvmbg": source_summary(volcanic)})
            if len(observations) % 5 == 0:
                print(f"Outage {time.monotonic()-start:.0f}s: PVMBG={state['status']}, BMKG healthy", flush=True)
            time.sleep(args.sample_interval)
        if not degraded or not new_bmkg:
            raise AssertionError("Outage did not demonstrate stale/unavailable PVMBG and new BMKG data.")
    finally:
        # Admin can log in even during outage; restore even after a failed check.
        admin.set("outage", {"enabled": False})
        evidence["outage_disabled"] = True
        dump(path / "outage-observations.json", observations)
    deadline = time.monotonic() + args.recovery_timeout
    while time.monotonic() < deadline:
        bmkg = client.hazards("bmkg")
        volcanic = client.hazards("pvmbg")
        if (volcanic["sources"]["PVMBG"]["status"] == "healthy"
                and has_new(volcanic, stamps["pvmbg"]) and has_new(bmkg, stamps["bmkg"])):
            evidence["recovery"] = {"bmkg": source_summary(bmkg), "pvmbg": source_summary(volcanic)}
            break
        time.sleep(args.sample_interval)
    else:
        raise AssertionError("PVMBG did not recover with new events before recovery timeout.")
    evidence["containers_after"] = container_state()
    unchanged(before, evidence["containers_after"])
    evidence["degradation_observed"] = degraded
    evidence["new_bmkg_during_outage"] = new_bmkg


def run(test_id: str) -> int:
    parser = argparse.ArgumentParser(description=f"P02/{test_id} acceptance test")
    parser.add_argument("--api", default=os.getenv("API_BASE", "http://localhost:8080"))
    parser.add_argument("--pvmbg", default=os.getenv("PVMBG_BASE", "http://localhost:8082"))
    parser.add_argument("--workers", type=int, default=50 if test_id == "T04" else 6)
    parser.add_argument("--duration", type=float, default=65 if test_id == "T04" else 30)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--restore-delay-min", type=int)
    parser.add_argument("--restore-delay-max", type=int)
    parser.add_argument("--outage-seconds", type=float, default=60)
    parser.add_argument("--sample-interval", type=float, default=2)
    parser.add_argument("--recovery-timeout", type=float, default=90)
    args = parser.parse_args()
    if args.workers < 3 or args.duration <= 0 or args.warmup < 0 or args.sample_interval <= 0 or args.outage_seconds <= 0 or args.recovery_timeout <= 0:
        parser.error("Need >=3 workers, positive durations/intervals, and nonnegative warm-up.")
    path = results_directory("P02") / test_id
    path.mkdir(parents=True, exist_ok=True)
    evidence = metadata(test_id, timestamp())
    evidence.update({"result": "FAIL", "configuration": vars(args),
                     "machine": {"os": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count()}})
    try:
        assert_status(request("GET", args.api + "/ready"), 200, "API readiness")
        evidence["docker_version"] = docker("--version").strip()
        evidence["compose_version"] = docker("compose", "version").strip()
        if test_id == "T05":
            test_outage(args, path, evidence)
        else:
            test_load(test_id, args, path, evidence)
        evidence["result"] = "PASS"
        return_code = 0
    except Exception as error:
        evidence["failure"] = str(error)
        print(f"{test_id} FAIL: {error}", flush=True)
        return_code = 1
    finally:
        evidence["finished_at"] = timestamp()
        dump(path / "result.json", evidence)
        print(f"{test_id} {evidence['result']}: {path / 'result.json'}", flush=True)
    return return_code
