# Shared helpers for the Milestone 2 evidence scripts in testing/M2-P2, testing/M2-P3 and testing/M2-P4.
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent))
from testlib import (  # noqa: E402
    HttpResult, assert_status, bearer, bnpb_login, git, load_dotenv, pvmbg_login, repository_root, request, require_value,
)

NAMESPACE = os.getenv("K8S_NAMESPACE", "sistem-informasi-kebencanaan-bnpb")
# Profiled Compose services must be enabled for kill, logs and exec to find them.
COMPOSE_ENV = {**os.environ, "COMPOSE_PROFILES": "satellite,field-reports"}


def parser(description: str) -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=description)
    result.add_argument("--target", choices=("compose", "k8s"), default=os.getenv("M2_TARGET", "compose"),
                        help="where the system runs; only changes how pods are stopped and inspected")
    return result


class Target:
    def __init__(self, name: str):
        self.name = name
        self.root = repository_root()
        node = os.getenv("NODE_IP", "localhost")
        k8s = name == "k8s"
        self.api = os.getenv("API_BASE", f"http://{node}:30080" if k8s else "http://localhost:8080")
        self.aggregator = os.getenv("AGGREGATOR_BASE", f"http://{node}:30083" if k8s else "http://localhost:8083")
        # PVMBG, field-intake and Prometheus are reached through kubectl port-forward on k8s.
        self.pvmbg = os.getenv("PVMBG_BASE", "http://localhost:8082")
        self.bmkg = os.getenv("BMKG_BASE", "http://localhost:8081")
        self.intake = os.getenv("INTAKE_BASE", "http://localhost:8088")
        self.prometheus = os.getenv("PROMETHEUS_BASE", f"http://{node}:30900" if k8s else "http://localhost:9090")
        self._env: dict[str, str] | None = None

    # Credentials -----------------------------------------------------------------
    def env(self) -> dict[str, str]:
        if self._env is None:
            if self.name == "k8s":
                values: dict[str, str] = {}
                for name in ("config/identities.env", "config/pvmbg.env", "secrets.env"):
                    values |= load_dotenv(self.root / "k8s" / name)
            else:
                values = load_dotenv(self.root / "pvmbg/.env") | load_dotenv(self.root / "bnpb/.env")
            self._env = values
        return self._env

    def value(self, name: str) -> str:
        return require_value(self.env(), name)

    def token(self, role: str) -> str:
        return bnpb_login(self.api, self.value(f"BNPB_{role}_CLIENT_ID"), self.value(f"BNPB_{role}_CLIENT_SECRET"))["access_token"]

    def pvmbg_admin(self, path: str, body: dict[str, Any]) -> None:
        credentials = {"client_id": self.value("PVMBG_ADMIN_CLIENT_ID"), "client_secret": self.value("PVMBG_ADMIN_CLIENT_SECRET")}
        # PVMBG rate-limits logins; a scenario switches faults often enough to hit it.
        for _ in range(8):
            login = request("POST", f"{self.pvmbg}/auth/login", body=credentials)
            if login.status != 429:
                break
            time.sleep(10)
        assert_status(login, 200, "PVMBG admin login")
        token = login.json()["access_token"]
        assert_status(request("POST", f"{self.pvmbg}/admin/{path}", headers=bearer(token), body=body), 204, f"PVMBG admin {path}")

    # Processes -------------------------------------------------------------------
    def run(self, *command: str, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(command, cwd=self.root, env=COMPOSE_ENV, check=check, text=True,
                              stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE if capture else None)

    def kubectl(self, *arguments: str, check: bool = True) -> str:
        return self.run("kubectl", "-n", NAMESPACE, *arguments, check=check).stdout

    def pods(self, component: str) -> list[dict[str, Any]]:
        listing = json.loads(self.kubectl("get", "pods", "-l", f"app.kubernetes.io/name={component}", "-o", "json"))
        return listing["items"]

    def restart_count(self, component: str) -> int:
        if self.name == "k8s":
            return sum(status.get("restartCount", 0) for pod in self.pods(component)
                       for status in pod.get("status", {}).get("containerStatuses", []))
        container = self.run("docker", "compose", "ps", "-q", component).stdout.strip()
        if not container:
            raise AssertionError(f"Compose service {component} is not running.")
        state = json.loads(self.run("docker", "inspect", container).stdout)[0]
        if not state["State"]["Running"]:
            raise AssertionError(f"Compose service {component} has stopped.")
        return int(state["RestartCount"])

    def kill(self, component: str, pod: str | None = None) -> None:
        if self.name == "k8s":
            names = [pod] if pod else [item["metadata"]["name"] for item in self.pods(component)]
            self.kubectl("delete", "pod", "--force", "--grace-period=0", "--wait=false", *names)
        else:
            self.run("docker", "compose", "kill", component)
            self.run("docker", "compose", "up", "-d", "--no-deps", component)

    def stop(self, component: str) -> None:
        if self.name == "k8s":
            self.kubectl("scale", "statefulset", component, "--replicas=0")
        else:
            self.run("docker", "compose", "stop", component)

    def start(self, component: str, replicas: int = 1) -> None:
        if self.name == "k8s":
            self.kubectl("scale", "statefulset", component, f"--replicas={replicas}")
        else:
            self.run("docker", "compose", "up", "-d", "--no-deps", component)

    def satellite_leader(self) -> str:
        if self.name != "k8s":
            return "satellite"
        return self.lease_holder("satellite-feed")

    def lease_holder(self, lease: str) -> str:
        return self.kubectl("get", "lease", lease, "-o", "jsonpath={.spec.holderIdentity}", check=False).strip()

    def require_k8s(self) -> None:
        if self.name != "k8s":
            raise SystemExit("This scenario needs --target k8s: Compose runs a single Aggregator.")

    def pod_ips(self, component: str) -> dict[str, str]:
        return {pod["status"]["podIP"]: pod["metadata"]["name"] for pod in self.pods(component) if pod.get("status", {}).get("podIP")}

    def ready(self, pod: str, port: int) -> dict[str, Any]:
        # Through the API server proxy, so no port-forward per pod is needed.
        result = self.run("kubectl", "get", "--raw", f"/api/v1/namespaces/{NAMESPACE}/pods/{pod}:{port}/proxy/ready", check=False)
        try:
            return json.loads(result.stdout)
        except ValueError:
            return {"error": (result.stderr or result.stdout).strip()[:200]}

    def bmkg_events(self, since: datetime) -> list[dict[str, Any]]:
        stamp = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        response = request("GET", f"{self.bmkg}/seismic-events?{urlencode({'since': stamp})}",
                           headers={"X-BMKG-Key": self.value("BMKG_API_KEY" if self.name == "k8s" else "BNPB_BMKG_API_KEY")})
        assert_status(response, 200, "BMKG seismic events")
        return response.json()

    def satellite_verify(self, pod: str | None = None) -> dict[str, Any]:
        pod = pod or self.satellite_leader()
        if self.name == "k8s":
            result = self.run("kubectl", "-n", NAMESPACE, "exec", pod, "--", "/bnpb-satellite", "-verify", check=False)
        else:
            result = self.run("docker", "compose", "exec", "-T", "satellite", "/bnpb-satellite", "-verify", check=False)
        if not result.stdout.strip():
            raise AssertionError(f"satellite -verify produced no report: {result.stderr.strip()[:300]}")
        return json.loads(result.stdout)

    def logs(self, component: str, since: datetime, pod: str | None = None) -> list[dict[str, Any]]:
        stamp = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if self.name == "k8s":
            names = [pod] if pod else [item["metadata"]["name"] for item in self.pods(component)]
            return [record for name in names
                    for record in parse_log_lines(self.kubectl("logs", name, "--timestamps", f"--since-time={stamp}", check=False), name)]
        text = self.run("docker", "compose", "logs", "--no-log-prefix", "--timestamps", "--since", stamp, component).stdout
        return parse_log_lines(text, component)


# Each record gains _pod and _time, the runtime's timestamp for the line.
def parse_log_lines(text: str, pod: str) -> list[dict[str, Any]]:
    records = []
    for line in text.splitlines():
        start = line.find("{")
        if start < 0:
            continue
        try:
            record = json.loads(line[start:])
        except ValueError:
            continue
        if isinstance(record.get("fields"), dict):
            record = record["fields"] | record
        prefix = line[:start].strip().split(" ")[0]
        try:
            record["_time"] = parse_time(prefix[:26] + prefix[-1] if len(prefix) > 27 else prefix)
        except ValueError:
            record["_time"] = None
        record["_pod"] = pod
        records.append(record)
    return records


# Keeps the logs of pods that are killed during a scenario.
class LogFollower:
    def __init__(self, target: Target, component: str, since: datetime):
        self.target, self.component, self.since = target, component, since
        self.processes: list[tuple[str, subprocess.Popen]] = []
        stamp = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for pod in target.pods(component):
            name = pod["metadata"]["name"]
            process = subprocess.Popen(["kubectl", "-n", NAMESPACE, "logs", "-f", name, "--timestamps", f"--since-time={stamp}"],
                                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            self.processes.append((name, process))

    def stop(self) -> list[dict[str, Any]]:
        seen, records = set(), []
        for name, process in self.processes:
            process.terminate()
            text, _ = process.communicate(timeout=10)
            for record in parse_log_lines(text, name):
                records.append(record)
                seen.add((name, json.dumps(record, sort_keys=True, default=str)))
        for record in self.target.logs(self.component, self.since):
            key = (record["_pod"], json.dumps(record, sort_keys=True, default=str))
            if key not in seen:
                seen.add(key)
                records.append(record)
        return records


# Paged readers ---------------------------------------------------------------------
def failed_messages(target: Target, token: str, source: str | None = None) -> list[dict[str, Any]]:
    items, cursor = [], None
    while True:
        query = {"limit": 20} | ({"source": source} if source else {}) | ({"cursor": cursor} if cursor else {})
        response = request("GET", f"{target.api}/v1/ops/failed-messages?{urlencode(query)}", headers=bearer(token))
        assert_status(response, 200, "Ops failed message listing")
        page = response.json()
        items.extend(page["items"])
        cursor = page.get("next_cursor")
        if not cursor:
            return items


def hazards(target: Target, token: str, source: str) -> list[dict[str, Any]]:
    items, cursor = [], None
    for _ in range(1000):
        query = {"source": source, "limit": 500, "view": "full"} | ({"cursor": cursor} if cursor else {})
        response = request("GET", f"{target.api}/v1/hazards?{urlencode(query)}", headers=bearer(token), timeout=30)
        assert_status(response, 200, f"{source} hazard listing")
        page = response.json()
        items.extend(page["items"])
        if not page.get("next_cursor") or not page["items"]:
            return items
        cursor = page["next_cursor"]
    raise AssertionError("Hazard listing did not finish.")


def payload_json(item: dict[str, Any]) -> Any:
    try:
        return json.loads(base64.b64decode(item["payload_base64"]))
    except ValueError:
        return None


# Utilities -------------------------------------------------------------------------
def now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def wait_for(label: str, check: Callable[[], Any], timeout: float, interval: float = 3) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        result = check()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"Timed out after {timeout:.0f}s waiting for {label}.")
        time.sleep(interval)


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered) - 1, int(fraction * len(ordered)))], 2)


def write_evidence(suite: str, test_id: str, evidence: dict[str, Any]) -> Path:
    folder = repository_root() / "testing" / "results" / suite
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{test_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    print(f"Evidence: {path}")
    return path


def metadata(test_id: str, target: Target, started: datetime) -> dict[str, Any]:
    try:
        commit = str(git("rev-parse", "HEAD")).strip()
    except (OSError, subprocess.CalledProcessError):
        commit = "unknown"
    return {"test_id": test_id, "target": target.name, "started_at": started.isoformat(timespec="seconds"),
            "finished_at": now().isoformat(timespec="seconds"), "git_commit": commit}


def finish(suite: str, test_id: str, target: Target, started: datetime, checks: dict[str, bool], details: dict[str, Any]) -> None:
    passed = all(checks.values())
    write_evidence(suite, test_id, metadata(test_id, target, started) | {"result": "PASS" if passed else "FAIL",
                                                                       "checks": checks} | details)
    for name, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    print(f"{test_id} {'PASS' if passed else 'FAIL'}")
    sys.exit(0 if passed else 1)


__all__ = ["HttpResult", "LogFollower", "Target", "assert_status", "bearer", "failed_messages", "finish", "hazards", "now", "parse_time",
           "parser", "payload_json", "percentile", "request", "wait_for", "write_evidence"]
