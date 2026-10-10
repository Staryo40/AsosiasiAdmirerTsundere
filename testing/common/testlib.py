from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class HttpResult:
    status: int
    body: str
    duration_ms: float

    def json(self) -> Any:
        return json.loads(self.body)


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_dotenv(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise AssertionError(f"Environment file not found: {path}")
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[name.strip()] = value
    return values


def require_value(environment: dict[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not value:
        raise AssertionError(f"Required environment variable is missing or empty: {name}")
    return value


def request(method: str, url: str, *, headers: dict[str, str] | None = None,
            body: Any | None = None, timeout: float = 15) -> HttpResult:
    encoded = None
    request_headers = dict(headers or {})
    if body is not None:
        encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    http_request = urllib.request.Request(url, data=encoded, headers=request_headers, method=method)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(http_request, timeout=timeout) as response:
            status = response.status
            response_body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        status = error.code
        response_body = error.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as error:
        raise AssertionError(f"Could not reach {url}: {error.reason}") from error
    return HttpResult(status, response_body, round((time.perf_counter() - started) * 1000, 2))


def assert_status(response: HttpResult, expected: int | Iterable[int], label: str) -> None:
    accepted = {expected} if isinstance(expected, int) else set(expected)
    if response.status not in accepted:
        choices = "/".join(str(value) for value in sorted(accepted))
        raise AssertionError(f"{label} expected HTTP {choices}, received HTTP {response.status}.")


def assert_fields(actual: Iterable[str], expected: Iterable[str], label: str) -> None:
    actual_set, expected_set = set(actual), set(expected)
    if actual_set != expected_set:
        raise AssertionError(
            f"{label} fields differ. Expected {sorted(expected_set)}, received {sorted(actual_set)}."
        )


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def bnpb_login(api_base: str, client_id: str, client_secret: str) -> dict[str, Any]:
    response = request("POST", f"{api_base}/auth/login", body={"client_id": client_id, "client_secret": client_secret})
    assert_status(response, 200, f"BNPB login for {client_id}")
    return response.json()


def bnpb_refresh(api_base: str, refresh_token: str) -> HttpResult:
    return request("POST", f"{api_base}/auth/refresh", body={"refresh_token": refresh_token})


def pvmbg_login(base: str, client_id: str, client_secret: str) -> dict[str, Any]:
    response = request("POST", f"{base}/auth/login", body={"client_id": client_id, "client_secret": client_secret})
    assert_status(response, 200, f"PVMBG login for {client_id}")
    return response.json()


def timestamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def results_directory(suite: str = "P03") -> Path:
    configured = os.getenv("TEST_RESULTS_ROOT")
    path = Path(configured) if configured else repository_root() / "testing" / "results" / suite / datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def git(*arguments: str, text: bool = True) -> str | bytes:
    return subprocess.check_output(["git", "-C", str(repository_root()), *arguments], text=text)


def metadata(test_id: str, started_at: str) -> dict[str, Any]:
    return {
        "test_id": test_id,
        "started_at": started_at,
        "finished_at": timestamp(),
        "git_commit": str(git("rev-parse", "HEAD")).strip(),
    }


def write_evidence(test_id: str, evidence: dict[str, Any]) -> Path:
    path = results_directory() / f"{test_id}.json"
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Evidence: {path}")
    return path


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
