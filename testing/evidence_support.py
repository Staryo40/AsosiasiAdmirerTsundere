from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


class EvidenceError(ValueError):
    """An operator-facing error that must not contain input values."""


def _object(payload):
    document = json.loads(payload)
    if not isinstance(document, dict):
        raise EvidenceError("Expected a JSON object.")
    return document


def read_json(path):
    return _object(Path(path).read_text(encoding="utf-8-sig"))


def read_jsonl(path):
    with Path(path).open(encoding="utf-8-sig") as stream:
        for number, line in enumerate(stream, 1):
            if line.strip():
                try:
                    yield _object(line)
                except ValueError:
                    raise EvidenceError(f"Invalid JSON object at input line {number}.") from None


def utc(value):
    if not isinstance(value, str):
        raise EvidenceError("Expected an ISO 8601 timestamp.")
    # Python 3.9 accepts microseconds, while Go emits up to nine digits.
    value = re.sub(r"\.(\d+)(?=Z$|[+-]\d{2}:\d{2}$)", lambda match: "." + match[1][:6].ljust(6, "0"), value)
    stamp = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    if stamp.utcoffset() is None:
        raise EvidenceError("Use an ISO 8601 timestamp with an explicit UTC offset.")
    return stamp.astimezone(timezone.utc)


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def secret(path):
    with Path(path).open(encoding="utf-8-sig") as stream:
        credential = stream.read(4097).strip()
    if not credential or len(credential) > 4096 or any(ord(char) < 32 or ord(char) == 127 for char in credential):
        raise EvidenceError("Credential files must contain one non-empty line (maximum 4096 characters).")
    return credential


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_opener = build_opener(ProxyHandler({}), _NoRedirect())
_max_response_bytes = 16 << 20


def _read_payload(response, deadline):
    payload = bytearray()
    while len(payload) <= _max_response_bytes:
        if time.monotonic() >= deadline:
            raise EvidenceError("Endpoint exceeded the response deadline.")
        segment = response.read1(min(64 << 10, _max_response_bytes + 1 - len(payload)))
        if not segment:
            document = json.loads(payload)
            if not isinstance(document, (dict, list)):
                raise EvidenceError("Expected a JSON object or array from the endpoint.")
            return document
        payload.extend(segment)
    raise EvidenceError("Endpoint response exceeds the evidence size limit.")


def get_json(base, path, headers, timeout):
    origin = urlsplit(base)
    if (origin.scheme not in {"http", "https"} or not origin.hostname or origin.username is not None
            or origin.query or origin.fragment or not path.startswith("/") or path.startswith("//")):
        raise EvidenceError("Use an HTTP(S) base URL without credentials, query, or fragment.")
    request = Request(base.rstrip("/") + path, headers=headers, method="GET")
    started = time.monotonic()
    try:
        try:
            response = _opener.open(request, timeout=timeout)
        except HTTPError as status_response:
            response = status_response
        with response:
            status = response.code
            try:
                payload = _read_payload(response, started + timeout)
            except (json.JSONDecodeError, UnicodeError):
                if 200 <= status < 300:
                    raise EvidenceError("Endpoint returned invalid JSON.") from None
                payload = {}
    except (OSError, HTTPException):
        raise EvidenceError("Endpoint did not return a bounded response.") from None
    return status, payload, round((time.monotonic() - started) * 1000, 2)


def _revision():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parent.parent,
            stderr=subprocess.DEVNULL, text=True, timeout=5,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return None


def report(path, flag, started_at, checks, observations):
    passed = bool(checks) and all(checks.values())
    document = {"flag": flag, "started_at": started_at, "finished_at": now(), "git_revision": _revision(),
                "status": "pass" if passed else "not_proven", "checks": checks, "observations": observations}
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(document, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(content)
    print(f"{document['status']}: {path}")
    return 0 if passed else 1


def run(main):
    try:
        return main()
    except EvidenceError as error:
        print(f"Evidence not produced: {error}", file=sys.stderr)
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError):
        print("Evidence not produced: invalid input, unavailable dependency, or output already exists.", file=sys.stderr)
    return 1
