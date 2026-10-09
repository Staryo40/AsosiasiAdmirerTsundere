from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from testlib import *  # noqa: E402,F403


def main() -> None:
    started = timestamp(); root = repository_root(); api = os.getenv("API_BASE", "http://localhost:8080")
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "10")); env = load_dotenv(root / "bnpb/.env")
    session = bnpb_login(api, require_value(env, "BNPB_FIELD_CLIENT_ID"), require_value(env, "BNPB_FIELD_CLIENT_SECRET"))
    ttl = int(session["expires_in"])
    if not 5 <= ttl <= 180: raise AssertionError(f"Access TTL is {ttl}s; expected 5..180s for a live demonstration.")
    old_access, old_refresh = session["access_token"], session["refresh_token"]
    old_access_hash, old_refresh_hash = sha256_text(old_access), sha256_text(old_refresh)
    timeline = []; monotonic_start = time.monotonic()

    def record(action: str, status: int) -> None:
        timeline.append({"elapsed_seconds": round(time.monotonic() - monotonic_start, 2), "action": action, "status": status})

    response = request("GET", f"{api}/v1/hazards?limit=1&view=full", headers=bearer(old_access))
    assert_status(response, 200, "Initial Field access"); record("old_access", response.status)
    deadline = ttl + 30
    while time.monotonic() - monotonic_start < deadline:
        time.sleep(poll_interval)
        response = request("GET", f"{api}/v1/hazards?limit=1&view=full", headers=bearer(old_access))
        record("active_session_probe", response.status)
        if response.status == 401: break
        assert_status(response, 200, "Active Field session before expiry")
    else: raise AssertionError(f"Old access token did not expire within {deadline}s.")

    response = bnpb_refresh(api, old_refresh); assert_status(response, 200, "Refresh after natural expiry"); record("refresh", response.status)
    new_session = response.json(); new_access, new_refresh = new_session["access_token"], new_session["refresh_token"]
    new_access_hash, new_refresh_hash = sha256_text(new_access), sha256_text(new_refresh)
    if old_access_hash == new_access_hash or old_refresh_hash == new_refresh_hash: raise AssertionError("Refresh did not rotate both tokens.")
    response = request("GET", f"{api}/v1/hazards?limit=1&view=full", headers=bearer(new_access))
    assert_status(response, 200, "New access token"); record("new_access", response.status)
    response = request("GET", f"{api}/v1/hazards?limit=1&view=full", headers=bearer(old_access))
    assert_status(response, 401, "Old access replay"); record("old_access_replay", response.status)
    response = bnpb_refresh(api, old_refresh); assert_status(response, 401, "Old refresh replay"); record("old_refresh_replay", response.status)
    evidence = metadata("T08", started) | {
        "result": "PASS", "access_ttl_seconds": ttl, "login_count": 1,
        "old_access_token_sha256": old_access_hash, "new_access_token_sha256": new_access_hash,
        "old_refresh_token_sha256": old_refresh_hash, "new_refresh_token_sha256": new_refresh_hash, "timeline": timeline,
    }
    write_evidence("T08", evidence); print("T08 PASS: natural expiry, refresh rotation, and old-token rejection are proven.")


if __name__ == "__main__":
    main()
