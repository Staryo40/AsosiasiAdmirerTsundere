from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from testlib import *  # noqa: E402,F403

SUMMARY = {"hazard_id", "source", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"}
FULL = SUMMARY | {"source_ref_id", "latitude", "longitude", "attributes"}


def main() -> None:
    started = timestamp(); root = repository_root(); api = os.getenv("API_BASE", "http://localhost:8080")
    env = load_dotenv(root / "bnpb/.env"); cases = []
    tokens = {}
    for role in ("MEDIA", "FIELD", "OPS"):
        tokens[role] = bnpb_login(api, require_value(env, f"BNPB_{role}_CLIENT_ID"), require_value(env, f"BNPB_{role}_CLIENT_SECRET"))["access_token"]

    response = request("GET", f"{api}/v1/hazards?limit=5&view=summary", headers=bearer(tokens["MEDIA"]))
    assert_status(response, 200, "Media summary"); items = response.json()["items"]
    if not items: raise AssertionError("Media summary returned no hazards; wait for ingestion.")
    assert_fields(items[0].keys(), SUMMARY, "Media summary")
    cases.append({"case": "media_summary", "status": response.status, "fields": sorted(items[0])})
    response = request("GET", f"{api}/v1/hazards?limit=5&view=full", headers=bearer(tokens["MEDIA"]))
    assert_status(response, 403, "Media full view"); cases.append({"case": "media_full", "status": response.status})
    for role in ("FIELD", "OPS"):
        response = request("GET", f"{api}/v1/hazards?limit=5&view=full", headers=bearer(tokens[role]))
        assert_status(response, 200, f"{role} full view"); items = response.json()["items"]
        if not items: raise AssertionError(f"{role} full view returned no hazards.")
        assert_fields(items[0].keys(), FULL, f"{role} full view")
        cases.append({"case": f"{role.lower()}_full", "status": response.status, "fields": sorted(items[0])})
    response = request("GET", f"{api}/v1/hazards?limit=1")
    assert_status(response, 401, "Missing token"); cases.append({"case": "missing_token", "status": response.status})
    write_evidence("T07", metadata("T07", started) | {"result": "PASS", "cases": cases})
    print("T07 PASS: server-side field authorization is enforced.")


if __name__ == "__main__":
    main()
