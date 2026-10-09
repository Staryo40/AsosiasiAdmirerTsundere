from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from testlib import *  # noqa: E402,F403


def main() -> None:
    started = timestamp()
    root = repository_root()
    bmkg_base = os.getenv("BMKG_BASE", "http://localhost:8081")
    pvmbg_base = os.getenv("PVMBG_BASE", "http://localhost:8082")
    bmkg = load_dotenv(root / "bmkg/.env")
    pvmbg = load_dotenv(root / "pvmbg/.env")
    bmkg_key = require_value(bmkg, "BMKG_API_KEY")

    cases = []
    response = request("GET", f"{bmkg_base}/seismic-events", headers={"X-BMKG-Key": bmkg_key})
    assert_status(response, 200, "BMKG key on BMKG")
    cases.append({"case": "bmkg_key_to_bmkg", "status": response.status, "duration_ms": response.duration_ms})
    response = request("GET", f"{pvmbg_base}/volcanic-reports", headers=bearer(bmkg_key))
    assert_status(response, (401, 403), "BMKG key on PVMBG")
    cases.append({"case": "bmkg_key_to_pvmbg", "status": response.status, "duration_ms": response.duration_ms})

    pvmbg_session = pvmbg_login(pvmbg_base, require_value(pvmbg, "PVMBG_READER_CLIENT_ID"), require_value(pvmbg, "PVMBG_READER_CLIENT_SECRET"))
    token = pvmbg_session["access_token"]
    response = request("GET", f"{pvmbg_base}/volcanic-reports", headers=bearer(token))
    assert_status(response, 200, "PVMBG token on PVMBG")
    cases.append({"case": "pvmbg_token_to_pvmbg", "status": response.status, "duration_ms": response.duration_ms})
    response = request("GET", f"{bmkg_base}/seismic-events", headers={"X-BMKG-Key": token})
    assert_status(response, (401, 403), "PVMBG token on BMKG")
    cases.append({"case": "pvmbg_token_to_bmkg", "status": response.status, "duration_ms": response.duration_ms})

    evidence = metadata("T06", started) | {"result": "PASS", "cases": cases}
    write_evidence("T06", evidence)
    print("T06 PASS: cross-domain credentials are rejected.")


if __name__ == "__main__":
    main()
