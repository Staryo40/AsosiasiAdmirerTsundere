# T05: every field report answered with 202 is stored by field-intake exactly once, even when components die.
from __future__ import annotations

import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, now, parser, percentile, request, wait_for  # noqa: E402

MAX_ATTEMPTS = 40


class FieldClient:
    def __init__(self, target: Target):
        self.target, self.lock, self.token = target, threading.Lock(), target.token("FIELD")

    def refresh(self, stale: str) -> None:
        with self.lock:
            if self.token == stale:
                self.token = self.target.token("FIELD")

    def submit(self, body: dict) -> dict:
        attempts, started = 0, time.perf_counter()
        while attempts < MAX_ATTEMPTS:
            attempts += 1
            token = self.token
            try:
                response = request("POST", f"{self.target.api}/v1/field-reports", headers={"Authorization": f"Bearer {token}"},
                                   body=body, timeout=10)
            except AssertionError:
                time.sleep(1)
                continue
            if response.status == 202:
                return {"report_id": body["report_id"], "status": 202, "attempts": attempts,
                        "latency_ms": response.duration_ms, "total_ms": round((time.perf_counter() - started) * 1000, 1)}
            if response.status == 401:
                self.refresh(token)
            elif response.status in (502, 503, 504):
                time.sleep(2)
            else:
                return {"report_id": body["report_id"], "status": response.status, "attempts": attempts}
        return {"report_id": body["report_id"], "status": "gave_up", "attempts": attempts}


def stored_reports(target: Target, prefix: str) -> list[dict]:
    key = {"X-BNPB-Internal-Key": target.value("BNPB_FIELD_INTAKE_INTERNAL_KEY")}
    items, after = [], 0
    while True:
        response = request("GET", f"{target.intake}/internal/field-reports?{urlencode({'after': after, 'limit': 1000})}", headers=key)
        if response.status != 200:
            raise AssertionError(f"field-intake listing answered HTTP {response.status}.")
        page = response.json()
        items.extend(item for item in page["items"] if item["report_id"].startswith(prefix))
        if page["next_after"] is None:
            return items
        after = page["next_after"]


def main() -> None:
    options = parser("T05 field reports: no 202 lost, none stored twice")
    options.add_argument("--count", type=int, default=500)
    options.add_argument("--concurrency", type=int, default=32)
    options.add_argument("--resend", type=int, default=50, help="reports sent a second time with identical bodies")
    options.add_argument("--stop-intake", action="store_true", help="keep field-intake stopped while the reports are sent")
    options.add_argument("--kill", choices=("api", "field-intake"), help="kill this component halfway through")
    args = options.parse_args()
    target, started = Target(args.target), now()
    run_id = started.strftime("%Y%m%d%H%M%S")
    prefix = f"m2t05-{run_id}-"
    bodies = [{"report_id": f"{prefix}{index:05d}", "reported_at": started.isoformat(timespec="seconds"),
               "latitude": round(random.uniform(-10, 5), 5), "longitude": round(random.uniform(95, 140), 5),
               "hazard_type": random.choice(["SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL", "OTHER"]),
               "description": f"T05 laporan uji {index}", "related_hazard_id": None} for index in range(args.count)]
    client = FieldClient(target)
    events = []
    if args.stop_intake:
        target.stop("field-intake")
        events.append({"at": now().isoformat(), "event": "field-intake stopped"})
    try:
        half = threading.Event()
        def send(body: dict, position: int) -> dict:
            if position == args.count // 2:
                half.set()
            return client.submit(body)
        with ThreadPoolExecutor(args.concurrency) as pool:
            futures = [pool.submit(send, body, position) for position, body in enumerate(bodies)]
            if args.kill:
                half.wait()
                target.kill(args.kill)
                events.append({"at": now().isoformat(), "event": f"{args.kill} killed"})
            results = [future.result() for future in futures]
        resent = [client.submit(body) for body in bodies[:args.resend]]
    finally:
        if args.stop_intake:
            target.start("field-intake")
            events.append({"at": now().isoformat(), "event": "field-intake started"})

    accepted = {result["report_id"] for result in results if result["status"] == 202}
    refused = [result for result in results if result["status"] != 202]
    def all_stored():
        try:
            stored = stored_reports(target, prefix)
        except AssertionError:
            return None
        return stored if accepted <= {item["report_id"] for item in stored} else None
    try:
        stored = wait_for("field-intake to store every accepted report", all_stored, timeout=180, interval=5)
    except AssertionError:
        stored = stored_reports(target, prefix)
    counts: dict[str, int] = {}
    for item in stored:
        counts[item["report_id"]] = counts.get(item["report_id"], 0) + 1
    lost = sorted(accepted - set(counts))
    doubled = sorted(report for report, count in counts.items() if count > 1)
    unexpected = sorted(set(counts) - accepted)
    redelivered = sum(1 for item in stored if item["deliveries"] > 1)
    latencies = [result["latency_ms"] for result in results if result["status"] == 202]
    checks = {
        "every_report_accepted": not refused,
        "no_accepted_report_lost": not lost,
        "no_report_stored_twice": not doubled,
        "no_unaccepted_report_stored": not unexpected,
        "resent_reports_accepted_again": all(result["status"] == 202 for result in resent),
    }
    finish("M2-P3", "T05", target, started, checks, {
        "parameters": vars(args), "events": events,
        "sent": len(bodies), "accepted_202": len(accepted), "refused": refused[:20],
        "retried_before_202": sum(1 for result in results if result["status"] == 202 and result["attempts"] > 1),
        "latency_202_ms": {"p50": percentile(latencies, 0.5), "p95": percentile(latencies, 0.95), "max": max(latencies, default=None)},
        "stored_rows": len(stored), "lost": lost[:50], "stored_twice": doubled[:50], "unexpected": unexpected[:50],
        "rows_with_redeliveries": redelivered, "resent": len(resent),
    })


if __name__ == "__main__":
    main()
