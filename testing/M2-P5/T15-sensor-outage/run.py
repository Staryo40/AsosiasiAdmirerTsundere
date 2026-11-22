# T15: while Sensor Pantai is down the breaker stops the polling, clients get fast answers,
# BMKG keeps its M1 latency, and coastal polling resumes on its own once the sensor is back.
from __future__ import annotations

import sys
import threading
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, bearer, failed_messages, finish, now, parser, percentile, request, wait_for  # noqa: E402

SENSOR = "sensor-pantai"
# The breaker skips the call entirely for these; anything else reached (or tried to reach) the sensor.
SKIPPED = ("circuit_open", "bulkhead_rejected")
BMKG_P95_MS = 300
STATUS_P95_MS = 1000


class Sampler(threading.Thread):
    def __init__(self, target: Target, token: str, interval: float):
        super().__init__(daemon=True)
        self.target, self.headers, self.interval = target, bearer(token), interval
        self.phase, self.samples, self.running = "baseline", [], True

    def run(self) -> None:
        while self.running:
            status = request("GET", f"{self.target.api}/v1/coastal/status", headers=self.headers, timeout=10)
            bmkg = request("GET", f"{self.target.api}/v1/hazards?{urlencode({'source': 'BMKG', 'limit': 20})}",
                           headers=self.headers, timeout=10)
            body = status.json() if status.status in (200, 503) and status.body.startswith("{") else {}
            self.samples.append({"phase": self.phase, "at": now().isoformat(), "status_code": status.status,
                                 "status_ms": status.duration_ms, "breaker": body.get("breaker_state"),
                                 "coastal_status": body.get("status"), "bmkg_code": bmkg.status, "bmkg_ms": bmkg.duration_ms})
            time.sleep(self.interval)


def coastal_polls(target: Target, since) -> list[dict]:
    return [record for record in target.logs("aggregator", since)
            if record.get("event") == "agency_poll" and record.get("endpoint") == "coastal" and record["_time"]]


def phase_stats(samples: list[dict], polls: list[dict], start, end) -> dict:
    minutes = max((end - start).total_seconds() / 60, 1 / 60)
    inside = [poll for poll in polls if start <= poll["_time"] < end]
    outcomes = Counter(poll.get("category") or "ok" for poll in inside)
    attempts = sum(count for outcome, count in outcomes.items() if outcome not in SKIPPED)
    return {"minutes": round(minutes, 2), "sensor_attempts": attempts, "attempts_per_minute": round(attempts / minutes, 1),
            "poll_outcomes": dict(outcomes),
            "status_p95_ms": percentile([s["status_ms"] for s in samples], 0.95),
            "status_codes": dict(Counter(s["status_code"] for s in samples)),
            "breaker_states": dict(Counter(s["breaker"] for s in samples)),
            "bmkg_p95_ms": percentile([s["bmkg_ms"] for s in samples if s["bmkg_code"] == 200], 0.95),
            "bmkg_codes": dict(Counter(s["bmkg_code"] for s in samples))}


def main() -> None:
    options = parser("T15 Sensor Pantai outage: breaker, fast answers, other sources, self-recovery")
    options.add_argument("--baseline", type=int, default=60, help="seconds of normal running before the outage")
    options.add_argument("--outage", type=int, default=180, help="seconds the sensor stays down")
    options.add_argument("--recovery", type=int, default=180, help="seconds allowed for polling to resume on its own")
    options.add_argument("--interval", type=float, default=2)
    args = options.parse_args()
    target, started = Target(args.target), now()
    target.require_k8s()
    sampler = Sampler(target, target.token("FIELD"), args.interval)
    sampler.start()

    time.sleep(args.baseline)
    outage_at = now()
    sampler.phase = "outage"
    target.kubectl("scale", "deployment", SENSOR, "--replicas=0")
    print(f"Sensor Pantai scaled to 0; watching for {args.outage} s.")
    time.sleep(args.outage)

    restored_at = now()
    sampler.phase = "recovery"
    target.kubectl("scale", "deployment", SENSOR, "--replicas=1")
    print("Sensor Pantai scaled back to 1; nothing else is touched from here on.")

    def first_success():
        return next((poll for poll in coastal_polls(target, restored_at) if not poll.get("category")), None)
    resumed = wait_for("a successful coastal poll", first_success, args.recovery, 5)
    time.sleep(30)
    sampler.running = False
    sampler.join(timeout=30)
    ended = now()

    polls = coastal_polls(target, started)
    by_phase = {phase: [s for s in sampler.samples if s["phase"] == phase] for phase in ("baseline", "outage", "recovery")}
    stats = {"baseline": phase_stats(by_phase["baseline"], polls, started, outage_at),
             "outage": phase_stats(by_phase["outage"], polls, outage_at, restored_at),
             "recovery": phase_stats(by_phase["recovery"], polls, restored_at, ended)}
    rejected = failed_messages(target, target.token("OPS"), "SENSOR_PANTAI")
    reasons = Counter(item.get("reason") or "(none)" for item in rejected)
    outage, baseline = stats["outage"], stats["baseline"]

    checks = {
        "breaker_opened_during_outage": outage["breaker_states"].get("open", 0) > 0,
        "sensor_attempts_did_not_rise": outage["attempts_per_minute"] <= max(baseline["attempts_per_minute"], 1),
        "coastal_status_answered_fast": outage["status_p95_ms"] is not None and outage["status_p95_ms"] <= STATUS_P95_MS
                                        and all(code in (200, 503) for code in outage["status_codes"]),
        "bmkg_kept_m1_latency": outage["bmkg_p95_ms"] is not None and outage["bmkg_p95_ms"] < BMKG_P95_MS
                                and set(outage["bmkg_codes"]) == {200},
        "polling_resumed_without_intervention": bool(resumed),
        "rejected_records_have_reasons": "(none)" not in reasons,
    }
    finish("M2-P5", "T15", target, started, checks, {
        "phases": stats, "outage": {"from": outage_at.isoformat(), "to": restored_at.isoformat()},
        "first_success_after_restore_s": round((resumed["_time"] - restored_at).total_seconds(), 1) if resumed else None,
        "limits": {"bmkg_p95_ms": BMKG_P95_MS, "status_p95_ms": STATUS_P95_MS},
        "rejected_sensor_records": len(rejected), "rejection_reasons": dict(reasons),
        "samples": sampler.samples,
        "note": "Restoring the sensor is the end of the fault, not an intervention in BNPB; nothing in BNPB is "
                "touched. BMKG latency is checked against the M1 limit of p95 < 300 ms.",
    })


if __name__ == "__main__":
    main()
