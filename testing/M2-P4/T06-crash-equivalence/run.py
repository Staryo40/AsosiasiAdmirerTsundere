# T06: kill the satellite leader mid-stream; afterwards its fires equal an uninterrupted replay, and the Aggregator agrees.
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, hazards, now, parser, wait_for  # noqa: E402


def aggregator_fires(target: Target, token: str) -> tuple[dict[str, str], list[str]]:
    fires, duplicates = {}, []
    for item in hazards(target, token, "SATELIT"):
        if item["source_ref_id"] in fires:
            duplicates.append(item["source_ref_id"])
        fires[item["source_ref_id"]] = ",".join(sorted(item["attributes"].get("detection_keys", [])))
    return fires, duplicates


def main() -> None:
    options = parser("T06 crash equivalence for the Feed Satelit processor")
    options.add_argument("--warmup", type=int, default=60, help="seconds of streaming before the kill")
    options.add_argument("--timeout", type=int, default=600, help="seconds to wait for catch-up and delivery")
    args = options.parse_args()
    target, started = Target(args.target), now()

    before_leader = target.satellite_leader()
    wait_for("the leader to publish fires", lambda: target.satellite_verify(before_leader)["published_active"] > 0, args.warmup + 120, 10)
    time.sleep(args.warmup)
    before = target.satellite_verify(before_leader)
    killed_at = now()
    target.kill("satellite", pod=before_leader if target.name == "k8s" else None)
    print(f"Killed {before_leader} at offset {before['next_offset']}; waiting for recovery.")

    def recovered():
        try:
            leader = target.satellite_leader()
            report = target.satellite_verify(leader)
        except (AssertionError, ValueError):
            return None
        caught_up = report["next_offset"] > before["next_offset"] and report["undelivered_changes"] == 0
        return (leader, report) if caught_up else None
    after_leader, after = wait_for("the new leader to pass the kill offset and drain its outbox", recovered, args.timeout, 10)

    def published(report: dict) -> dict[str, str]:
        return {fire["id"]: ",".join(fire["detection_keys"]) for fire in report["fires"]}

    stored, duplicates, missing, extra, differing = {}, [], [], [], []
    for _ in range(6):
        # Listing every fire takes a while on a live feed; a fire only counts as wrong if the
        # Aggregator disagrees with the satellite both before and after the listing.
        first = published(target.satellite_verify(after_leader))
        stored, duplicates = aggregator_fires(target, target.token("OPS"))
        after = target.satellite_verify(after_leader)
        second = published(after)
        missing = sorted(fire for fire in set(first) & set(second) if fire not in stored)
        extra = sorted(set(stored) - set(first) - set(second))
        differing = sorted(fire for fire in stored if fire in first or fire in second
                           if stored[fire] not in (first.get(fire), second.get(fire)))
        if not (missing or extra or differing):
            break
        time.sleep(10)

    checks = {
        "published_equals_uninterrupted_replay": after["published_equals_rebuild"],
        "published_equals_batch_clustering": after["published_equals_batch"],
        "aggregator_has_no_missing_fire": not missing,
        "aggregator_has_no_extra_fire": not extra,
        "aggregator_fires_match": not differing,
        "aggregator_has_no_duplicate_fire": not duplicates,
    }
    finish("M2-P4", "T06", target, started, checks, {
        "killed": {"pod": before_leader, "at": killed_at.isoformat(), "next_offset": before["next_offset"],
                   "published_active": before["published_active"]},
        "recovered": {"pod": after_leader, "next_offset": after["next_offset"], "published_active": after["published_active"],
                      "rebuilt_active": after["rebuilt_active"], "batch_fires": after["batch_fires"], "stats": after["stats"],
                      "only_published": (after["only_published"] or [])[:20], "only_rebuilt": (after["only_rebuilt"] or [])[:20]},
        "aggregator": {"active_satelit_hazards": len(stored), "missing": missing[:20], "extra": extra[:20],
                       "differing": differing[:20], "duplicates": duplicates[:20]},
    })


if __name__ == "__main__":
    main()
