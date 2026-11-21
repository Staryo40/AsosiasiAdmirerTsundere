# T09: cut the feed connection; the satellite service resumes from its checkpoint with no gap and no duplicate.
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, now, parser, wait_for  # noqa: E402


def compose_networks(target: Target, container: str) -> list[str]:
    output = target.run("docker", "inspect", "-f", "{{range $name, $_ := .NetworkSettings.Networks}}{{$name}} {{end}}", container).stdout
    return output.split()


def main() -> None:
    options = parser("T09 feed disconnect and resume")
    options.add_argument("--outage", type=int, default=45, help="seconds without a connection")
    args = options.parse_args()
    target, started = Target(args.target), now()
    leader = target.satellite_leader()
    before = target.satellite_verify(leader)
    cut_at = now()
    if target.name == "k8s":
        # A graceful delete closes the stream; the standby takes the lease and resumes from its own checkpoint.
        standby = next((pod["metadata"]["name"] for pod in target.pods("satellite") if pod["metadata"]["name"] != leader), None)
        resume_from = target.satellite_verify(standby)["next_offset"] if standby else None
        target.kubectl("delete", "pod", leader, "--wait=false")
        method = f"deleted pod {leader}"
    else:
        container = target.run("docker", "compose", "ps", "-q", "satellite").stdout.strip()
        networks = compose_networks(target, container)
        for network in networks:
            target.run("docker", "network", "disconnect", network, container)
        try:
            time.sleep(args.outage)
        finally:
            for network in networks:
                target.run("docker", "network", "connect", network, container, check=False)
        resume_from = None
        method = f"disconnected {container[:12]} from {', '.join(networks)} for {args.outage}s"

    def reconnected():
        try:
            pod = target.satellite_leader()
        except AssertionError:
            return None
        records = target.logs("satellite", cut_at, pod=pod if target.name == "k8s" else None)
        connected = [record for record in records if record.get("event") == "feed_connected"]
        return (pod, records, connected) if connected else None
    pod, records, connected = wait_for("the feed to reconnect", reconnected, 300, 10)
    time.sleep(20)
    after = target.satellite_verify(pod)
    first_from = connected[0].get("from")
    expected_from = resume_from if resume_from is not None else before["next_offset"]
    gaps = [record for record in records if record.get("category") == "offset_gap"]
    checks = {
        "resumed_at_the_checkpoint": first_from is not None and (first_from == expected_from if resume_from is not None
                                                                 else first_from >= expected_from),
        "no_offset_gap_reported": not gaps,
        "progress_after_reconnect": after["next_offset"] > (first_from or 0),
        "fires_still_equal_replay": after["published_equals_rebuild"] and after["published_equals_batch"],
    }
    finish("M2-P4", "T09", target, started, checks, {
        "method": method, "leader_before": leader, "leader_after": pod,
        "next_offset_before": before["next_offset"], "expected_resume_offset": expected_from,
        "reconnects": [{"time": record.get("time") or record.get("timestamp"), "from": record.get("from")} for record in connected[:10]],
        "disconnects": [{"category": record.get("category"), "events": record.get("events")}
                        for record in records if record.get("event") == "feed_disconnected"][:10],
        "next_offset_after": after["next_offset"],
    })


if __name__ == "__main__":
    main()
