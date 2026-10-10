#!/usr/bin/env python3
"""
Usage (run from root):
    python3 testing/P05/p5_demo.py | tee testing/P05/evidence-$(date +%Y%m%d-%H%M%S).log

Options:
    --project NAME        docker compose project name (default: aat)
    --wait-baseline SECS  how long to wait for organic traffic before the
                           P5.1/P5.2 check (default: 20)
    --wait-down SECS      how long consumer-2 stays stopped for P5.3
                           (default: 30)
    --wait-p54 SECS       how long to wait for the next event after
                           consumer-3 joins, for P5.4 (default: 15)
"""
import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

MGMT_URL = "http://127.0.0.1:15672/api"

def load_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def mgmt_get(path, user, password, timeout=10):
    request = urllib.request.Request(MGMT_URL + path)
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {auth}")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def container_stats(project, service, port, path="/stats", timeout=30):
    result = subprocess.run(
        ["docker", "run", "--rm", "--network", f"{project}_events",
         "curlimages/curl", "-s", f"http://{service}:{port}{path}"],
        capture_output=True, text=True, timeout=timeout,
    )
    if not result.stdout.strip():
        raise RuntimeError(f"no response from {service}:{port}{path} (stderr: {result.stderr.strip()})")
    return json.loads(result.stdout)


def container_name(project, service):
    return f"{project}-{service}-1"


def docker_logs_tail(name, n=5):
    result = subprocess.run(["docker", "logs", name, "--tail", str(n)], capture_output=True, text=True)
    return (result.stdout + result.stderr).strip()


def started_at(name):
    result = subprocess.run(
        ["docker", "inspect", name, "--format", "{{.State.StartedAt}}"],
        capture_output=True, text=True,
    )
    return result.stdout.strip()


def compose(args, project_dir, project):
    subprocess.run(["docker", "compose", "-p", project, *args], cwd=project_dir, check=True)


def section(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def check(label, condition):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}")
    return condition


def show(label, obj):
    """Pretty-print a dict as indented JSON, screenshot-ready."""
    print(f"{label}:")
    print(json.dumps(obj, indent=4))


def show_logs(label, raw):
    """Pretty-print each JSON log line on its own indented block."""
    print(f"{label}:")
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            print(json.dumps(json.loads(line), indent=4))
        except json.JSONDecodeError:
            print(line)


def wait_for_catchup(project, user, password, expected_seen, timeout=30):
    """Wait for processing and sampled broker metrics to reflect recovery."""
    deadline = time.monotonic() + timeout
    stats, queue = {}, {}
    print(f"Waiting up to {timeout}s for messages_seen >= {expected_seen} and an empty queue...")
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            queue = mgmt_get("/queues/%2F/hazard.events.notifications", user, password,
                             timeout=min(10, remaining))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            stats = container_stats(project, "consumer-2", 8086, timeout=remaining)
            print("P5.3 recovery: " + json.dumps({
                "messages_seen": stats.get("messages_seen"),
                **{key: queue.get(key) for key in
                   ("messages", "messages_ready", "messages_unacknowledged", "consumers")},
            }))
            if (time.monotonic() <= deadline
                    and stats.get("messages_seen", -1) >= expected_seen
                    and (queue.get("consumers") or 0) >= 1
                    and all(queue.get(key) == 0 for key in
                            ("messages", "messages_ready", "messages_unacknowledged"))):
                return stats, queue, True
        except (urllib.error.URLError, TimeoutError, subprocess.TimeoutExpired, RuntimeError) as error:
            print(f"P5.3 recovery observation unavailable: {error}")
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    print(f"Timed out waiting for P5.3 recovery after {timeout}s (expected messages_seen >= {expected_seen}).")
    return stats, queue, False


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", default="aat")
    parser.add_argument("--wait-baseline", type=int, default=20)
    parser.add_argument("--wait-down", type=int, default=30)
    parser.add_argument("--wait-p54", type=int, default=15)
    parser.add_argument("--repo-dir", default=".")
    args = parser.parse_args()

    project = args.project
    repo_dir = args.repo_dir
    env = load_env(f"{repo_dir}/bnpb/.env")
    admin_user = env["RABBITMQ_DEFAULT_USER"]
    admin_pass = env["RABBITMQ_DEFAULT_PASS"]

    results = []

    section("PREFLIGHT")
    subprocess.run(["docker", "compose", "-p", project, "ps"], cwd=repo_dir)

    section("P5.1 / P5.2 -- fanout to >=2 independent consumers (organic traffic)")
    c1_before = container_stats(project, "consumer-1", 8085)
    c2_before = container_stats(project, "consumer-2", 8086)
    show("consumer-1 /stats (before)", c1_before)
    show("consumer-2 /stats (before)", c2_before)
    print(f"\nWaiting {args.wait_baseline}s for organic BMKG/PVMBG traffic through the Aggregator...")
    time.sleep(args.wait_baseline)

    c1_after = container_stats(project, "consumer-1", 8085)
    c2_after = container_stats(project, "consumer-2", 8086)
    show("consumer-1 /stats (after)", c1_after)
    show("consumer-2 /stats (after)", c2_after)
    results.append(check(
        "P5.2: consumer-1 advanced on organic traffic",
        c1_after["messages_seen"] > c1_before["messages_seen"]))
    results.append(check(
        "P5.2: consumer-2 advanced on organic traffic, independently",
        c2_after["messages_seen"] > c2_before["messages_seen"]))

    print()
    show_logs("Recent consumer-1 log lines", docker_logs_tail(container_name(project, "consumer-1"), 3))
    print()
    show_logs("Recent consumer-2 log lines", docker_logs_tail(container_name(project, "consumer-2"), 3))

    section("P5.3 -- stop consumer-2 (notifier)")
    c2_before_stop = container_stats(project, "consumer-2", 8086)
    show("consumer-2 /stats immediately before stop", c2_before_stop)
    compose(["stop", "consumer-2"], repo_dir, project)
    print(f"consumer-2 stopped. Waiting {args.wait_down}s for organic traffic to accumulate while it's down...")
    time.sleep(args.wait_down)

    c1_during = container_stats(project, "consumer-1", 8085)
    print()
    show("consumer-1 /stats while consumer-2 is down (must keep advancing)", c1_during)
    results.append(check(
        "P5.3: producer/other consumer unaffected by the down consumer",
        c1_during["messages_seen"] > c1_after["messages_seen"]))

    queue_during = mgmt_get("/queues/%2F/hazard.events.notifications", admin_user, admin_pass)
    depth_during = {
        "messages_ready": queue_during.get("messages_ready"),
        "messages": queue_during.get("messages"),
        "consumers": queue_during.get("consumers"),
    }
    show("hazard.events.notifications while down", depth_during)
    results.append(check(
        "P5.3: backlog accumulated in the queue while the consumer was down",
        depth_during["messages"] and depth_during["messages"] > 0 and depth_during["consumers"] == 0))

    section("P5.3 -- restart consumer-2, show catch-up")
    compose(["start", "consumer-2"], repo_dir, project)
    expected_seen = c2_before_stop["messages_seen"] + max(depth_during["messages"] or 0, 1)
    c2_after_restart, queue_after, recovered = wait_for_catchup(
        project, admin_user, admin_pass, expected_seen)
    show("consumer-2 /stats after restart", c2_after_restart)
    results.append(check(
        "P5.3: downtime backlog was caught up, not lost",
        c2_after_restart.get("messages_seen", -1) >= expected_seen))

    print()
    show_logs("Drained backlog log lines", docker_logs_tail(container_name(project, "consumer-2"), max(depth_during["messages"] or 5, 5) + 2))

    print()
    show("hazard.events.notifications after catch-up", {
        key: queue_after.get(key) for key in
        ("messages", "messages_ready", "messages_unacknowledged", "consumers")})
    results.append(check(
        "P5.3: queue fully drained after catch-up",
        recovered))

    section("P5.4 -- add consumer-3 live, no restart of 1/2, no producer change")
    c1_name, c2_name = container_name(project, "consumer-1"), container_name(project, "consumer-2")
    before_c1, before_c2 = started_at(c1_name), started_at(c2_name)
    print("consumer-1 StartedAt (before):", before_c1)
    print("consumer-2 StartedAt (before):", before_c2)

    compose(["--profile", "demo", "up", "-d", "--build", "consumer-3"], repo_dir, project)
    time.sleep(3)

    after_c1, after_c2 = started_at(c1_name), started_at(c2_name)
    results.append(check(
        "P5.4: consumer-1 was never restarted to make room for consumer-3",
        before_c1 == after_c1))
    results.append(check(
        "P5.4: consumer-2 was never restarted to make room for consumer-3",
        before_c2 == after_c2))

    print(f"\nWaiting {args.wait_p54}s for the next organic event to reach all three...")
    time.sleep(args.wait_p54)

    c1_final = container_stats(project, "consumer-1", 8085)
    c2_final = container_stats(project, "consumer-2", 8086)
    c3_final = container_stats(project, "consumer-3", 8087)
    show("consumer-1 /stats", c1_final)
    show("consumer-2 /stats", c2_final)
    show("consumer-3 /stats", c3_final)
    results.append(check(
        "P5.4: the new consumer received at least one event with zero producer changes",
        c3_final["messages_seen"] > 0))

    section("SUMMARY")
    passed = sum(1 for r in results if r)
    print(f"{passed}/{len(results)} checks passed.")
    if passed != len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
