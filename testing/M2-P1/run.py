"""Run the local P1 checks. Test runner drafted with OpenAI Codex assistance."""

import argparse
import os
from pathlib import Path
import subprocess
import sys


TESTING = Path(__file__).resolve().parents[1]
BNPB = TESTING.parent / "bnpb"
CATALOGUE = "internal/placementcompare/testdata"
UNIT_TESTS = (
    "^Test(M2ShardGoldenVectors|M2WarningColocation|M2PlacementAdmissionCountsTransientCopies|"
    "Audit.*|Catalog.*|Analytics.*|Repair.*|PatroniRepair.*|VerifyRepairEvidence.*)$"
)
DATABASE_TESTS = (
    "^Test(M2ShardStoresRouteAndEnforceIdentity|RoutedWriteIntegrationWarningAndDataStayOnOwningShard|"
    "CatalogLoadCLIThroughRoutedAggregator|AuditCrossLogicalShardPagesAndChecksum|"
    "AuditLocationChecksLiveRoutedIdentityAndPlacement|GlobalReadIntegration.*|"
    "RepairReservationResumeAndCompletionRejectsPlacementABA|RepairResolvesReversedPlacementRoles|"
    "RepairAdmissionRejectsFullNode|AnalyticsMatchesAuditActiveEventsAndMarksMissingShard|"
    "StartupReadIntegrationRecoversUnavailableShardWithoutRestart)$"
)


def run(label, command, directory):
    print(f"\n=== {label} ===", flush=True)
    try:
        result = subprocess.run(command, cwd=directory, timeout=300, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"FAIL {label} ({error})", flush=True)
        return False
    print(f"{'PASS' if result.returncode == 0 else 'FAIL'} {label}", flush=True)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", action="store_true", help="use a disposable PostgreSQL primary with a synchronous standby")
    parser.add_argument("--replication", action="store_true", help="start and remove the disposable PostgreSQL pair")
    args = parser.parse_args()
    if args.database and not os.environ.get("BNPB_M2_LOCAL_TEST_DATABASE_URL"):
        parser.error("--database requires BNPB_M2_LOCAL_TEST_DATABASE_URL")
    if args.replication and not os.environ.get("BNPB_TEST_POSTGRES_BIN"):
        parser.error("--replication requires BNPB_TEST_POSTGRES_BIN")

    test = ["go", "test", "-v", "-count=1", "-timeout=180s"]
    aggregator = "./internal/aggregator"
    results = [
        run("Aggregator contracts", [*test, "-run", UNIT_TESTS, aggregator], BNPB),
        run("Partial API projection", [*test, "-run", "^TestPartialProjection", "./internal/api"], BNPB),
        run("Placement comparison tests", [*test, "./internal/placementcompare"], BNPB),
        run("Synthetic catalogue and regional subset", [
            "go", "run", "./cmd/placement-compare", "-input", f"{CATALOGUE}/catalog.jsonl",
            "-resources", f"{CATALOGUE}/resources.json", "-burst-area", "Kabupaten Contoh",
        ], BNPB),
        run("Evidence collector fixtures", [
            sys.executable, "-B", "-m", "unittest", "-v", "test_evidence_catalogue", "test_evidence_repair",
        ], TESTING),
        run("Dashboard data contract", ["node", str(TESTING / "P1" / "dashboard.mjs")], TESTING),
    ]
    if args.database:
        results.append(run("Local shard integration", [*test, "-tags=integration", "-run", DATABASE_TESTS, aggregator], BNPB))
    else:
        print("SKIP Local shard integration (enable --database)")
    if args.replication:
        results.append(run("Local physical replication", [
            *test, "-tags=integration", "-run", "^TestReplicatedMutationIntegration$", aggregator,
        ], BNPB))
    else:
        print("SKIP Local physical replication (enable --replication)")

    print(f"\nP1 local checks: {sum(results)} passed, {results.count(False)} failed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
