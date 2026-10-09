from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from testlib import *  # noqa: E402,F403

ENV_FILES = ("bmkg/.env", "pvmbg/.env", "bnpb/.env")
EXAMPLES = ("bmkg/.env.example", "pvmbg/.env.example", "bnpb/.env.example")
SECRET_NAMES = {
    "BMKG_API_KEY", "PVMBG_JWT_SECRET", "PVMBG_READER_CLIENT_SECRET", "PVMBG_ADMIN_CLIENT_SECRET",
    "POSTGRES_PASSWORD", "RABBITMQ_DEFAULT_PASS", "BNPB_AUTH_INTERNAL_KEY", "BNPB_AGGREGATOR_INTERNAL_KEY",
    "BNPB_MEDIA_CLIENT_SECRET", "BNPB_FIELD_CLIENT_SECRET", "BNPB_OPS_CLIENT_SECRET",
    "BNPB_RABBITMQ_PUBLISHER_PASSWORD", "BNPB_CONSUMER_1_RABBITMQ_PASSWORD",
    "BNPB_CONSUMER_2_RABBITMQ_PASSWORD", "BNPB_CONSUMER_3_RABBITMQ_PASSWORD",
}


def main() -> None:
    started = timestamp(); root = repository_root()
    tracked = str(git("ls-files", "--", *ENV_FILES)).splitlines()
    if tracked: raise AssertionError(f"Active .env files are tracked: {tracked}")
    for filename in ENV_FILES:
        result = subprocess.run(["git", "-C", str(root), "check-ignore", "-q", "--", filename], check=False)
        if result.returncode != 0: raise AssertionError(f"{filename} is not ignored by Git.")

    secrets: set[str] = set(); coverage = []
    for env_name, example_name in zip(ENV_FILES, EXAMPLES):
        active = load_dotenv(root / env_name); example = load_dotenv(root / example_name)
        missing = sorted(set(active) - set(example))
        if missing: raise AssertionError(f"{example_name} is missing variables: {missing}")
        coverage.append({"file": example_name, "active_variable_count": len(active), "missing_variables": []})
        secrets.update(value for name, value in active.items() if name in SECRET_NAMES and len(value) >= 8 and not value.startswith("<"))
    if not secrets: raise AssertionError("No active secrets were available for repository scanning.")

    history = git("log", "--all", "-p", "--no-ext-diff", "--", ".", text=False)
    matches = sum(secret.encode("utf-8") in history for secret in secrets)
    if matches: raise AssertionError("One or more active secret values appear in Git history.")
    evidence = metadata("T09", started) | {
        "result": "PASS", "active_env_files_tracked": 0, "ignored_env_files": list(ENV_FILES),
        "example_coverage": coverage, "active_secret_values_scanned": len(secrets),
        "active_secret_matches_in_git_history": 0,
        "note": "Secret values and Git history content were kept in memory and were not written to evidence.",
    }
    write_evidence("T09", evidence); print("T09 PASS: env files are ignored, examples are complete, and active secrets were not found in Git history.")


if __name__ == "__main__":
    main()
