from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from acceptance import run_live, run_scenario


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="P1 live schema evolution without container restart")
    parser.add_argument("--deadline", type=float, default=45, help="Maximum seconds for each eventual observation")
    arguments = parser.parse_args()
    if not math.isfinite(arguments.deadline) or arguments.deadline <= 0:
        parser.error("--deadline must be finite and positive")
    raise SystemExit(run_scenario("T02", lambda evidence: run_live(evidence, arguments.deadline)))
