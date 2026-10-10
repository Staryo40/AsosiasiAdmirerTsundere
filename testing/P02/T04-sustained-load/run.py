import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from p2lib import run

if __name__ == "__main__":
    raise SystemExit(run("T04"))
