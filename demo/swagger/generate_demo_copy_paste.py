"""Generate a local Swagger demo sheet with credentials from service .env files."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "demo" / "swagger" / "DEMO-COPY-PASTE.md"
OUTPUT = ROOT / "demo" / "swagger" / "DEMO-COPY-PASTE.local.md"
ENV_KEYS = {
    "bmkg": ("BMKG_API_KEY",),
    "pvmbg": (
        "PVMBG_READER_CLIENT_ID",
        "PVMBG_READER_CLIENT_SECRET",
        "PVMBG_ADMIN_CLIENT_ID",
        "PVMBG_ADMIN_CLIENT_SECRET",
        "PVMBG_RESPONSE_DELAY_MIN_MS",
        "PVMBG_RESPONSE_DELAY_MAX_MS",
    ),
    "bnpb": (
        "BNPB_MEDIA_CLIENT_ID",
        "BNPB_MEDIA_CLIENT_SECRET",
        "BNPB_FIELD_CLIENT_ID",
        "BNPB_FIELD_CLIENT_SECRET",
        "BNPB_OPS_CLIENT_ID",
        "BNPB_OPS_CLIENT_SECRET",
        "BNPB_BMKG_API_KEY",
        "BNPB_PVMBG_READER_CLIENT_ID",
        "BNPB_PVMBG_READER_CLIENT_SECRET",
    ),
}
SECRET_KEYS = {
    "BMKG_API_KEY",
    "PVMBG_READER_CLIENT_SECRET",
    "PVMBG_ADMIN_CLIENT_SECRET",
    "BNPB_MEDIA_CLIENT_SECRET",
    "BNPB_FIELD_CLIENT_SECRET",
    "BNPB_OPS_CLIENT_SECRET",
    "BNPB_BMKG_API_KEY",
    "BNPB_PVMBG_READER_CLIENT_SECRET",
}
DEFAULT_SECRETS = {"change-me", "change-me-too", "change-me-to-a-long-random-string"}
SHARED_CREDENTIALS = (
    ("BMKG_API_KEY", "BNPB_BMKG_API_KEY"),
    ("PVMBG_READER_CLIENT_ID", "BNPB_PVMBG_READER_CLIENT_ID"),
    ("PVMBG_READER_CLIENT_SECRET", "BNPB_PVMBG_READER_CLIENT_SECRET"),
)
KEY_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
QUOTED_VALUE = re.compile(r"^((?:\"(?:\\.|[^\"\\])*\")|(?:'[^'\r\n]*'))(?:\s+#.*)?$")


def parse_env_value(raw_value: str, path: Path, line_number: int) -> str:
    value = raw_value.strip()
    if value.startswith(("'", '"')):
        match = QUOTED_VALUE.fullmatch(value)
        if not match:
            raise SystemExit(f"Malformed quoted value in {path.relative_to(ROOT)}:{line_number}.")
        quoted = match.group(1)
        if quoted.startswith('"'):
            try:
                return json.loads(quoted)
            except json.JSONDecodeError as error:
                raise SystemExit(f"Malformed quoted value in {path.relative_to(ROOT)}:{line_number}.") from error
        return quoted[1:-1]
    return re.split(r"\s+#", value, maxsplit=1)[0].rstrip()


def parse_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise SystemExit(f"Required file is missing: {path.relative_to(ROOT)}")
    parsed = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        key = key.strip()
        if not separator or not KEY_PATTERN.fullmatch(key):
            raise SystemExit(f"Malformed entry in {path.relative_to(ROOT)}:{line_number}.")
        parsed[key] = parse_env_value(raw_value, path, line_number)
    return parsed


def load_values() -> dict[str, str]:
    values = {}
    for service, required_keys in ENV_KEYS.items():
        env_path = ROOT / service / ".env"
        parsed = parse_env_file(env_path)
        for key in required_keys:
            value = parsed.get(key, "").strip()
            if not value:
                raise SystemExit(f"{key} is missing or empty in {env_path.relative_to(ROOT)}.")
            if key in SECRET_KEYS and value.lower() in DEFAULT_SECRETS:
                raise SystemExit(f"{key} in {env_path.relative_to(ROOT)} still has a sample secret.")
            if "\r" in value or "\n" in value:
                raise SystemExit(f"{key} in {env_path.relative_to(ROOT)} must be a single-line value.")
            values[key] = value
    return values


def validate_values(values: dict[str, str]) -> None:
    for left, right in SHARED_CREDENTIALS:
        if values[left] != values[right]:
            raise SystemExit(f"{left} and {right} must match across the service .env files.")
    for key in ("PVMBG_RESPONSE_DELAY_MIN_MS", "PVMBG_RESPONSE_DELAY_MAX_MS"):
        if not re.fullmatch(r"\d+", values[key]):
            raise SystemExit(f"{key} must be a nonnegative integer in pvmbg/.env.")
        values[key] = str(int(values[key]))
    if int(values["PVMBG_RESPONSE_DELAY_MIN_MS"]) > int(values["PVMBG_RESPONSE_DELAY_MAX_MS"]):
        raise SystemExit("PVMBG_RESPONSE_DELAY_MIN_MS must not exceed PVMBG_RESPONSE_DELAY_MAX_MS.")


def markdown_code_span(value: str) -> str:
    longest_run = max((len(run) for run in re.findall(r"`+", value)), default=0)
    fence = "`" * (longest_run + 1)
    padding = " " if value.startswith("`") or value.endswith("`") else ""
    return f"{fence}{padding}{value}{padding}{fence}"


def render_template(values: dict[str, str]) -> str:
    document = TEMPLATE.read_text(encoding="utf-8")
    document = document.replace("{{INLINE:BMKG_API_KEY}}", markdown_code_span(values["BMKG_API_KEY"]))
    for key, value in values.items():
        escaped = json.dumps(value, ensure_ascii=False)[1:-1]
        document = document.replace("{{" + key + "}}", escaped)
    unresolved = re.findall(r"\{\{[^{}]+\}\}", document)
    if unresolved:
        raise SystemExit(f"Template has unresolved placeholders: {', '.join(unresolved)}")
    return document


def write_local_copy(document: str) -> None:
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n", delete=False,
            dir=OUTPUT.parent, prefix=".DEMO-COPY-PASTE.local.", suffix=".tmp",
        ) as local_file:
            temporary_path = Path(local_file.name)
            local_file.write(document)
        os.replace(temporary_path, OUTPUT)
    except OSError:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def main() -> int:
    values = load_values()
    validate_values(values)
    write_local_copy(render_template(values))
    print("Generated demo/swagger/DEMO-COPY-PASTE.local.md. It contains credentials and is ignored by Git.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
