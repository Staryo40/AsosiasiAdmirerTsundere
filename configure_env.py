"""Create local service configuration from the tracked examples."""

from pathlib import Path
from secrets import token_hex


ROOT = Path(__file__).resolve().parent


def credential_values() -> dict:
    bmkg_key = token_hex(32)
    reader_secret = token_hex(32)
    postgres_password = token_hex(32)
    publisher_password = token_hex(32)
    return {
        "bmkg": {"BMKG_API_KEY": bmkg_key},
        "pvmbg": {
            "PVMBG_JWT_SECRET": token_hex(32),
            "PVMBG_READER_CLIENT_ID": "reader-client",
            "PVMBG_READER_CLIENT_SECRET": reader_secret,
            "PVMBG_ADMIN_CLIENT_SECRET": token_hex(32),
        },
        "bnpb": {
            "POSTGRES_PASSWORD": postgres_password,
            "RABBITMQ_DEFAULT_PASS": token_hex(32),
            "BNPB_AUTH_INTERNAL_KEY": token_hex(32),
            "BNPB_MEDIA_CLIENT_SECRET": token_hex(32),
            "BNPB_FIELD_CLIENT_SECRET": token_hex(32),
            "BNPB_OPS_CLIENT_SECRET": token_hex(32),
            "BNPB_BMKG_API_KEY": bmkg_key,
            "BNPB_PVMBG_READER_CLIENT_ID": "reader-client",
            "BNPB_PVMBG_READER_CLIENT_SECRET": reader_secret,
            "BNPB_AGGREGATOR_INTERNAL_KEY": token_hex(32),
            "BNPB_CANONICAL_DATABASE_URL":
                f"postgres://bnpb:{postgres_password}@postgres:5432/bnpb?sslmode=disable",
            "BNPB_AMQP_URL": f"amqp://publisher:{publisher_password}@rabbitmq:5672",
            "BNPB_RABBITMQ_PUBLISHER_USER": "publisher",
            "BNPB_RABBITMQ_PUBLISHER_PASSWORD": publisher_password,
            "BNPB_CONSUMER_1_RABBITMQ_PASSWORD": token_hex(32),
            "BNPB_CONSUMER_2_RABBITMQ_PASSWORD": token_hex(32),
            "BNPB_CONSUMER_3_RABBITMQ_PASSWORD": token_hex(32),
        },
    }


def env_contents(path: Path, replacements: dict) -> str:
    lines = path.with_name(".env.example").read_text(encoding="utf-8").splitlines()
    configured_lines = []
    for line in lines:
        key = line.partition("=")[0]
        configured_lines.append(f"{key}={replacements[key]}" if key in replacements else line)
    return "\n".join(configured_lines) + "\n"


def main() -> int:
    targets = [ROOT / name / ".env" for name in ("bmkg", "pvmbg", "bnpb")]
    if any(path.exists() for path in targets):
        raise SystemExit("An .env file already exists; edit existing files manually.")

    credentials = credential_values()
    configurations = {path: env_contents(path, credentials[path.parent.name]) for path in targets}
    for path, contents in configurations.items():
        with path.open("x", encoding="utf-8", newline="\n") as env_file:
            env_file.write(contents)
    print("Created bmkg/.env, pvmbg/.env, and bnpb/.env.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
