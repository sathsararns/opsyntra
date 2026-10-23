"""Create local Docker environment settings once; never overwrite existing secrets."""
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    destination = ROOT / ".env"
    if destination.exists():
        print("Root .env already exists; preserved")
    else:
        content = (ROOT / ".env.example").read_text(encoding="utf-8")
        for suffix in ("ADMIN", "OWNER", "WRITE", "READ"):
            content = content.replace("CHANGE_ME_" + suffix, secrets.token_hex(24))
        with destination.open("x", encoding="utf-8") as output:
            output.write(content)
        print("Created ignored root .env with distinct random database passwords")
