#!/usr/bin/env python3
"""Generate TELEGRAM_SESSION_STRING for sentiment telegram ingestion.

Run from data_platform/:
  python scripts/generate_telegram_session.py
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession


def _load_dotenv_if_present() -> None:
    root = Path(__file__).resolve().parents[1]
    workspace = root.parent

    for env_path in (workspace / ".env", root / ".env"):
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        break


async def _generate() -> None:
    api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()

    if not api_id_raw or not api_hash:
        raise SystemExit("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in environment/.env")

    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise SystemExit("TELEGRAM_API_ID must be an integer") from exc

    # Empty StringSession means: create a fresh in-memory session and print it after login.
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()

    session = client.session.save()
    print("\nTELEGRAM_SESSION_STRING generated successfully:\n")
    print(session)
    print("\nAdd this to your .env as:")
    print("TELEGRAM_SESSION_STRING=<paste_the_value>")

    await client.disconnect()


def main() -> None:
    _load_dotenv_if_present()
    asyncio.run(_generate())


if __name__ == "__main__":
    main()
