#!/usr/bin/env python3
"""
Sync local game_data.json from the uploaded Supabase Storage index.

This ensures local JSON only contains clips that are actually present in
`indexes/all_clips.json` in your Supabase bucket.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH = REPO_ROOT / ".env"
DEFAULT_LOCAL_GAME_DATA = REPO_ROOT / "webui" / "data" / "game_data.json"


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip("'").strip('"')


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def fetch_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "dialect-game-sync/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync local game_data.json from Supabase index.")
    parser.add_argument("--bucket", default="dialect-game")
    parser.add_argument("--index-object-path", default="indexes/all_clips.json")
    parser.add_argument("--output", type=Path, default=DEFAULT_LOCAL_GAME_DATA)
    args = parser.parse_args()

    load_dotenv_file(DEFAULT_ENV_PATH)
    supabase_url = require_env("SUPABASE_URL").rstrip("/")
    bucket = args.bucket
    index_path = "/".join(part.strip("/") for part in args.index_object_path.split("/"))
    encoded_path = "/".join(urllib.parse.quote(seg) for seg in index_path.split("/"))
    url = f"{supabase_url}/storage/v1/object/public/{bucket}/{encoded_path}"

    remote = fetch_json(url)
    items = remote.get("items")
    if not isinstance(items, list):
        raise RuntimeError("Remote index is missing 'items' list.")

    payload = {
        "generated_at_unix": int(time.time()),
        "source_root": "supabase:indexes/all_clips.json",
        "total_metadata_files": len(items),
        "clips_with_coordinates": len(items),
        "unique_locations_cached": len({str(i.get('target_location', '')).strip() for i in items if i.get("target_location")}),
        "new_geocoding_requests": 0,
        "items": items,
    }

    write_json(args.output, payload)
    print(f"Wrote {len(items)} clips to {args.output}")
    print(f"Synced from: {url}")


if __name__ == "__main__":
    main()
