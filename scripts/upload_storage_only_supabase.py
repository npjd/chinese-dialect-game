#!/usr/bin/env python3
"""
Upload dialect game assets to Supabase Storage only (no database writes).

It uploads:
  - audio clips to clips/{bvid}/{filename}.m4a
  - index JSON to indexes/all_clips.json

Env vars:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
"""

from __future__ import annotations

import argparse
import json
import logging
import mimetypes
import os
import time
from pathlib import Path
from typing import Any

from supabase import Client, create_client


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_JSON = REPO_ROOT / "webui" / "data" / "game_data.json"
DEFAULT_ENV_PATH = REPO_ROOT / ".env"
DEFAULT_LOG_PATH = REPO_ROOT / "logs" / "supabase_storage_upload.log"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


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
        value = value.strip().strip("'").strip('"')
        os.environ[key] = value


def guess_content_type(path: Path) -> str:
    mime, _ = mimetypes.guess_type(path.name)
    return mime or "application/octet-stream"


def ensure_bucket(client: Client, bucket: str, public: bool) -> None:
    existing = client.storage.list_buckets()
    names = {getattr(b, "name", None) or getattr(b, "id", None) for b in existing}
    if bucket not in names:
        client.storage.create_bucket(
            bucket,
            options={"public": public, "file_size_limit": "52428800"},
        )


def upload_bytes(client: Client, bucket: str, object_path: str, data: bytes, content_type: str) -> None:
    client.storage.from_(bucket).upload(
        path=object_path,
        file=data,
        file_options={"content-type": content_type, "upsert": "true"},
    )


def upload_file(client: Client, bucket: str, object_path: str, local_path: Path) -> None:
    content_type = guess_content_type(local_path)
    with local_path.open("rb") as f:
        upload_bytes(client, bucket, object_path, f.read(), content_type)


def upload_file_with_retry(
    client: Client,
    bucket: str,
    object_path: str,
    local_path: Path,
    max_attempts: int = 4,
) -> None:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            upload_file(client, bucket, object_path, local_path)
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == max_attempts:
                break
            sleep_sec = min(10, 1.5 * attempt)
            logging.warning(
                "upload failed (attempt %s/%s) for %s: %s. Retrying in %.1fs...",
                attempt,
                max_attempts,
                object_path,
                exc,
                sleep_sec,
            )
            time.sleep(sleep_sec)
    raise RuntimeError(f"Failed upload after {max_attempts} attempts: {object_path}") from last_error


def build_index_payload(data: dict[str, Any], bucket: str) -> dict[str, Any]:
    items = data.get("items") or []
    out_items: list[dict[str, Any]] = []

    for item in items:
        clip_path = str(item.get("clip_path") or "").strip()
        if not clip_path:
            continue
        filename = Path(clip_path).name
        bvid = str(item.get("bvid") or "").strip()
        if not bvid:
            continue
        storage_path = f"clips/{bvid}/{filename}"
        out_item = dict(item)
        out_item["storage_path"] = storage_path
        out_item["bucket"] = bucket
        out_items.append(out_item)

    return {
        "generated_at_unix": data.get("generated_at_unix"),
        "source": "storage_only_supabase",
        "items_count": len(out_items),
        "items": out_items,
    }


def deduplicate_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    duplicates = 0
    for row in rows:
        key = str(row.get("storage_path") or row.get("id") or "")
        if not key:
            continue
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, duplicates


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload clips + index to Supabase Storage.")
    parser.add_argument("--data-json", type=Path, default=DEFAULT_DATA_JSON)
    parser.add_argument("--bucket", type=str, default="dialect-game")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--public-bucket", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-file", type=Path, default=DEFAULT_LOG_PATH)
    args = parser.parse_args()
    setup_logging(args.log_file)

    data = load_json(args.data_json)
    items = data.get("items") or []
    if args.limit is not None:
        data = dict(data)
        data["items"] = items[: args.limit]

    index_payload = build_index_payload(data, args.bucket)
    clip_rows, duplicates = deduplicate_rows(index_payload["items"])
    index_payload["items"] = clip_rows
    index_payload["items_count"] = len(clip_rows)
    if not clip_rows:
        raise RuntimeError("No clips found in input data.")

    if args.dry_run:
        logging.info("[dry-run] unique clips to upload: %s", len(clip_rows))
        logging.info("[dry-run] duplicates removed: %s", duplicates)
        logging.info("[dry-run] would upload index to indexes/all_clips.json")
        return

    load_dotenv_file(DEFAULT_ENV_PATH)
    url = require_env("SUPABASE_URL")
    key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    client = create_client(url, key)

    ensure_bucket(client, args.bucket, public=args.public_bucket)
    logging.info("Bucket ready: %s (public=%s)", args.bucket, args.public_bucket)

    uploaded = 0
    failed = 0
    uploaded_rows: list[dict[str, Any]] = []
    total = len(clip_rows)
    for idx, row in enumerate(clip_rows, start=1):
        local_clip_path = REPO_ROOT / row["clip_path"]
        if not local_clip_path.exists():
            logging.warning("[skip] missing local clip: %s", row["clip_path"])
            failed += 1
            continue
        try:
            upload_file_with_retry(client, args.bucket, row["storage_path"], local_clip_path)
            uploaded += 1
            uploaded_rows.append(row)
            if idx % 25 == 0 or idx == total:
                logging.info(
                    "[progress] %s/%s processed, %s uploaded, %s failed",
                    idx,
                    total,
                    uploaded,
                    failed,
                )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logging.error("[skip] failed upload for %s: %s", row["storage_path"], exc)

    index_payload["items"] = uploaded_rows
    index_payload["items_count"] = len(uploaded_rows)

    index_bytes = json.dumps(index_payload, ensure_ascii=False, indent=2).encode("utf-8")
    upload_bytes(
        client,
        args.bucket,
        "indexes/all_clips.json",
        index_bytes,
        "application/json; charset=utf-8",
    )

    logging.info("Uploaded %s clips to bucket '%s'", uploaded, args.bucket)
    logging.info("Failed/skipped clips: %s", failed)
    logging.info("Duplicates removed before upload: %s", duplicates)
    logging.info("Uploaded index: indexes/all_clips.json")
    logging.info("Done.")


if __name__ == "__main__":
    main()
