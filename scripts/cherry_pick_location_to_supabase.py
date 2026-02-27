#!/usr/bin/env python3
"""
Cherry-pick clips by location, annotate, upload to Supabase Storage, and update index.

What it does:
1) scans local *.metadata.json files for the requested location(s)
2) builds clip records with coordinates
3) uploads audio clips to Supabase bucket (upsert)
4) merges into remote `indexes/all_clips.json` (dedupe by storage_path)
5) writes synced local `webui/data/game_data.json`
"""

from __future__ import annotations

import argparse
import json
import logging
import mimetypes
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from supabase import Client, create_client


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH = REPO_ROOT / ".env"
DEFAULT_CLIPS_ROOT = REPO_ROOT / "bili_gemini_test_out" / "clips_by_bvid"
DEFAULT_GAME_DATA = REPO_ROOT / "webui" / "data" / "game_data.json"
DEFAULT_LOG_FILE = REPO_ROOT / "logs" / "cherry_pick_location_to_supabase.log"


def setup_logging(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(path, encoding="utf-8")],
    )


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


def upload_file(client: Client, bucket: str, object_path: str, local_path: Path) -> None:
    content_type = guess_content_type(local_path)
    with local_path.open("rb") as f:
        client.storage.from_(bucket).upload(
            path=object_path,
            file=f.read(),
            file_options={"content-type": content_type, "upsert": "true"},
        )


def upload_bytes(client: Client, bucket: str, object_path: str, data: bytes, content_type: str) -> None:
    client.storage.from_(bucket).upload(
        path=object_path,
        file=data,
        file_options={"content-type": content_type, "upsert": "true"},
    )


def fetch_public_json(supabase_url: str, bucket: str, object_path: str) -> dict[str, Any]:
    base = supabase_url.rstrip("/")
    encoded = "/".join(urllib.parse.quote(seg) for seg in object_path.split("/"))
    url = f"{base}/storage/v1/object/public/{bucket}/{encoded}"
    req = urllib.request.Request(url, headers={"User-Agent": "dialect-game-cherry-pick/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def load_location_coords_from_local(path: Path) -> dict[str, tuple[float, float]]:
    coords: dict[str, tuple[float, float]] = {}
    if not path.exists():
        return coords
    payload = json.loads(path.read_text(encoding="utf-8"))
    for row in payload.get("items", []):
        loc = str(row.get("target_location") or row.get("city") or "").strip()
        if not loc:
            continue
        lat = row.get("latitude")
        lon = row.get("longitude")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            coords[loc] = (float(lat), float(lon))
    return coords


def build_candidates(
    clips_root: Path,
    locations: set[str],
    location_coords: dict[str, tuple[float, float]],
    bucket: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(clips_root.glob("**/*.metadata.json")):
        try:
            meta = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        target = str(meta.get("target_location") or "").strip()
        city = str(meta.get("city") or "").strip()
        if target not in locations and city not in locations:
            continue

        loc_key = target or city
        coord = location_coords.get(loc_key)
        if not coord:
            logging.warning("skip (no coordinates for location): %s (%s)", loc_key, path)
            continue

        clip_path = str(meta.get("clip_path") or "").strip()
        if not clip_path:
            continue
        local_clip = REPO_ROOT / clip_path
        if not local_clip.exists():
            logging.warning("skip missing local clip: %s", local_clip)
            continue

        bvid = str(meta.get("bvid") or path.parent.name).strip()
        filename = Path(clip_path).name
        storage_path = f"clips/{bvid}/{filename}"
        clip_id = f"{bvid}:{Path(filename).stem}"

        out.append(
            {
                "id": clip_id,
                "bvid": bvid,
                "title": str(meta.get("title") or "").strip(),
                "target_location": target,
                "city": city,
                "clip_path": clip_path,
                "clip_start": str(meta.get("clip_start") or "").strip(),
                "clip_end": str(meta.get("clip_end") or "").strip(),
                "transcript": str(meta.get("transcript") or "").strip(),
                "translation_en": str(meta.get("translation_en") or "").strip(),
                "description": str(meta.get("description") or "").strip(),
                "confidence": meta.get("confidence"),
                "latitude": coord[0],
                "longitude": coord[1],
                "storage_path": storage_path,
                "bucket": bucket,
                "annotation": {
                    "curated_by_location": True,
                    "curated_location_filter": sorted(locations),
                    "curated_at_unix": int(time.time()),
                },
            }
        )
    return out


def merge_items(existing: list[dict[str, Any]], new_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in existing:
        key = str(item.get("storage_path") or item.get("id") or "")
        if key:
            merged[key] = item
    for item in new_items:
        key = str(item.get("storage_path") or item.get("id") or "")
        if key:
            merged[key] = item
    return list(merged.values())


def main() -> None:
    parser = argparse.ArgumentParser(description="Cherry-pick clips by location and push to Supabase Storage.")
    parser.add_argument("--locations", required=True, help="Comma-separated location list, e.g. 北京,上海")
    parser.add_argument("--bucket", default="dialect-game")
    parser.add_argument("--index-object-path", default="indexes/all_clips.json")
    parser.add_argument("--clips-root", type=Path, default=DEFAULT_CLIPS_ROOT)
    parser.add_argument("--public-bucket", action="store_true")
    parser.add_argument("--sync-local-game-data", action="store_true", default=True)
    parser.add_argument("--log-file", type=Path, default=DEFAULT_LOG_FILE)
    args = parser.parse_args()

    setup_logging(args.log_file)
    load_dotenv_file(DEFAULT_ENV_PATH)
    supabase_url = require_env("SUPABASE_URL")
    service_key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    client = create_client(supabase_url, service_key)

    ensure_bucket(client, args.bucket, public=args.public_bucket)

    locations = {x.strip() for x in args.locations.split(",") if x.strip()}
    if not locations:
        raise RuntimeError("No locations specified.")

    location_coords = load_location_coords_from_local(DEFAULT_GAME_DATA)
    candidates = build_candidates(args.clips_root, locations, location_coords, args.bucket)
    if not candidates:
        raise RuntimeError(f"No candidate clips found for locations: {sorted(locations)}")

    logging.info("Found %s local candidate clips for %s", len(candidates), sorted(locations))

    uploaded = 0
    for idx, row in enumerate(candidates, start=1):
        local_clip = REPO_ROOT / row["clip_path"]
        upload_file(client, args.bucket, row["storage_path"], local_clip)
        uploaded += 1
        if idx % 25 == 0 or idx == len(candidates):
            logging.info("Uploaded %s/%s location-picked clips", idx, len(candidates))

    remote_index = fetch_public_json(supabase_url, args.bucket, args.index_object_path)
    existing_items = remote_index.get("items") if isinstance(remote_index.get("items"), list) else []
    merged_items = merge_items(existing_items, candidates)
    merged_payload = {
        "generated_at_unix": int(time.time()),
        "source": "storage_only_supabase+location_cherry_pick",
        "items_count": len(merged_items),
        "items": merged_items,
    }
    upload_bytes(
        client,
        args.bucket,
        args.index_object_path,
        json.dumps(merged_payload, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/json; charset=utf-8",
    )
    logging.info("Updated remote index %s with %s total clips", args.index_object_path, len(merged_items))

    if args.sync_local_game_data:
        local_payload = {
            "generated_at_unix": int(time.time()),
            "source_root": f"supabase:{args.index_object_path}",
            "total_metadata_files": len(merged_items),
            "clips_with_coordinates": len(merged_items),
            "unique_locations_cached": len(
                {
                    str(i.get("target_location") or i.get("city") or "").strip()
                    for i in merged_items
                    if str(i.get("target_location") or i.get("city") or "").strip()
                }
            ),
            "new_geocoding_requests": 0,
            "items": merged_items,
        }
        DEFAULT_GAME_DATA.write_text(json.dumps(local_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logging.info("Synced local game data: %s", DEFAULT_GAME_DATA)

    logging.info("Done. Uploaded %s clips for location filter %s.", uploaded, sorted(locations))


if __name__ == "__main__":
    main()
