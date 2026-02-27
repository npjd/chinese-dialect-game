#!/usr/bin/env python3
"""
Build a local dataset for the dialect geo-guess game.

It scans all clip metadata files, geocodes `target_location` to lat/lon using
OpenStreetMap Nominatim, caches results, and writes webui/data/game_data.json.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
CLIPS_ROOT = REPO_ROOT / "bili_gemini_test_out" / "clips_by_bvid"
OUTPUT_PATH = REPO_ROOT / "webui" / "data" / "game_data.json"
CACHE_PATH = REPO_ROOT / "webui" / "data" / "location_cache.json"
USER_AGENT = "dialect-geo-guess-local/1.0 (contact: local-dev)"


@dataclass
class ClipItem:
    id: str
    bvid: str
    title: str
    target_location: str
    city: str
    clip_path: str
    clip_start: str
    clip_end: str
    transcript: str
    translation_en: str
    description: str
    confidence: float | None
    latitude: float
    longitude: float


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_cache(path: Path) -> dict[str, dict[str, float] | None]:
    if not path.exists():
        return {}
    data = read_json(path)
    if not isinstance(data, dict):
        return {}
    return data


def geocode_location(location: str, timeout_s: float = 15.0) -> tuple[float, float] | None:
    query = f"{location}, China"
    params = urllib.parse.urlencode(
        {
            "q": query,
            "format": "jsonv2",
            "limit": 1,
            "accept-language": "zh,en",
        }
    )
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if not payload:
        return None
    top = payload[0]
    return float(top["lat"]), float(top["lon"])


def normalize_clip_path(raw_path: str) -> str:
    return raw_path.replace("\\", "/")


def build_items(
    metadata_paths: list[Path],
    cache: dict[str, dict[str, float] | None],
    geocode_pause_s: float,
    max_clips: int | None,
    max_new_locations: int | None,
) -> tuple[list[ClipItem], int]:
    items: list[ClipItem] = []
    geocoded_new = 0

    for metadata_path in sorted(metadata_paths):
        if max_clips is not None and len(items) >= max_clips:
            break
        try:
            meta = read_json(metadata_path)
        except (json.JSONDecodeError, OSError):
            continue

        target_location = str(meta.get("target_location") or "").strip()
        city = str(meta.get("city") or "").strip()
        loc_key = target_location or city
        if not loc_key:
            continue

        coord = cache.get(loc_key)
        if coord is None and loc_key not in cache:
            if max_new_locations is not None and geocoded_new >= max_new_locations:
                continue
            try:
                resolved = geocode_location(loc_key)
            except Exception:
                resolved = None
            geocoded_new += 1
            if resolved is None:
                cache[loc_key] = None
                time.sleep(geocode_pause_s)
                continue
            cache[loc_key] = {"lat": resolved[0], "lon": resolved[1]}
            coord = cache[loc_key]
            time.sleep(geocode_pause_s)

        if not coord:
            continue

        clip_path = normalize_clip_path(str(meta.get("clip_path") or ""))
        if not clip_path:
            continue
        if not (REPO_ROOT / clip_path).exists():
            continue

        clip_name = Path(clip_path).stem
        bvid = str(meta.get("bvid") or metadata_path.parent.name).strip()
        clip_id = f"{bvid}:{clip_name}"

        confidence = meta.get("confidence")
        confidence_value = float(confidence) if isinstance(confidence, (int, float)) else None

        items.append(
            ClipItem(
                id=clip_id,
                bvid=bvid,
                title=str(meta.get("title") or "").strip(),
                target_location=target_location,
                city=city,
                clip_path=clip_path,
                clip_start=str(meta.get("clip_start") or "").strip(),
                clip_end=str(meta.get("clip_end") or "").strip(),
                transcript=str(meta.get("transcript") or "").strip(),
                translation_en=str(meta.get("translation_en") or "").strip(),
                description=str(meta.get("description") or "").strip(),
                confidence=confidence_value,
                latitude=float(coord["lat"]),
                longitude=float(coord["lon"]),
            )
        )

    return items, geocoded_new


def main() -> None:
    parser = argparse.ArgumentParser(description="Build local dialect game JSON data.")
    parser.add_argument(
        "--pause",
        type=float,
        default=0.2,
        help="Seconds to wait between uncached geocoding requests (default: 0.2).",
    )
    parser.add_argument(
        "--max-clips",
        type=int,
        default=None,
        help="Optional max number of playable clips to output.",
    )
    parser.add_argument(
        "--max-new-locations",
        type=int,
        default=None,
        help="Optional cap for new geocoding requests this run.",
    )
    args = parser.parse_args()

    metadata_paths = list(CLIPS_ROOT.glob("**/*.metadata.json"))
    cache = load_cache(CACHE_PATH)
    items, geocoded_new = build_items(
        metadata_paths,
        cache,
        geocode_pause_s=args.pause,
        max_clips=args.max_clips,
        max_new_locations=args.max_new_locations,
    )

    payload = {
        "generated_at_unix": int(time.time()),
        "source_root": str(CLIPS_ROOT.relative_to(REPO_ROOT)),
        "total_metadata_files": len(metadata_paths),
        "clips_with_coordinates": len(items),
        "unique_locations_cached": len(cache),
        "new_geocoding_requests": geocoded_new,
        "items": [asdict(i) for i in items],
    }

    write_json(CACHE_PATH, cache)
    write_json(OUTPUT_PATH, payload)
    print(f"Wrote {len(items)} clips to {OUTPUT_PATH}")
    print(f"Cached {len(cache)} locations at {CACHE_PATH}")


if __name__ == "__main__":
    main()
