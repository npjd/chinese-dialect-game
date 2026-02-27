#!/usr/bin/env python3
"""
Random-sample test pipeline:
1) sample rows from bilibili_corpus_mvp.jsonl
2) download audio with yt-dlp
3) send audio to Gemini
4) extract dialect examples (if any)

Usage:
  GEMINI_API_KEY=... python test_gemini_dialect_examples.py \
    --input-jsonl bili_corpus_out/live_run/bilibili_corpus_mvp.jsonl \
    --sample-size 5 \
    --out-dir bili_gemini_test_out
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import subprocess
import time
from pathlib import Path
from typing import Any

from google import genai
from google.genai import types


GEMINI_DIALECT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {
            "type": "string",
            "description": "Concise summary of the audio content.",
        },
        "examples": {
            "type": "array",
            "description": "Dialect-specific timestamped examples. Empty if none found.",
            "items": {
                "type": "object",
                "properties": {
                    "start": {"type": "string", "description": "Start timestamp in MM:SS."},
                    "end": {"type": "string", "description": "End timestamp in MM:SS."},
                    "transcript": {
                        "type": "string",
                        "description": "Original transcript for this segment.",
                    },
                    "translation_en": {
                        "type": "string",
                        "description": "English translation of transcript.",
                    },
                    "reason": {
                        "type": "string",
                        "description": "Why this segment appears dialect-specific.",
                    },
                    "confidence": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "Confidence score from 0.0 to 1.0.",
                    },
                },
                "required": [
                    "start",
                    "end",
                    "transcript",
                    "translation_en",
                    "reason",
                    "confidence",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["summary", "examples"],
    "additionalProperties": False,
}


class YtDlpDownloadError(RuntimeError):
    def __init__(self, message: str, returncode: int, stdout: str, stderr: str, cmd: list[str]):
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.cmd = cmd


class ClipExtractError(RuntimeError):
    pass


class YtDlpSectionDownloadError(RuntimeError):
    def __init__(self, message: str, returncode: int, stdout: str, stderr: str, cmd: list[str]):
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.cmd = cmd


def load_env_file(env_path: Path) -> None:
    """
    Minimal .env loader for KEY=VALUE lines.
    Existing environment variables are not overwritten.
    """
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        if key not in os.environ:
            os.environ[key] = value


def load_rows(jsonl_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_bvid: set[str] = set()
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line_no, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            bvid = str(row.get("bvid", "")).strip()
            video_url = str(row.get("video_url", "")).strip()
            if not bvid or not video_url:
                continue
            if bvid in seen_bvid:
                continue
            seen_bvid.add(bvid)
            row["_line_no"] = line_no
            rows.append(row)
    return rows


def parse_mmss_to_seconds(ts: str) -> float:
    val = (ts or "").strip()
    if not val:
        raise ValueError("Empty timestamp")
    parts = val.split(":")
    if len(parts) == 2:
        mm, ss = parts
        return int(mm) * 60 + float(ss)
    if len(parts) == 3:
        hh, mm, ss = parts
        return int(hh) * 3600 + int(mm) * 60 + float(ss)
    raise ValueError(f"Unsupported timestamp format: {ts}")


def safe_slug(text: str) -> str:
    out = re.sub(r"[^\w\-]+", "_", (text or "").strip(), flags=re.UNICODE)
    out = re.sub(r"_+", "_", out).strip("_")
    return out or "unknown"


def format_seconds_hhmmss(seconds: float) -> str:
    total = max(0.0, seconds)
    hh = int(total // 3600)
    mm = int((total % 3600) // 60)
    ss = total % 60
    return f"{hh:02d}:{mm:02d}:{ss:06.3f}"


def download_audio_with_ytdlp(
    video_url: str,
    bvid: str,
    audio_dir: Path,
    ytdlp_cookies_from_browser: str | None = None,
    ytdlp_cookie_file: str | None = None,
    bilibili_cookie: str | None = None,
    max_retries: int = 2,
    retry_backoff_sec: float = 2.0,
    timeout_sec: float = 600.0,
) -> Path:
    audio_dir.mkdir(parents=True, exist_ok=True)
    output_template = str(audio_dir / f"{bvid}.%(ext)s")
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        cmd = [
            "yt-dlp",
            "--no-playlist",
            # Prefer m4a, fall back to other audio-only streams if needed.
            "-f",
            "ba[ext=m4a]/ba[ext=mp4]/ba",
            "--output",
            output_template,
        ]
        if ytdlp_cookies_from_browser:
            cmd.extend(["--cookies-from-browser", ytdlp_cookies_from_browser])
        if ytdlp_cookie_file:
            cmd.extend(["--cookies", ytdlp_cookie_file])
        if bilibili_cookie:
            cmd.extend(["--add-header", f"Cookie: {bilibili_cookie}"])
        cmd.extend(["--add-header", "Referer: https://www.bilibili.com/"])
        cmd.extend(
            [
                "--add-header",
                "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            ]
        )
        cmd.append(video_url)

        try:
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
        except subprocess.TimeoutExpired as e:
            last_exc = YtDlpDownloadError(
                message=f"yt-dlp timed out after {timeout_sec:.1f}s",
                returncode=-1,
                stdout=(e.stdout or ""),
                stderr=(e.stderr or ""),
                cmd=cmd,
            )
            if attempt < max_retries:
                sleep_s = retry_backoff_sec * (attempt + 1)
                print(
                    f"[WARN] yt-dlp timeout retry {attempt + 1}/{max_retries} for {bvid}; sleeping {sleep_s:.1f}s",
                    flush=True,
                )
                time.sleep(sleep_s)
                continue
            raise last_exc
        if proc.returncode == 0:
            break
        last_exc = YtDlpDownloadError(
            message=f"yt-dlp failed with exit code {proc.returncode}",
            returncode=proc.returncode,
            stdout=proc.stdout or "",
            stderr=proc.stderr or "",
            cmd=cmd,
        )
        if attempt < max_retries:
            sleep_s = retry_backoff_sec * (attempt + 1)
            print(
                f"[WARN] yt-dlp retry {attempt + 1}/{max_retries} for {bvid}; sleeping {sleep_s:.1f}s",
                flush=True,
            )
            time.sleep(sleep_s)
            continue
        raise last_exc

    if last_exc and proc.returncode != 0:
        raise last_exc
    matches = sorted(audio_dir.glob(f"{bvid}.*"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"No audio file found after yt-dlp for bvid={bvid}")
    return matches[0]


def download_audio_section_with_ytdlp(
    video_url: str,
    output_path: Path,
    start_sec: float,
    end_sec: float,
    ytdlp_cookies_from_browser: str | None = None,
    ytdlp_cookie_file: str | None = None,
    bilibili_cookie: str | None = None,
    max_retries: int = 2,
    retry_backoff_sec: float = 2.0,
    timeout_sec: float = 240.0,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    start_hms = format_seconds_hhmmss(start_sec)
    end_hms = format_seconds_hhmmss(end_sec)
    section_spec = f"*{start_hms}-{end_hms}"
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        cmd = [
            "yt-dlp",
            "--no-playlist",
            "-f",
            "ba[ext=m4a]/ba[ext=mp4]/ba",
            "--download-sections",
            section_spec,
            "--force-keyframes-at-cuts",
            "--output",
            str(output_path),
        ]
        if ytdlp_cookies_from_browser:
            cmd.extend(["--cookies-from-browser", ytdlp_cookies_from_browser])
        if ytdlp_cookie_file:
            cmd.extend(["--cookies", ytdlp_cookie_file])
        if bilibili_cookie:
            cmd.extend(["--add-header", f"Cookie: {bilibili_cookie}"])
        cmd.extend(["--add-header", "Referer: https://www.bilibili.com/"])
        cmd.extend(
            [
                "--add-header",
                "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            ]
        )
        cmd.append(video_url)
        try:
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
        except subprocess.TimeoutExpired as e:
            last_exc = YtDlpSectionDownloadError(
                message=f"yt-dlp section download timed out after {timeout_sec:.1f}s",
                returncode=-1,
                stdout=(e.stdout or ""),
                stderr=(e.stderr or ""),
                cmd=cmd,
            )
            if attempt < max_retries:
                sleep_s = retry_backoff_sec * (attempt + 1)
                print(
                    f"[WARN] yt-dlp section timeout retry {attempt + 1}/{max_retries}; sleeping {sleep_s:.1f}s",
                    flush=True,
                )
                time.sleep(sleep_s)
                continue
            raise last_exc
        if proc.returncode == 0 and output_path.exists():
            return
        last_exc = YtDlpSectionDownloadError(
            message=f"yt-dlp section download failed with exit code {proc.returncode}",
            returncode=proc.returncode,
            stdout=proc.stdout or "",
            stderr=proc.stderr or "",
            cmd=cmd,
        )
        if attempt < max_retries:
            sleep_s = retry_backoff_sec * (attempt + 1)
            print(
                f"[WARN] yt-dlp section retry {attempt + 1}/{max_retries}; sleeping {sleep_s:.1f}s",
                flush=True,
            )
            time.sleep(sleep_s)
            continue
        raise last_exc
    if last_exc:
        raise last_exc


def build_prompt(row: dict[str, Any], max_examples: int) -> str:
    query = str(row.get("query", "") or "")
    title = str(row.get("title", "") or "")
    target_location = str(row.get("target_location", "") or "")
    return f"""
You are analyzing one short audio track from a Bilibili video.

Video metadata:
- query: {query}
- title: {title}
- target_location: {target_location}

Task:
1) Produce a concise summary of the audio.
2) Find up to {max_examples} timestamped segments that are likely dialect speech.
3) If no dialect examples are found, return an empty list for examples.
4) Timestamps must be MM:SS format.

Return strict JSON with this shape:
{{
  "summary": "string",
  "examples": [
    {{
      "start": "MM:SS",
      "end": "MM:SS",
      "transcript": "original text",
      "translation_en": "english translation",
      "reason": "why this sounds dialect-specific",
      "confidence": 0.0
    }}
  ]
}}
Only return JSON.
""".strip()


def parse_json_response(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return json.loads(cleaned)


def extract_audio_clip(
    src_audio_path: Path,
    clip_path: Path,
    start_sec: float,
    end_sec: float,
    timeout_sec: float = 180.0,
) -> None:
    duration = max(0.0, end_sec - start_sec)
    if duration <= 0:
        raise ClipExtractError(f"Invalid clip duration: start={start_sec}, end={end_sec}")

    clip_path.parent.mkdir(parents=True, exist_ok=True)
    # First try stream copy for speed.
    copy_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{start_sec:.3f}",
        "-t",
        f"{duration:.3f}",
        "-i",
        str(src_audio_path),
        "-vn",
        "-c",
        "copy",
        str(clip_path),
    ]
    try:
        proc = subprocess.run(
            copy_cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as e:
        proc = subprocess.CompletedProcess(
            args=copy_cmd,
            returncode=-1,
            stdout=(e.stdout or ""),
            stderr=f"ffmpeg copy timed out after {timeout_sec:.1f}s",
        )
    if proc.returncode == 0 and clip_path.exists():
        return

    # Fallback: re-encode clip to m4a/aac.
    reencode_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{start_sec:.3f}",
        "-t",
        f"{duration:.3f}",
        "-i",
        str(src_audio_path),
        "-vn",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        str(clip_path),
    ]
    try:
        proc2 = subprocess.run(
            reencode_cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as e:
        proc2 = subprocess.CompletedProcess(
            args=reencode_cmd,
            returncode=-1,
            stdout=(e.stdout or ""),
            stderr=f"ffmpeg reencode timed out after {timeout_sec:.1f}s",
        )
    if proc2.returncode != 0 or not clip_path.exists():
        raise ClipExtractError(
            f"ffmpeg failed to extract clip. copy_err={proc.stderr!r} reencode_err={proc2.stderr!r}"
        )


def city_guess(row: dict[str, Any]) -> str:
    target = str(row.get("target_location", "") or "").strip()
    video_ip = str(row.get("video_ip_location", "") or "").strip()
    uploader_loc = str(row.get("uploader_declared_location", "") or "").strip()
    return target or video_ip or uploader_loc or "unknown"


def list_existing_bvid_dirs(clips_root: Path) -> set[str]:
    if not clips_root.exists():
        return set()
    existing: set[str] = set()
    for p in clips_root.iterdir():
        if p.is_dir() and p.name:
            existing.add(p.name)
    return existing


def analyze_audio_with_gemini(
    client: genai.Client,
    model: str,
    audio_path: Path,
    row: dict[str, Any],
    max_examples: int,
) -> dict[str, Any]:
    uploaded = client.files.upload(file=str(audio_path))
    prompt = build_prompt(row, max_examples=max_examples)
    response = client.models.generate_content(
        model=model,
        contents=[
            types.Content(
                parts=[
                    types.Part(file_data=types.FileData(file_uri=uploaded.uri)),
                    types.Part(text=prompt),
                ]
            )
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_json_schema=GEMINI_DIALECT_SCHEMA,
        ),
    )
    parsed_raw = parse_json_response(response.text or "{}")

    # Be defensive: model can sometimes return a top-level list instead of object.
    if isinstance(parsed_raw, list):
        return {
            "summary": "",
            "examples": parsed_raw,
        }
    if isinstance(parsed_raw, dict):
        parsed = dict(parsed_raw)
        if "summary" not in parsed or not isinstance(parsed.get("summary"), str):
            parsed["summary"] = ""
        if "examples" not in parsed or not isinstance(parsed.get("examples"), list):
            parsed["examples"] = []
        return parsed

    # Fallback for unexpected JSON type.
    return {
        "summary": "",
        "examples": [],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Test random dialect extraction with yt-dlp + Gemini.")
    parser.add_argument(
        "--input-jsonl",
        default="bili_corpus_out/live_run/bilibili_corpus_mvp.jsonl",
        help="Path to bilibili_corpus_mvp.jsonl",
    )
    parser.add_argument("--sample-size", type=int, default=5, help="How many random videos to test")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--model", default="gemini-3-flash-preview", help="Gemini model name")
    parser.add_argument("--max-examples", type=int, default=5, help="Max dialect examples per audio")
    parser.add_argument(
        "--keep-audio",
        action="store_true",
        help="Keep full downloaded source audios after clipping.",
    )
    parser.add_argument("--out-dir", default="bili_gemini_test_out", help="Output directory")
    parser.add_argument(
        "--ytdlp-cookies-from-browser",
        default=None,
        help="Optional browser name for yt-dlp --cookies-from-browser (e.g. safari, chrome).",
    )
    parser.add_argument(
        "--ytdlp-cookie-file",
        default=None,
        help="Optional cookie file path for yt-dlp --cookies.",
    )
    parser.add_argument(
        "--bilibili-cookie-env-key",
        default="BILIBILI_COOKIE",
        help="Env key for raw Bilibili cookie string (default: BILIBILI_COOKIE).",
    )
    parser.add_argument(
        "--download-sections-only",
        action="store_true",
        help=(
            "For each detected example, redownload only the timestamp section via yt-dlp --download-sections. "
            "Falls back to ffmpeg clip extraction if section download fails."
        ),
    )
    parser.add_argument(
        "--ytdlp-max-retries",
        type=int,
        default=2,
        help="Max retries for yt-dlp download failures (default: 2).",
    )
    parser.add_argument(
        "--ytdlp-retry-backoff-sec",
        type=float,
        default=2.0,
        help="Linear backoff base seconds between yt-dlp retries (default: 2.0).",
    )
    parser.add_argument(
        "--ytdlp-timeout-sec",
        type=float,
        default=600.0,
        help="Timeout for each yt-dlp invocation in seconds (default: 600).",
    )
    parser.add_argument(
        "--section-ytdlp-timeout-sec",
        type=float,
        default=240.0,
        help="Timeout for yt-dlp --download-sections calls in seconds (default: 240).",
    )
    parser.add_argument(
        "--ffmpeg-timeout-sec",
        type=float,
        default=180.0,
        help="Timeout for each ffmpeg clip extraction attempt in seconds (default: 180).",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Optional .env file path to load GEMINI_API_KEY from (default: .env)",
    )
    args = parser.parse_args()

    load_env_file(Path(args.env_file))
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit(
            "GEMINI_API_KEY is required. Set it in environment or place it in .env."
        )
    bilibili_cookie = os.getenv(args.bilibili_cookie_env_key, "").strip() or None

    input_path = Path(args.input_jsonl)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    audio_dir = out_dir / "audio_tmp"
    clips_root = out_dir / "clips_by_bvid"
    results_jsonl = out_dir / "results.jsonl"
    failures_jsonl = out_dir / "failures.jsonl"
    summary_json = out_dir / "summary.json"

    rows = load_rows(input_path)
    if not rows:
        raise SystemExit(f"No valid rows found in {input_path}")
    if args.sample_size <= 0:
        raise SystemExit("--sample-size must be > 0")

    existing_bvids = list_existing_bvid_dirs(clips_root)
    if existing_bvids:
        print(
            f"[INFO] found {len(existing_bvids)} existing BVID folders under {clips_root}; they will be skipped",
            flush=True,
        )
    before_filter_count = len(rows)
    rows = [r for r in rows if str(r.get("bvid", "")) not in existing_bvids]
    skipped_existing = before_filter_count - len(rows)
    if not rows:
        raise SystemExit(
            "No candidate rows remain after skipping existing clips_by_bvid entries."
        )

    rng = random.Random(args.seed)
    sample_n = min(args.sample_size, len(rows))
    picked = rng.sample(rows, sample_n)

    client = genai.Client(api_key=api_key)

    processed = 0
    ok = 0
    failed = 0
    clips_created = 0
    started = int(time.time())

    with (
        results_jsonl.open("a", encoding="utf-8") as rf,
        failures_jsonl.open("a", encoding="utf-8") as ff,
    ):
        for idx, row in enumerate(picked, start=1):
            bvid = str(row.get("bvid", ""))
            video_url = str(row.get("video_url", ""))
            print(f"[INFO] {idx}/{sample_n} bvid={bvid} downloading audio...", flush=True)
            processed += 1
            try:
                audio_path = download_audio_with_ytdlp(
                    video_url,
                    bvid,
                    audio_dir,
                    ytdlp_cookies_from_browser=args.ytdlp_cookies_from_browser,
                    ytdlp_cookie_file=args.ytdlp_cookie_file,
                    bilibili_cookie=bilibili_cookie,
                    max_retries=args.ytdlp_max_retries,
                    retry_backoff_sec=args.ytdlp_retry_backoff_sec,
                    timeout_sec=args.ytdlp_timeout_sec,
                )
                print(f"[INFO] {idx}/{sample_n} bvid={bvid} Gemini analysis...", flush=True)
                analysis = analyze_audio_with_gemini(
                    client=client,
                    model=args.model,
                    audio_path=audio_path,
                    row=row,
                    max_examples=args.max_examples,
                )
                bvid_dir = clips_root / bvid
                bvid_dir.mkdir(parents=True, exist_ok=True)
                clip_rows: list[dict[str, Any]] = []
                for ex_idx, ex in enumerate(analysis.get("examples", []), start=1):
                    try:
                        start_raw = str(ex.get("start", "") or "")
                        end_raw = str(ex.get("end", "") or "")
                        start_sec = parse_mmss_to_seconds(start_raw)
                        end_sec = parse_mmss_to_seconds(end_raw)
                        if end_sec <= start_sec:
                            # Minimal fallback when model gives same timestamp.
                            end_sec = start_sec + 1.0

                        clip_name = f"clip_{ex_idx:03d}_{safe_slug(start_raw)}_{safe_slug(end_raw)}.m4a"
                        clip_path = bvid_dir / clip_name
                        if args.download_sections_only:
                            try:
                                download_audio_section_with_ytdlp(
                                    video_url=video_url,
                                    output_path=clip_path,
                                    start_sec=start_sec,
                                    end_sec=end_sec,
                                    ytdlp_cookies_from_browser=args.ytdlp_cookies_from_browser,
                                    ytdlp_cookie_file=args.ytdlp_cookie_file,
                                    bilibili_cookie=bilibili_cookie,
                                    max_retries=args.ytdlp_max_retries,
                                    retry_backoff_sec=args.ytdlp_retry_backoff_sec,
                                    timeout_sec=args.section_ytdlp_timeout_sec,
                                )
                            except Exception:
                                # Fallback keeps pipeline robust if site/format doesn't support partial section download.
                                extract_audio_clip(
                                    src_audio_path=audio_path,
                                    clip_path=clip_path,
                                    start_sec=start_sec,
                                    end_sec=end_sec,
                                    timeout_sec=args.ffmpeg_timeout_sec,
                                )
                        else:
                            extract_audio_clip(
                                src_audio_path=audio_path,
                                clip_path=clip_path,
                                start_sec=start_sec,
                                end_sec=end_sec,
                                timeout_sec=args.ffmpeg_timeout_sec,
                            )

                        clip_meta = {
                            "bvid": bvid,
                            "video_url": video_url,
                            "title": row.get("title"),
                            "query": row.get("query"),
                            "target_location": row.get("target_location"),
                            "city": city_guess(row),
                            "video_ip_location": row.get("video_ip_location"),
                            "uploader_declared_location": row.get("uploader_declared_location"),
                            "audio_source_path": str(audio_path),
                            "clip_path": str(clip_path),
                            "clip_start": start_raw,
                            "clip_end": end_raw,
                            "clip_start_sec": round(start_sec, 3),
                            "clip_end_sec": round(end_sec, 3),
                            "clip_duration_sec": round(max(0.0, end_sec - start_sec), 3),
                            # Requested metadata fields.
                            "description": ex.get("reason", ""),
                            "translation_en": ex.get("translation_en", ""),
                            # Other useful metadata.
                            "transcript": ex.get("transcript", ""),
                            "confidence": ex.get("confidence"),
                            "summary": analysis.get("summary", ""),
                            "example_index": ex_idx,
                            "created_at_unix": int(time.time()),
                            "section_download_only": bool(args.download_sections_only),
                        }
                        clip_meta_path = bvid_dir / f"{clip_path.stem}.metadata.json"
                        clip_meta_path.write_text(
                            json.dumps(clip_meta, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                        clip_rows.append(clip_meta)
                        clips_created += 1
                    except Exception as clip_e:
                        print(
                            f"[WARN] clip extraction failed for bvid={bvid} ex#{ex_idx}: {clip_e}",
                            flush=True,
                        )

                bvid_manifest = {
                    "bvid": bvid,
                    "video_url": video_url,
                    "title": row.get("title"),
                    "query": row.get("query"),
                    "target_location": row.get("target_location"),
                    "city": city_guess(row),
                    "summary": analysis.get("summary", ""),
                    "example_count_model": len(analysis.get("examples", [])),
                    "example_count_clipped": len(clip_rows),
                    "clips": clip_rows,
                }
                (bvid_dir / "manifest.json").write_text(
                    json.dumps(bvid_manifest, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                out_row = {
                    "bvid": bvid,
                    "video_url": video_url,
                    "query": row.get("query"),
                    "target_location": row.get("target_location"),
                    "title": row.get("title"),
                    "audio_path": str(audio_path),
                    "summary": analysis.get("summary", ""),
                    "examples": analysis.get("examples", []),
                    "example_count": len(analysis.get("examples", [])),
                    "bvid_folder": str(bvid_dir),
                    "clips_created": len(clip_rows),
                }
                rf.write(json.dumps(out_row, ensure_ascii=False) + "\n")
                rf.flush()
                ok += 1
                if not args.keep_audio and audio_path.exists():
                    try:
                        audio_path.unlink()
                    except Exception:
                        pass
            except Exception as e:
                err_text = repr(e)
                ytdlp_stderr = None
                ytdlp_stdout = None
                ytdlp_cmd = None
                ytdlp_returncode = None
                if isinstance(e, YtDlpDownloadError):
                    ytdlp_stderr = e.stderr[-8000:] if e.stderr else ""
                    ytdlp_stdout = e.stdout[-8000:] if e.stdout else ""
                    ytdlp_cmd = e.cmd
                    ytdlp_returncode = e.returncode
                    # Show the useful tail in terminal for quick debugging.
                    if ytdlp_stderr:
                        print(
                            f"[WARN] yt-dlp stderr tail for {bvid}:\n{ytdlp_stderr[-1000:]}",
                            flush=True,
                        )
                fail_row = {
                    "bvid": bvid,
                    "video_url": video_url,
                    "query": row.get("query"),
                    "target_location": row.get("target_location"),
                    "title": row.get("title"),
                    "error": err_text,
                    "ytdlp_returncode": ytdlp_returncode,
                    "ytdlp_cmd": ytdlp_cmd,
                    "ytdlp_stdout": ytdlp_stdout,
                    "ytdlp_stderr": ytdlp_stderr,
                }
                ff.write(json.dumps(fail_row, ensure_ascii=False) + "\n")
                ff.flush()
                failed += 1
                print(f"[WARN] bvid={bvid} failed: {e}", flush=True)

    if not args.keep_audio and audio_dir.exists():
        for p in audio_dir.glob("*"):
            try:
                p.unlink()
            except Exception:
                pass
        try:
            audio_dir.rmdir()
        except Exception:
            pass

    summary = {
        "started_at_unix": started,
        "ended_at_unix": int(time.time()),
        "input_jsonl": str(input_path),
        "sample_size_requested": args.sample_size,
        "sample_size_used": sample_n,
        "skipped_existing_bvid_dirs": skipped_existing,
        "processed": processed,
        "ok": ok,
        "failed": failed,
        "clips_created": clips_created,
        "results_jsonl": str(results_jsonl),
        "failures_jsonl": str(failures_jsonl),
        "clips_root": str(clips_root),
        "model": args.model,
        "download_sections_only": bool(args.download_sections_only),
        "bilibili_cookie_env_key": args.bilibili_cookie_env_key,
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[DONE] processed={processed} ok={ok} failed={failed}", flush=True)
    print(f"[DONE] clips_created={clips_created}", flush=True)
    print(f"[DONE] results: {results_jsonl}", flush=True)
    print(f"[DONE] failures: {failures_jsonl}", flush=True)
    print(f"[DONE] clips root: {clips_root}", flush=True)
    print(f"[DONE] summary: {summary_json}", flush=True)


if __name__ == "__main__":
    main()
