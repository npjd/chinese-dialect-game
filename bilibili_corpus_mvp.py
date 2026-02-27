#!/usr/bin/env python3
import argparse
import csv
import html
import json
import re
import time
from pathlib import Path
from typing import Any

import requests


SEARCH_API = "https://api.bilibili.com/x/web-interface/search/type"
CARD_API = "https://api.bilibili.com/x/web-interface/card"
VIDEO_PAGE = "https://www.bilibili.com/video/{bvid}"


def build_session(cookie: str | None) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.bilibili.com/",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
    )
    if cookie:
        s.headers["Cookie"] = cookie
    return s


def clean_title(raw: str) -> str:
    text = re.sub(r"</?em[^>]*>", "", raw)
    return html.unescape(text).strip()


def build_queries(keywords: list[str], locations: list[str]) -> list[tuple[str, str | None]]:
    if not locations:
        return [(kw, None) for kw in keywords]
    queries: list[tuple[str, str | None]] = []
    for loc in locations:
        for kw in keywords:
            queries.append((f"{loc} {kw}", loc))
    return queries


def location_match(row: dict[str, Any], locations: list[str]) -> bool:
    if not locations:
        return True
    haystack = " ".join(
        [
            str(row.get("video_ip_location", "") or ""),
            str(row.get("uploader_declared_location", "") or ""),
            str(row.get("title", "") or ""),
            str(row.get("uploader_sign", "") or ""),
        ]
    )
    return any(loc in haystack for loc in locations)


def parse_locations(locations_arg: str, locations_file: str | None) -> list[str]:
    locations = [x.strip() for x in locations_arg.split(",") if x.strip()]
    if locations_file:
        p = Path(locations_file)
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                city = line.strip()
                if city and not city.startswith("#"):
                    locations.append(city)
    # Keep order, remove duplicates.
    deduped = list(dict.fromkeys(locations))
    return deduped


def get_search_page(
    session: requests.Session,
    keyword: str,
    page: int,
    page_size: int,
    max_retries: int,
    retry_backoff_sec: float,
) -> list[dict[str, Any]]:
    params = {
        "search_type": "video",
        "keyword": keyword,
        "page": page,
        "order": "pubdate",
        "duration": 0,
        "tids": 0,
        "page_size": page_size,
    }
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            r = session.get(SEARCH_API, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json()
            data = payload.get("data", {})
            return data.get("result", []) or []
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                sleep_s = retry_backoff_sec * (attempt + 1)
                print(
                    f"[WARN] search retry {attempt + 1}/{max_retries} keyword={keyword} page={page} err={e}",
                    flush=True,
                )
                time.sleep(sleep_s)
    if last_exc:
        raise last_exc
    return []


def get_uploader_meta(session: requests.Session, mid: int, cache: dict[int, dict[str, Any]]) -> dict[str, Any]:
    if mid in cache:
        return cache[mid]
    meta: dict[str, Any] = {"uploader_declared_location": None, "uploader_sign": None}
    try:
        r = session.get(CARD_API, params={"mid": mid, "photo": "true"}, timeout=20)
        r.raise_for_status()
        payload = r.json().get("data", {})
        card = payload.get("card", {}) if isinstance(payload, dict) else {}
        if isinstance(card, dict):
            meta["uploader_sign"] = card.get("sign")
            # "place" appears on some profiles; often empty.
            meta["uploader_declared_location"] = card.get("place")
    except Exception:
        pass
    cache[mid] = meta
    return meta


def extract_ip_location_from_video_page(session: requests.Session, bvid: str) -> str | None:
    try:
        url = VIDEO_PAGE.format(bvid=bvid)
        r = session.get(url, timeout=20)
        r.raise_for_status()
        text = r.text
        patterns = [
            r'"ip_location"\s*:\s*"([^"]+)"',
            r'"pub_location"\s*:\s*"([^"]+)"',
            r'IP属地[:：]\s*([^\\"<]+)',
        ]
        for p in patterns:
            m = re.search(p, text)
            if m:
                return m.group(1).strip()
    except Exception:
        return None
    return None


def collect(
    keywords: list[str],
    locations: list[str],
    strict_location: bool,
    drop_null_video_ip_location: bool,
    only_null_video_ip_location: bool,
    pages_per_keyword: int,
    page_size: int,
    delay_sec: float,
    cookie: str | None,
    log_every: int,
    max_retries: int,
    retry_backoff_sec: float,
    out_dir: Path,
    status_every: int,
) -> dict[str, Any]:
    session = build_session(cookie)
    uploader_cache: dict[int, dict[str, Any]] = {}
    processed = 0
    collected = 0
    failures = 0
    started_at = int(time.time())

    out_dir.mkdir(parents=True, exist_ok=True)
    data_jsonl = out_dir / "bilibili_corpus_mvp.jsonl"
    data_csv = out_dir / "bilibili_corpus_mvp.csv"
    fail_jsonl = out_dir / "bilibili_corpus_failures.jsonl"
    fail_csv = out_dir / "bilibili_corpus_failures.csv"
    status_path = out_dir / "run_status.json"

    row_fields = [
        "keyword",
        "query",
        "target_location",
        "bvid",
        "aid",
        "video_url",
        "title",
        "pubdate",
        "duration",
        "author",
        "mid",
        "typename",
        "play",
        "danmaku",
        "uploader_declared_location",
        "uploader_sign",
        "video_ip_location",
    ]
    failure_fields = ["stage", "query", "page", "bvid", "mid", "error"]

    # Start each run with clean output files.
    data_jsonl.write_text("", encoding="utf-8")
    fail_jsonl.write_text("", encoding="utf-8")
    with data_csv.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=row_fields).writeheader()
    with fail_csv.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=failure_fields).writeheader()

    def write_status(current_query: str | None = None, current_page: int | None = None, done: bool = False) -> None:
        status = {
            "started_at_unix": started_at,
            "updated_at_unix": int(time.time()),
            "done": done,
            "processed": processed,
            "collected": collected,
            "failures": failures,
            "current_query": current_query,
            "current_page": current_page,
            "data_jsonl": str(data_jsonl),
            "data_csv": str(data_csv),
            "fail_jsonl": str(fail_jsonl),
            "fail_csv": str(fail_csv),
        }
        status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    write_status()

    queries = build_queries(keywords, locations)
    last_status_write = 0
    with (
        data_jsonl.open("a", encoding="utf-8") as data_jsonl_f,
        data_csv.open("a", newline="", encoding="utf-8") as data_csv_f,
        fail_jsonl.open("a", encoding="utf-8") as fail_jsonl_f,
        fail_csv.open("a", newline="", encoding="utf-8") as fail_csv_f,
    ):
        data_csv_writer = csv.DictWriter(data_csv_f, fieldnames=row_fields)
        fail_csv_writer = csv.DictWriter(fail_csv_f, fieldnames=failure_fields)

        for kw_idx, (query, target_location) in enumerate(queries, start=1):
            print(f"[INFO] query {kw_idx}/{len(queries)}: {query}", flush=True)
            for page in range(1, pages_per_keyword + 1):
                print(
                    f"[INFO] fetching page {page}/{pages_per_keyword} for query={query}",
                    flush=True,
                )
                try:
                    results = get_search_page(
                        session,
                        query,
                        page,
                        page_size,
                        max_retries=max_retries,
                        retry_backoff_sec=retry_backoff_sec,
                    )
                except Exception as e:
                    print(f"[WARN] search failed query={query} page={page}: {e}", flush=True)
                    fail_row = {
                        "stage": "search",
                        "query": query,
                        "page": page,
                        "bvid": "",
                        "mid": "",
                        "error": repr(e),
                    }
                    failures += 1
                    fail_jsonl_f.write(json.dumps(fail_row, ensure_ascii=False) + "\n")
                    fail_jsonl_f.flush()
                    fail_csv_writer.writerow(fail_row)
                    fail_csv_f.flush()
                    continue

                if not results:
                    print(f"[INFO] no results for query={query} page={page}; moving on", flush=True)
                    break

                print(f"[INFO] found {len(results)} videos on page {page} for query={query}", flush=True)
                for item in results:
                    mid = int(item.get("mid", 0) or 0)
                    bvid = str(item.get("bvid", "")).strip()
                    if not bvid:
                        continue
                    processed += 1

                    try:
                        uploader_meta = get_uploader_meta(session, mid, uploader_cache)
                    except Exception as e:
                        uploader_meta = {
                            "uploader_declared_location": None,
                            "uploader_sign": None,
                        }
                        fail_row = {
                            "stage": "uploader_meta",
                            "query": query,
                            "page": page,
                            "bvid": bvid,
                            "mid": mid,
                            "error": repr(e),
                        }
                        failures += 1
                        fail_jsonl_f.write(json.dumps(fail_row, ensure_ascii=False) + "\n")
                        fail_jsonl_f.flush()
                        fail_csv_writer.writerow(fail_row)
                        fail_csv_f.flush()
                    try:
                        ip_location = extract_ip_location_from_video_page(session, bvid)
                    except Exception as e:
                        ip_location = None
                        fail_row = {
                            "stage": "video_page",
                            "query": query,
                            "page": page,
                            "bvid": bvid,
                            "mid": mid,
                            "error": repr(e),
                        }
                        failures += 1
                        fail_jsonl_f.write(json.dumps(fail_row, ensure_ascii=False) + "\n")
                        fail_jsonl_f.flush()
                        fail_csv_writer.writerow(fail_row)
                        fail_csv_f.flush()

                    row = {
                        "keyword": item.get("keyword", ""),
                        "query": query,
                        "target_location": target_location,
                        "bvid": bvid,
                        "aid": item.get("aid"),
                        "video_url": f"https://www.bilibili.com/video/{bvid}",
                        "title": clean_title(str(item.get("title", ""))),
                        "pubdate": item.get("pubdate"),
                        "duration": item.get("duration"),
                        "author": item.get("author"),
                        "mid": mid,
                        "typename": item.get("typename"),
                        "play": item.get("play"),
                        "danmaku": item.get("video_review"),
                        "uploader_declared_location": uploader_meta.get("uploader_declared_location"),
                        "uploader_sign": uploader_meta.get("uploader_sign"),
                        "video_ip_location": ip_location,
                    }
                    if drop_null_video_ip_location and not row.get("video_ip_location"):
                        continue
                    if only_null_video_ip_location and row.get("video_ip_location"):
                        continue
                    if strict_location and not location_match(row, locations):
                        continue

                    collected += 1
                    data_jsonl_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    data_jsonl_f.flush()
                    data_csv_writer.writerow(row)
                    data_csv_f.flush()

                    if log_every > 0 and collected % log_every == 0:
                        print(
                            f"[INFO] processed={processed} collected={collected} failures={failures} last_bvid={bvid}",
                            flush=True,
                        )
                    now = int(time.time())
                    if now - last_status_write >= status_every:
                        write_status(current_query=query, current_page=page, done=False)
                        last_status_write = now
                    if delay_sec > 0:
                        time.sleep(delay_sec)

    write_status(done=True)
    return {
        "processed": processed,
        "collected": collected,
        "failures": failures,
        "data_jsonl": data_jsonl,
        "data_csv": data_csv,
        "fail_jsonl": fail_jsonl,
        "fail_csv": fail_csv,
        "status_path": status_path,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MVP Bilibili corpus collector (no transcript, metadata-first)."
    )
    parser.add_argument(
        "--keywords",
        default="四川话,东北话,上海话,粤语,闽南语,客家话",
        help="Comma-separated search keywords.",
    )
    parser.add_argument(
        "--locations",
        default="",
        help="Optional comma-separated location hints, e.g. 四川,广东,上海.",
    )
    parser.add_argument(
        "--locations-file",
        default=None,
        help="Optional text file with one location per line.",
    )
    parser.add_argument(
        "--strict-location",
        action="store_true",
        help="Only keep rows where detected location fields/text match --locations.",
    )
    parser.add_argument(
        "--drop-null-video-ip-location",
        action="store_true",
        help="Drop rows where video_ip_location is empty/null.",
    )
    parser.add_argument(
        "--only-null-video-ip-location",
        action="store_true",
        help="Keep only rows where video_ip_location is empty/null.",
    )
    parser.add_argument("--pages-per-keyword", type=int, default=2)
    parser.add_argument("--page-size", type=int, default=20)
    parser.add_argument("--delay-sec", type=float, default=0.4)
    parser.add_argument(
        "--cookie",
        default=None,
        help="Optional Bilibili cookie string for better access.",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=10,
        help="Print progress every N processed videos (default: 10).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Max retries for search requests (default: 2).",
    )
    parser.add_argument(
        "--retry-backoff-sec",
        type=float,
        default=1.0,
        help="Linear backoff base seconds between retries (default: 1.0).",
    )
    parser.add_argument(
        "--status-every",
        type=int,
        default=3,
        help="Write run_status.json every N seconds (default: 3).",
    )
    parser.add_argument("--out-dir", default="bili_corpus_out")
    args = parser.parse_args()
    if args.drop_null_video_ip_location and args.only_null_video_ip_location:
        raise SystemExit(
            "Use only one of --drop-null-video-ip-location or --only-null-video-ip-location."
        )

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    locations = parse_locations(args.locations, args.locations_file)
    print(
        f"[INFO] start crawl keywords={len(keywords)} locations={len(locations)} pages_per_keyword={args.pages_per_keyword} page_size={args.page_size}",
        flush=True,
    )
    summary = collect(
        keywords=keywords,
        locations=locations,
        strict_location=args.strict_location,
        drop_null_video_ip_location=args.drop_null_video_ip_location,
        only_null_video_ip_location=args.only_null_video_ip_location,
        pages_per_keyword=args.pages_per_keyword,
        page_size=args.page_size,
        delay_sec=args.delay_sec,
        cookie=args.cookie,
        log_every=args.log_every,
        max_retries=args.max_retries,
        retry_backoff_sec=args.retry_backoff_sec,
        out_dir=Path(args.out_dir),
        status_every=args.status_every,
    )
    print(f"Processed: {summary['processed']}")
    print(f"Collected: {summary['collected']}")
    print(f"Failures: {summary['failures']}")
    print(f"JSONL: {summary['data_jsonl']}")
    print(f"CSV:   {summary['data_csv']}")
    print(f"FAIL JSONL: {summary['fail_jsonl']}")
    print(f"FAIL CSV:   {summary['fail_csv']}")
    print(f"STATUS: {summary['status_path']}")
    print("Note: IP location fields are best-effort and may be missing or approximate.")


if __name__ == "__main__":
    main()
