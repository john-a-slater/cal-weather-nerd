#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://mesowest.utah.edu/cgi-bin/droman/station_dl_output.cgi"
STATION = "MSSKI"
START_DATE = dt.date(2022, 1, 1)
END_DATE = dt.date(2026, 12, 31)
OUT_DIR = Path("/Users/slater/Desktop/Website/data")
CSV_OUT = OUT_DIR / "msski_daily_max_snow_2022_2026.csv"
JSON_OUT = OUT_DIR / "msski_daily_max_snow_2022_2026.json"
MAX_WORKERS = 14

PRE_RE = re.compile(r"<PRE>(.*?)</PRE>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?$")


def daterange(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def parse_daily_max_snow(html: str) -> tuple[str, float | None, int]:
    match = PRE_RE.search(html)
    if not match:
        return "no_data", None, 0

    text = TAG_RE.sub("", match.group(1))
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    vals = []
    for ln in lines:
        if ln.startswith("PARM") or ln.startswith("English Units"):
            continue
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) < 11:
            continue
        snow = parts[10]
        if NUM_RE.match(snow):
            vals.append(float(snow))

    if not vals:
        return "no_valid_snow", None, 0
    return "ok", max(vals), len(vals)


def fetch_one(target: dt.date) -> dict:
    params = {
        "stn": STATION,
        "unit": 0,
        "time": "LOCAL",
        "day1": f"{target.day:02d}",
        "month1": f"{target.month:02d}",
        "year1": str(target.year),
        "hour1": 15,
        "output": "csv",
    }
    query = urlencode(params)
    url = f"{BASE_URL}?{query}"

    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        status, max_snow, samples = parse_daily_max_snow(html)
    except Exception as exc:  # noqa: BLE001
        status = f"error:{type(exc).__name__}"
        max_snow = None
        samples = 0

    return {
        "date": target.isoformat(),
        "year": target.year,
        "month": target.month,
        "day": target.day,
        "max_snow_in": max_snow,
        "sample_count": samples,
        "status": status,
        "source_url": url,
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dates = list(daterange(START_DATE, END_DATE))
    total = len(dates)
    rows = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_one, d) for d in dates]
        for idx, fut in enumerate(as_completed(futures), start=1):
            rows.append(fut.result())
            if idx % 200 == 0 or idx == total:
                print(f"Processed {idx}/{total} days...")

    rows.sort(key=lambda r: r["date"])

    with CSV_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "year",
                "month",
                "day",
                "max_snow_in",
                "sample_count",
                "status",
                "source_url",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    with JSON_OUT.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=True, separators=(",", ":"))

    ok_days = sum(1 for r in rows if r["status"] == "ok")
    no_data_days = sum(1 for r in rows if r["status"] == "no_data")
    err_days = sum(1 for r in rows if r["status"].startswith("error:"))
    print(f"Saved CSV: {CSV_OUT}")
    print(f"Saved JSON: {JSON_OUT}")
    print(f"Rows: {len(rows)} | ok: {ok_days} | no_data: {no_data_days} | errors: {err_days}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
