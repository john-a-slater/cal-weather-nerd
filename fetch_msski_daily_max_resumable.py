#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://mesowest.utah.edu/cgi-bin/droman/station_dl_output.cgi"
OUT_DIR = Path('/Users/slater/Desktop/Website/data')
OUT_CSV = OUT_DIR / 'msski_daily_max_snow_2022_2026.csv'
STATION = 'MSSKI'
START = dt.date(2022, 1, 1)
END = dt.date(2026, 12, 31)

PRE_RE = re.compile(r'<PRE>(.*?)</PRE>', re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r'<[^>]+>')
NUM_RE = re.compile(r'^-?\d+(?:\.\d+)?$')
THROTTLE_PHRASE = 'high number of recent download attempts'


def daterange(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def load_existing() -> set[str]:
    done = set()
    if OUT_CSV.exists():
        with OUT_CSV.open('r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                done.add(row['date'])
    return done


def parse_max(html: str):
    if THROTTLE_PHRASE in html:
        return 'throttled', None, 0

    m = PRE_RE.search(html)
    if not m:
        return 'no_data', None, 0

    text = TAG_RE.sub('', m.group(1))
    vals = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('PARM') or line.startswith('English Units'):
            continue
        parts = [p.strip() for p in line.split(',')]
        if len(parts) < 11:
            continue
        if NUM_RE.match(parts[10]):
            vals.append(float(parts[10]))

    if not vals:
        return 'no_valid_snow', None, 0
    return 'ok', max(vals), len(vals)


def fetch_one(day: dt.date):
    params = {
        'stn': STATION,
        'unit': 0,
        'time': 'LOCAL',
        'day1': f'{day.day:02d}',
        'month1': f'{day.month:02d}',
        'year1': str(day.year),
        'hour1': 15,
        'output': 'csv',
    }
    url = f"{BASE_URL}?{urlencode(params)}"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urlopen(req, timeout=25) as resp:
        html = resp.read().decode('utf-8', errors='replace')

    status, max_snow, samples = parse_max(html)
    return {
        'date': day.isoformat(),
        'year': day.year,
        'month': day.month,
        'day': day.day,
        'max_snow_in': max_snow,
        'sample_count': samples,
        'status': status,
        'source_url': url,
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    done = load_existing()
    needs_header = not OUT_CSV.exists()

    with OUT_CSV.open('a', encoding='utf-8', newline='') as f:
        fields = ['date', 'year', 'month', 'day', 'max_snow_in', 'sample_count', 'status', 'source_url']
        writer = csv.DictWriter(f, fieldnames=fields)
        if needs_header:
            writer.writeheader()

        count = 0
        for day in daterange(START, END):
            if day.isoformat() in done:
                continue

            try:
                row = fetch_one(day)
            except Exception as exc:  # noqa: BLE001
                row = {
                    'date': day.isoformat(),
                    'year': day.year,
                    'month': day.month,
                    'day': day.day,
                    'max_snow_in': None,
                    'sample_count': 0,
                    'status': f'error:{type(exc).__name__}',
                    'source_url': BASE_URL,
                }

            if row['status'] == 'throttled':
                print(f"Throttle detected at {day.isoformat()}. Stopping safely; rerun later to resume.")
                break

            writer.writerow(row)
            f.flush()
            count += 1

            if count % 50 == 0:
                print(f"Added {count} new rows this run...")

            time.sleep(1.2)

    print(f"Done. Consolidated CSV at {OUT_CSV}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
