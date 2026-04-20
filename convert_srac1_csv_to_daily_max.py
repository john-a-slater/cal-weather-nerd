#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

INPUT = Path('/Users/slater/Desktop/Website/data/SRAC1.2026-04-19.csv')
OUT_DIR = Path('/Users/slater/Desktop/Website/data')
OUT_CSV = OUT_DIR / 'srac1_daily_max_snow_2020_to_current.csv'
OUT_JSON = OUT_DIR / 'srac1_daily_max_snow_2020_to_current.json'
EXCLUDE_SNOW_VALUES = {202.0}


def parse_float(value: str):
    v = (value or '').strip()
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def main() -> int:
    if not INPUT.exists():
        raise SystemExit(f'Input not found: {INPUT}')

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    daily = defaultdict(list)
    with INPUT.open('r', encoding='utf-8', newline='') as f:
        filtered = (line for line in f if not line.startswith('#'))
        reader = csv.DictReader(filtered)

        for row in reader:
            dt_raw = (row.get('Date_Time') or '').strip()
            if not dt_raw:
                continue
            try:
                dt = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M:%S%z')
            except ValueError:
                continue
            snow = parse_float(row.get('snow_depth_set_1') or '')
            if snow is not None and snow not in EXCLUDE_SNOW_VALUES:
                daily[dt.date().isoformat()].append(snow)

    rows = []
    for date in sorted(daily.keys()):
        vals = daily[date]
        y, m, d = date.split('-')
        rows.append({
            'date': date,
            'year': int(y),
            'month': int(m),
            'day': int(d),
            'max_snow_in': round(max(vals), 2) if vals else None,
            'sample_count': len(vals),
            'status': 'ok' if vals else 'no_data',
            'source_file': INPUT.name,
        })

    with OUT_CSV.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['date', 'year', 'month', 'day', 'max_snow_in', 'sample_count', 'status', 'source_file'],
        )
        writer.writeheader()
        writer.writerows(rows)

    with OUT_JSON.open('w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=True, indent=2)

    print(f'Saved CSV: {OUT_CSV}')
    print(f'Saved JSON: {OUT_JSON}')
    print(f'Rows: {len(rows)}')
    if rows:
        print(f"Date range: {rows[0]['date']} -> {rows[-1]['date']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
