#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

INPUT = Path('/Users/slater/Downloads/MSSKI.2026-04-18.csv')
OUT_DIR = Path('/Users/slater/Desktop/Website/data')
OUT_CSV = OUT_DIR / 'msski_daily_max_snow_2020_to_current.csv'
OUT_JSON = OUT_DIR / 'msski_daily_max_snow_2020_to_current.json'


def parse_float(val: str):
    val = (val or '').strip()
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def main() -> int:
    if not INPUT.exists():
        raise SystemExit(f'Input not found: {INPUT}')

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    daily_vals: dict[str, list[float]] = defaultdict(list)

    with INPUT.open('r', encoding='utf-8', newline='') as f:
        # Skip metadata comment lines.
        filtered = (line for line in f if not line.startswith('#'))
        reader = csv.DictReader(filtered)

        for row in reader:
            dt_raw = (row.get('Date_Time') or '').strip()
            snow = parse_float(row.get('snow_depth_set_1') or '')
            if not dt_raw:
                continue

            # Supports timestamps like 2020-10-01T10:00:00-0700
            try:
                dt_obj = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M:%S%z')
            except ValueError:
                # Ignore non-data rows like unit rows.
                continue

            if snow is not None:
                daily_vals[dt_obj.date().isoformat()].append(snow)

    rows = []
    if daily_vals:
        start = min(daily_vals)
        end = max(daily_vals)
    else:
        start = end = None

    for date_key in sorted(daily_vals.keys()):
        vals = daily_vals[date_key]
        y, m, d = date_key.split('-')
        rows.append(
            {
                'date': date_key,
                'year': int(y),
                'month': int(m),
                'day': int(d),
                'max_snow_in': round(max(vals), 2) if vals else None,
                'sample_count': len(vals),
                'status': 'ok' if vals else 'no_data',
                'source_file': INPUT.name,
            }
        )

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
    if start and end:
        print(f'Date range: {start} -> {end}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
