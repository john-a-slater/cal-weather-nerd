#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path

INPUT = Path('/Users/slater/Desktop/CSSL.csv')
OUT_DIR = Path('/Users/slater/Desktop/Website/data')
OUT_CSV = OUT_DIR / 'cssl_daily_snow_depth_2020_to_current.csv'
OUT_JSON = OUT_DIR / 'cssl_daily_snow_depth_2020_to_current.json'


def to_float(value: str):
    v = (value or '').strip()
    if v == '':
        return None
    try:
        return float(v)
    except ValueError:
        return None


def main() -> int:
    if not INPUT.exists():
        raise SystemExit(f'Input not found: {INPUT}')

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    with INPUT.open('r', encoding='utf-8', newline='') as f:
        filtered = (line for line in f if not line.startswith('#'))
        reader = csv.DictReader(filtered)

        for row in reader:
            date = (row.get('Date') or '').strip()
            if not date:
                continue

            snow = to_float(row.get('Snow Depth (in) Start of Day Values') or '')
            y, m, d = date.split('-')
            rows.append(
                {
                    'date': date,
                    'year': int(y),
                    'month': int(m),
                    'day': int(d),
                    'max_snow_in': snow,
                    'sample_count': 1 if snow is not None else 0,
                    'status': 'ok' if snow is not None else 'no_data',
                    'source_file': INPUT.name,
                }
            )

    rows.sort(key=lambda r: r['date'])

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
