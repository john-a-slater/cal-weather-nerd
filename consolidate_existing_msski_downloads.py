#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

ROOT = Path('/Users/slater/Desktop/Website')
OUT_DIR = ROOT / 'data'
CSV_OUT = OUT_DIR / 'msski_daily_max_from_local_downloads.csv'
JSON_OUT = OUT_DIR / 'msski_daily_max_from_local_downloads.json'
FILE_RE = re.compile(r'^MSSKI_(\d{2})-(\d{2})-(\d{4})\.csv$')
NUM_RE = re.compile(r'^-?\d+(?:\.\d+)?$')


def parse_max(path: Path):
    max_snow = None
    samples = 0
    with path.open('r', encoding='utf-8', errors='replace') as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('English Units') or line.startswith('PARM'):
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 11:
                continue
            snow = parts[10]
            if NUM_RE.match(snow):
                val = float(snow)
                samples += 1
                if max_snow is None or val > max_snow:
                    max_snow = val
    return max_snow, samples


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []

    for path in sorted(ROOT.glob('MSSKI_*.csv')):
        m = FILE_RE.match(path.name)
        if not m:
            continue
        mm, dd, yyyy = m.groups()
        date = f'{yyyy}-{mm}-{dd}'
        max_snow, samples = parse_max(path)
        status = 'ok' if max_snow is not None else 'no_data'
        rows.append({
            'date': date,
            'year': int(yyyy),
            'month': int(mm),
            'day': int(dd),
            'max_snow_in': max_snow,
            'sample_count': samples,
            'status': status,
            'source_file': path.name,
        })

    rows.sort(key=lambda r: r['date'])

    with CSV_OUT.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['date', 'year', 'month', 'day', 'max_snow_in', 'sample_count', 'status', 'source_file'],
        )
        writer.writeheader()
        writer.writerows(rows)

    with JSON_OUT.open('w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=True, indent=2)

    print(f'Saved {CSV_OUT}')
    print(f'Saved {JSON_OUT}')
    print(f'Rows: {len(rows)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
