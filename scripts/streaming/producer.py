"""
Stream NYC TLC Green taxi Parquet rows to Kafka (Redpanda), one row per interval.

Default file: ``green_tripdata_2021-01.parquet`` (repo root, cwd, or data/raw/nyc_taxi/).
Official download:
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet

Requires: Redpanda on localhost:9092, ``pip install -r requirements.txt``.
Use ``--fetch-missing`` to download the default file if it is not present.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Repository root (parent of ``scripts/``)
REPO_ROOT = Path(__file__).resolve().parents[2]

TOPIC = "taxi-topic"
DEFAULT_FILE = "green_tripdata_2021-01.parquet"
DEFAULT_SLEEP_SEC = 0.5

# NYC TLC public URL for the default month (Green taxi)
TLC_GREEN_2021_01_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet"
)


def _safe_int(val, default: int = 0) -> int:
    """Coerce to int; missing values become ``default``."""
    if pd.isna(val):
        return default
    return int(val)


def _safe_float(val) -> float | None:
    """Coerce to float; missing values become JSON ``null``."""
    if pd.isna(val):
        return None
    return float(val)


def row_to_payload(row: pd.Series) -> dict:
    """Map one Green-taxi Parquet row to a JSON-serializable dict."""
    return {
        "vendor_id": _safe_int(row["VendorID"], 0),
        "lpep_pickup_datetime": str(row["lpep_pickup_datetime"]),
        "lpep_dropoff_datetime": str(row["lpep_dropoff_datetime"]),
        "passenger_count": _safe_int(row["passenger_count"], 0),
        "trip_distance": _safe_float(row["trip_distance"]),
        "total_amount": _safe_float(row["total_amount"]),
    }


def resolve_parquet_path(user_path: Path) -> Path | None:
    """
    Return an existing path to the Parquet file, or None.

    Checks: absolute path, cwd-relative, repo root, data/raw/nyc_taxi/.
    """
    candidates: list[Path] = []
    if user_path.is_absolute():
        candidates.append(user_path)
    else:
        candidates.append(Path.cwd() / user_path)
        candidates.append(REPO_ROOT / user_path)
        candidates.append(REPO_ROOT / "data" / "raw" / "nyc_taxi" / user_path.name)

    seen: set[Path] = set()
    for p in candidates:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        if rp.is_file():
            return rp
    return None


def download_parquet(url: str, dest: Path) -> None:
    """Download Parquet from ``url`` to ``dest`` (stdlib only)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading:\n  {url}\n→ {dest}", file=sys.stderr)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "nyc-taxi-pipeline-analytics/scripts/streaming/producer.py"},
    )
    with urllib.request.urlopen(req, timeout=600) as resp:
        dest.write_bytes(resp.read())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Publish Green taxi Parquet rows to Kafka.")
    p.add_argument(
        "--file",
        "-f",
        type=Path,
        default=Path(DEFAULT_FILE),
        help=f"Path to Parquet file (default: {DEFAULT_FILE})",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP_SEC,
        help=f"Seconds between rows (default: {DEFAULT_SLEEP_SEC})",
    )
    p.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Stop after N rows (default: stream entire file)",
    )
    p.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap server (default: localhost:9092)",
    )
    p.add_argument(
        "--fetch-missing",
        action="store_true",
        help=(
            "If the Parquet file is not found, download the default "
            "green_tripdata_2021-01.parquet from NYC TLC into the repo root."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    path = resolve_parquet_path(args.file)

    if path is None and args.fetch_missing and args.file.name == Path(DEFAULT_FILE).name:
        dest = REPO_ROOT / DEFAULT_FILE
        try:
            download_parquet(TLC_GREEN_2021_01_URL, dest)
        except OSError as e:
            print(f"Download failed: {e}", file=sys.stderr)
            return 1
        path = dest.resolve() if dest.is_file() else None

    if path is None:
        print(
            f"File not found: {args.file}\n"
            f"  Tried: cwd, repo root ({REPO_ROOT}), "
            f"and data/raw/nyc_taxi/{args.file.name}\n"
            "  Fix: place the Parquet in repo root or data/raw/nyc_taxi/, pass -f path, or run:\n"
            "    python scripts/streaming/producer.py --fetch-missing\n"
            "  Manual URL:\n"
            f"    {TLC_GREEN_2021_01_URL}",
            file=sys.stderr,
        )
        return 1

    try:
        producer = KafkaProducer(
            bootstrap_servers=[args.bootstrap],
            request_timeout_ms=10_000,
            api_version_auto_timeout_ms=10_000,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except KafkaError as e:
        print(
            f"Cannot connect to Kafka at {args.bootstrap!r}: {e}\n"
            "  Start Redpanda, e.g. from repo root: docker compose up -d redpanda",
            file=sys.stderr,
        )
        return 1

    try:
        df = pd.read_parquet(path, engine="pyarrow")
    except OSError as e:
        print(f"Cannot read Parquet {path}: {e}", file=sys.stderr)
        return 1

    required = {
        "VendorID",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "total_amount",
    }
    missing = required - set(df.columns)
    if missing:
        print(f"Parquet missing columns: {sorted(missing)}", file=sys.stderr)
        return 1

    print(f"Streaming {path.name} to topic {TOPIC!r} ({len(df)} rows)...")

    try:
        for i, (_, row) in enumerate(df.iterrows()):
            if args.max_rows is not None and i >= args.max_rows:
                break
            data = row_to_payload(row)
            try:
                producer.send(TOPIC, value=data)
            except KafkaError as e:
                print(f"Send failed at row {i}: {e}", file=sys.stderr)
                return 1
            if i % 10 == 0:
                print(f"[{i}] sent pickup={data['lpep_pickup_datetime']!r}")
            time.sleep(args.sleep)
    except KeyboardInterrupt:
        print("\nStopped by user (Ctrl+C).", file=sys.stderr)
    finally:
        producer.flush()
        print("Producer closed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
