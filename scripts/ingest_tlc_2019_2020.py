"""
Download NYC TLC Yellow/Green Parquet (2019-01 .. 2020-12), upload to GCS,
load into BigQuery.

Example (full pipeline; needs network + GCP creds):
  cd DE/Project
  python scripts/ingest_tlc_2019_2020.py

Steps only:
  python scripts/ingest_tlc_2019_2020.py --download-only
  python scripts/ingest_tlc_2019_2020.py --upload-only
  python scripts/ingest_tlc_2019_2020.py --bq-only

Official file pattern:
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
  https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_YYYY-MM.parquet

BigQuery output tables (dataset trips_data_all):
  yellow_tripdata_2019_2020
  green_tripdata_2019_2020

Loads use merged Parquet (PyArrow) for both colors — multi-file wildcard loads can
mis-parse timestamps when schemas differ across months.

After success, run: cd nyc_taxi_dbt && dbt seed && dbt run

Environment (optional; defaults match README placeholders — set for your GCP project):
  GCP_PROJECT_ID   BigQuery / GCS project (default: YOUR_GCP_PROJECT)
  GCS_BUCKET       Lake bucket name (default: YOUR_GCS_BUCKET)
  BQ_DATASET       Dataset for trip tables (default: trips_data_all)
  GCP_CREDS_PATH   Path to service account JSON
  (default: <repo>/credentials/gcp-service-account.json)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_YELLOW = PROJECT_ROOT / "data" / "raw" / "nyc_taxi" / "yellow"
DATA_GREEN = PROJECT_ROOT / "data" / "raw" / "nyc_taxi" / "green"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
GCS_PREFIX_YELLOW = "raw/nyc_taxi/yellow"
GCS_PREFIX_GREEN = "raw/nyc_taxi/green"

# Align with README placeholders; override via env for real runs (see module docstring).
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "YOUR_GCP_PROJECT").strip()
BUCKET = os.environ.get("GCS_BUCKET", "YOUR_GCS_BUCKET").strip()
DATASET = os.environ.get("BQ_DATASET", "trips_data_all").strip()
TABLE_YELLOW = "yellow_tripdata_2019_2020"
TABLE_GREEN = "green_tripdata_2019_2020"

_creds_override = os.environ.get("GCP_CREDS_PATH", "").strip()
CREDS = (
    Path(_creds_override)
    if _creds_override
    else PROJECT_ROOT / "credentials" / "gcp-service-account.json"
)


def ym_list(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> list[str]:
    out: list[str] = []
    y, m = start_year, start_month
    while True:
        if (y, m) > (end_year, end_month):
            break
        out.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def download_one(url: str, dest: Path, retries: int = 3) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 1_000_000:
        print(f"  skip (exists): {dest.name}")
        return True
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0 (ingest_tlc_2019_2020)"}
            )
            with urllib.request.urlopen(req, timeout=300) as r:
                dest.write_bytes(r.read())
            print(f"  ok: {dest.name}")
            return True
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            print(f"  attempt {attempt + 1}/{retries} failed {dest.name}: {e}")
            time.sleep(2 * (attempt + 1))
    return False


def step_download(yms: list[str]) -> bool:
    ok = True
    for ym in yms:
        yurl = f"{BASE_URL}/yellow_tripdata_{ym}.parquet"
        gurl = f"{BASE_URL}/green_tripdata_{ym}.parquet"
        if not download_one(yurl, DATA_YELLOW / f"yellow_tripdata_{ym}.parquet"):
            ok = False
        if not download_one(gurl, DATA_GREEN / f"green_tripdata_{ym}.parquet"):
            ok = False
    return ok


def step_upload() -> bool:
    import os

    if not CREDS.is_file():
        print(f"Missing {CREDS}")
        return False
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CREDS)
    from google.cloud import storage

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET)

    for folder, prefix in (
        (DATA_YELLOW, GCS_PREFIX_YELLOW),
        (DATA_GREEN, GCS_PREFIX_GREEN),
    ):
        if not folder.is_dir():
            print(f"Missing folder: {folder}")
            return False
        for p in sorted(folder.glob("*.parquet")):
            blob = bucket.blob(f"{prefix}/{p.name}")
            # Large Parquet files need a generous timeout (default 60s often fails on
            # home networks).
            blob.upload_from_filename(str(p), timeout=900)
            print(f"  gcs: gs://{BUCKET}/{prefix}/{p.name}")
    return True


def _arrow_field_to_bq(field) -> "object":
    """Map one PyArrow field to google.cloud.bigquery.SchemaField (lazy import)."""
    import pyarrow as pa
    from google.cloud.bigquery import SchemaField

    t = field.type
    mode = "NULLABLE"
    if (
        pa.types.is_int64(t)
        or pa.types.is_int32(t)
        or pa.types.is_int16(t)
        or pa.types.is_int8(t)
    ):
        bq_type = "INT64"
    elif (
        pa.types.is_uint64(t)
        or pa.types.is_uint32(t)
        or pa.types.is_uint16(t)
        or pa.types.is_uint8(t)
    ):
        bq_type = "INT64"
    elif pa.types.is_float64(t) or pa.types.is_float32(t):
        bq_type = "FLOAT64"
    elif pa.types.is_boolean(t):
        bq_type = "BOOL"
    elif pa.types.is_string(t) or pa.types.is_large_string(t):
        bq_type = "STRING"
    elif pa.types.is_timestamp(t):
        bq_type = "TIMESTAMP"
    elif pa.types.is_date32(t):
        bq_type = "DATE"
    elif pa.types.is_binary(t) or pa.types.is_large_binary(t):
        bq_type = "BYTES"
    elif pa.types.is_decimal(t):
        bq_type = "NUMERIC"
    else:
        raise ValueError(f"Unsupported Arrow type for column {field.name!r}: {t}")

    return SchemaField(field.name, bq_type, mode=mode)


def arrow_schema_to_bq(arrow_schema) -> list:
    """Convert PyArrow schema to BigQuery schema (flat TLC Parquet only)."""
    out = []
    for i in range(len(arrow_schema)):
        out.append(_arrow_field_to_bq(arrow_schema.field(i)))
    return out


def _coerce_datetime_columns(merged, timestamp_cols: tuple[str, ...]):
    """Normalize TLC pickup/dropoff columns to TIMESTAMP(us) so BigQuery does not
    mis-read mixed Parquet types."""
    import pyarrow as pa
    import pyarrow.compute as pc

    target = pa.timestamp("us")

    def int_to_timestamp(col):
        # Try common physical encodings after schema unify (int/float → wrong buckets
        # in BQ if left as-is).
        for unit in ("ns", "us", "ms", "s"):
            try:
                ts = pc.cast(col, pa.timestamp(unit))
                return pc.cast(ts, target)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError):
                continue
        raise ValueError("Could not cast integer-like column to timestamp")

    for name in timestamp_cols:
        if name not in merged.column_names:
            continue
        idx = merged.schema.get_field_index(name)
        col = merged.column(name)
        if pa.types.is_timestamp(col.type):
            arr = pc.cast(col, target)
        elif pa.types.is_integer(col.type) or pa.types.is_floating(col.type):
            arr = int_to_timestamp(col)
        else:
            continue
        old_f = merged.schema.field(idx)
        merged = merged.set_column(
            idx, pa.field(old_f.name, target, nullable=old_f.nullable), arr
        )
    return merged


def _unify_parquet_schemas(paths: list[Path], pq_module) -> "object":
    """Build one Arrow schema from all Parquet footers (no full data load)."""
    import pyarrow as pa

    schemas = [pq_module.read_schema(p) for p in paths]
    try:
        unified = pa.unify_schemas(schemas)
    except Exception:
        unified = schemas[0]
        for s in schemas[1:]:
            unified = pa.unify_schemas([unified, s])
    return unified


def _schema_with_timestamp_us(schema, timestamp_cols: tuple[str, ...]):
    import pyarrow as pa

    fields = []
    for i in range(len(schema)):
        f = schema.field(i)
        if f.name in timestamp_cols:
            fields.append(pa.field(f.name, pa.timestamp("us"), nullable=f.nullable))
        else:
            fields.append(f)
    return pa.schema(fields)


def _align_table_to_schema(table, target_schema) -> "object":
    """Cast / pad columns so `table` matches `target_schema` by column name."""
    import pyarrow as pa
    import pyarrow.compute as pc

    n = table.num_rows
    cols: list = []
    names: list[str] = []
    for field in target_schema:
        name = field.name
        tgt = field.type
        if name not in table.column_names:
            cols.append(pa.nulls(n, type=tgt))
        else:
            src = table.column(name)
            if src.type != tgt:
                try:
                    cols.append(pc.cast(src, tgt))
                except Exception:
                    cols.append(pc.cast(src, tgt, safe=False))
            else:
                cols.append(src)
        names.append(name)
    return pa.Table.from_arrays(cols, schema=target_schema)


def merge_parquets_local(
    data_dir: Path,
    glob_pattern: str,
    merged_basename: str,
    label: str,
    timestamp_cols: tuple[str, ...] = (),
) -> Path:
    """Merge Parquet files with unified types. Streams one file at a time to avoid RAM
    spikes."""
    try:
        import pyarrow.parquet as pq
    except ImportError as e:
        raise RuntimeError("pip install pyarrow") from e

    paths = sorted(p for p in data_dir.glob(glob_pattern) if p.name != merged_basename)
    if not paths:
        raise FileNotFoundError(
            f"No parquet files matching {glob_pattern!r} under {data_dir}"
        )
    if len(paths) < 20:
        print(
            f"  warning: only {len(paths)} parquet file(s) - expected ~24 months for "
            "2019-01..2020-12; run --download-only with full range before --bq-only."
        )

    unified = _unify_parquet_schemas(paths, pq)
    if timestamp_cols:
        unified = _schema_with_timestamp_us(unified, timestamp_cols)

    out = data_dir / merged_basename
    writer = None

    def _open_writer(path_out, schema):
        try:
            return pq.ParquetWriter(
                path_out,
                schema,
                compression="snappy",
                version="2.6",
                coerce_timestamps="us",
            )
        except (TypeError, ValueError):
            return pq.ParquetWriter(
                path_out, schema, compression="snappy", version="2.6"
            )

    try:
        for i, path in enumerate(paths):
            t = pq.read_table(path)
            t = _align_table_to_schema(t, unified)
            if timestamp_cols:
                t = _coerce_datetime_columns(t, timestamp_cols)
            if i > 0 and writer is not None and t.schema != writer.schema:
                t = _align_table_to_schema(t, writer.schema)
            if writer is None:
                writer = _open_writer(out, t.schema)
            writer.write_table(t)
    finally:
        if writer is not None:
            writer.close()

    pick = (
        timestamp_cols[0]
        if timestamp_cols and timestamp_cols[0] in unified.names
        else None
    )
    if pick:
        import pyarrow.compute as pc

        try:
            pf = pq.ParquetFile(out)
            rg0 = pf.read_row_group(0, columns=[pick])
            y0 = pc.year(rg0.column(0))
            print(
                f"  {label} pickup_datetime sample year range (row group 0): "
                f"{pc.min(y0).as_py()} .. {pc.max(y0).as_py()}"
            )
        except Exception as e:
            print(f"  {label} pickup year sample skipped: {e}")

    mb = out.stat().st_size // 1_000_000
    print(f"  merged {len(paths)} {label} parquet files -> {out.name} ({mb} MB)")
    return out


def merge_yellow_parquets_local() -> Path:
    return merge_parquets_local(
        DATA_YELLOW,
        "yellow_tripdata_*.parquet",
        "yellow_tripdata_merged.parquet",
        "yellow",
        timestamp_cols=("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    )


def merge_green_parquets_local() -> Path:
    return merge_parquets_local(
        DATA_GREEN,
        "green_tripdata_*.parquet",
        "green_tripdata_merged.parquet",
        "green",
        timestamp_cols=("lpep_pickup_datetime", "lpep_dropoff_datetime"),
    )


def step_bigquery(*, skip_sanity: bool = False) -> bool:
    import os

    if not CREDS.is_file():
        print(f"Missing {CREDS}")
        return False
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CREDS)
    from google.cloud import bigquery
    import pyarrow.parquet as pq

    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = f"{PROJECT_ID}.{DATASET}"

    def load_merged_parquet(
        uri: str,
        table: str,
        partition_field: str,
        *,
        bq_schema: list | None = None,
    ) -> None:
        """Single merged Parquet — prefer explicit schema so BQ does not mis-map
        TIMESTAMP columns.

        Applies **daily** time partitioning on ``partition_field`` and **clustering**
        on ``VendorID``, ``PULocationID`` (NYC TLC Parquet column names) for
        filter/join-friendly scans.
        """
        table_id = f"{dataset_ref}.{table}"
        # Aligns with Kestra flows (gcs_to_bigquery*.yaml) and documented warehouse
        # design (partition + cluster).
        clustering_fields = ["VendorID", "PULocationID"]
        base_cfgs: list[bigquery.LoadJobConfig] = []
        if bq_schema:
            base_cfgs.append(
                bigquery.LoadJobConfig(
                    schema=bq_schema,
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    autodetect=False,
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=partition_field,
                    ),
                    clustering_fields=clustering_fields,
                )
            )
            base_cfgs.append(
                bigquery.LoadJobConfig(
                    schema=bq_schema,
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    autodetect=False,
                )
            )
        base_cfgs.extend(
            [
                bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    autodetect=True,
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=partition_field,
                    ),
                    clustering_fields=clustering_fields,
                ),
                bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    autodetect=True,
                ),
            ]
        )
        last_err: Exception | None = None
        for job_config in base_cfgs:
            try:
                load_job = client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )
                load_job.result()
                t = client.get_table(table_id)
                print(f"OK BigQuery: {table_id} rows={t.num_rows}")
                return
            except Exception as e:
                last_err = e
                print(f"  retry load ({table}): {e}")
        raise last_err if last_err else RuntimeError("merged parquet load failed")

    def validate_yellow_month_201903() -> None:
        """Fail fast if pickup timestamps are still wrong (wildcard-load symptom)."""
        q = f"""
        SELECT COUNT(*) AS c
        FROM `{dataset_ref}.{TABLE_YELLOW}`
        WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2019
          AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 3
        """
        rows = list(client.query(q).result())
        c = int(rows[0].c) if rows else 0
        if c < 500_000:
            raise RuntimeError(
                f"Yellow sanity check failed: only {c} rows in 2019-03 "
                "(expect ~millions). Re-download all monthly Parquet files, re-run "
                "merge + load."
            )
        print(f"  OK sanity: yellow 2019-03 rows={c}")

    def validate_green_month_201903() -> None:
        q = f"""
        SELECT COUNT(*) AS c
        FROM `{dataset_ref}.{TABLE_GREEN}`
        WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = 2019
          AND EXTRACT(MONTH FROM lpep_pickup_datetime) = 3
        """
        rows = list(client.query(q).result())
        c = int(rows[0].c) if rows else 0
        if c < 10_000:
            raise RuntimeError(
                f"Green sanity check failed: only {c} rows in 2019-03 "
                "(expect much more). Re-download monthly Parquet and re-run."
            )
        print(f"  OK sanity: green 2019-03 rows={c}")

    try:
        from google.cloud import storage

        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(CREDS))
        st = storage.Client(project=PROJECT_ID).bucket(BUCKET)

        yellow_merged = merge_yellow_parquets_local()
        try:
            yellow_bq_schema = arrow_schema_to_bq(pq.read_schema(yellow_merged))
        except (ValueError, TypeError) as e:
            print(
                f"  warning: could not build explicit BQ schema for yellow: {e}; "
                "using autodetect"
            )
            yellow_bq_schema = None

        yellow_blob = f"{GCS_PREFIX_YELLOW}/yellow_tripdata_merged.parquet"
        st.blob(yellow_blob).upload_from_filename(str(yellow_merged), timeout=900)
        print(f"  gcs: gs://{BUCKET}/{yellow_blob}")
        load_merged_parquet(
            f"gs://{BUCKET}/{yellow_blob}",
            TABLE_YELLOW,
            "tpep_pickup_datetime",
            bq_schema=yellow_bq_schema,
        )
        if not skip_sanity:
            validate_yellow_month_201903()

        green_merged = merge_green_parquets_local()
        try:
            green_bq_schema = arrow_schema_to_bq(pq.read_schema(green_merged))
        except (ValueError, TypeError) as e:
            print(
                f"  warning: could not build explicit BQ schema for green: {e}; "
                "using autodetect"
            )
            green_bq_schema = None

        green_blob = f"{GCS_PREFIX_GREEN}/green_tripdata_merged.parquet"
        st.blob(green_blob).upload_from_filename(str(green_merged), timeout=900)
        print(f"  gcs: gs://{BUCKET}/{green_blob}")
        load_merged_parquet(
            f"gs://{BUCKET}/{green_blob}",
            TABLE_GREEN,
            "lpep_pickup_datetime",
            bq_schema=green_bq_schema,
        )
        if not skip_sanity:
            validate_green_month_201903()
    except Exception as e:
        print(f"BigQuery load failed: {e}")
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-only", action="store_true")
    parser.add_argument("--upload-only", action="store_true")
    parser.add_argument("--bq-only", action="store_true")
    parser.add_argument("--start", default="2019-01", help="YYYY-MM")
    parser.add_argument("--end", default="2020-12", help="YYYY-MM")
    parser.add_argument(
        "--skip-sanity",
        action="store_true",
        help="Skip post-load March 2019 row-count checks (not recommended).",
    )
    args = parser.parse_args()

    sy, sm = map(int, args.start.split("-"))
    ey, em = map(int, args.end.split("-"))
    yms = ym_list(sy, sm, ey, em)
    print(f"Months: {len(yms)} (from {args.start} to {args.end})")

    if args.upload_only:
        sys.exit(0 if step_upload() else 1)
    if args.bq_only:
        sys.exit(0 if step_bigquery(skip_sanity=args.skip_sanity) else 1)
    if args.download_only:
        sys.exit(0 if step_download(yms) else 1)

    if not step_download(yms):
        print("Download had failures; fix network/URLs and re-run.")
        sys.exit(1)
    if not step_upload():
        sys.exit(1)
    if not step_bigquery(skip_sanity=args.skip_sanity):
        sys.exit(1)
    print("\nNext: update nyc_taxi_dbt models/schema.yml identifiers to")
    print(f"  yellow_tripdata: {TABLE_YELLOW}")
    print(f"  green_tripdata: {TABLE_GREEN}")
    print("Then: cd nyc_taxi_dbt && dbt seed && dbt run")


if __name__ == "__main__":
    main()
