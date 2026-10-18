"""
Load data/raw/online_retail_ii.csv into BigQuery as
<BQ_PROJECT>.<BQ_DATASET>_raw.retail_transactions (default retail_raw), the
BigQuery twin of Postgres raw.retail_transactions.

Runs on the host (requirements-bigquery.txt), not in Airflow.

Free BigQuery sandbox, so:
  * ONE batch load job from a local file (load jobs are free). No streaming
    inserts and no DML: the table is replaced with WRITE_TRUNCATE, which is a
    load-job setting, not an UPDATE/DELETE statement.
  * Sandbox tables expire 60 days after creation. Re-run this script (and
    `dbt run --target bigquery`) after that; nothing else needs to change.

Values are prepared exactly the way src/ingestion/load_raw_transactions.py
puts them into Postgres, so both warehouses start from the same rows:
  * same pandas read_csv defaults and the same 50,000-row chunks (pandas infers
    column types per chunk, so the chunk size is part of the parsing);
  * id = 1..N in file order (Postgres: BIGSERIAL after TRUNCATE ... RESTART IDENTITY);
  * customer_id: 13085.0 -> "13085" (same rule as the Postgres loader);
  * text columns: what Postgres stores when psycopg2 sends a Python value to a
    TEXT column (str as is, int -> "489434", float -> repr);
  * price: rounded to 4 decimals half away from zero, like NUMERIC(12, 4);
  * invoice_date: DATETIME (Postgres TIMESTAMP has no time zone either).

Usage (from the project root, with BQ_PROJECT and BQ_KEYFILE set):
    python src/bigquery/load_raw_to_bigquery.py
"""
import argparse
import csv
import gzip
import logging
import os
import tempfile
import time
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd

from bq_common import PROJECT_ROOT, RAW_TABLE, bq_client, bq_settings, dataset_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CSV = PROJECT_ROOT / "data" / "raw" / "online_retail_ii.csv"
CHUNKSIZE = 50_000  # must equal the Postgres loader's default, see module docstring

RENAME_MAP = {
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "price",
    "Customer ID": "customer_id",
    "Country": "country",
    "source_sheet": "source_sheet",
}
TEXT_COLS = ["invoice", "stock_code", "description", "country", "source_sheet"]
OUT_COLS = ["id", "invoice", "stock_code", "description", "quantity",
            "invoice_date", "price", "customer_id", "country", "source_sheet", "loaded_at"]


def bq_schema():
    from google.cloud import bigquery

    f = bigquery.SchemaField
    return [
        f("id", "INT64", mode="REQUIRED"),
        f("invoice", "STRING"),
        f("stock_code", "STRING"),
        f("description", "STRING"),
        f("quantity", "INT64"),
        f("invoice_date", "DATETIME"),
        f("price", "NUMERIC"),
        f("customer_id", "STRING"),
        f("country", "STRING"),
        f("source_sheet", "STRING"),
        f("loaded_at", "TIMESTAMP"),
    ]


def is_missing(v):
    return v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NaT


def as_pg_text(v):
    if is_missing(v):
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return repr(v)
    return str(v)


def as_pg_int(v):
    if is_missing(v):
        return None
    if isinstance(v, float):
        return str(int(Decimal(repr(v)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)))
    return str(int(v))


def as_pg_numeric_4(v):
    if is_missing(v):
        return None
    return str(Decimal(repr(float(v))).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def as_customer_id(v):
    # identical to the Postgres loader's rule
    return None if pd.isna(v) else str(int(v)) if isinstance(v, float) else str(v)


def prepare_file(csv_path, out_path):
    """Write a gzip CSV in the BigQuery schema's column order. Returns the row count."""
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # no zone = UTC in BigQuery
    next_id = 1
    with gzip.open(out_path, "wt", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(OUT_COLS)
        for chunk_idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNKSIZE)):
            chunk = chunk.rename(columns=RENAME_MAP)
            chunk["customer_id"] = chunk["customer_id"].apply(as_customer_id)
            chunk["invoice_date"] = pd.to_datetime(chunk["invoice_date"])
            records = chunk[list(RENAME_MAP.values())].to_dict("records")
            for rec in records:
                ts = rec["invoice_date"]
                writer.writerow([
                    next_id,
                    as_pg_text(rec["invoice"]),
                    as_pg_text(rec["stock_code"]),
                    as_pg_text(rec["description"]),
                    as_pg_int(rec["quantity"]),
                    None if is_missing(ts) else ts.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    as_pg_numeric_4(rec["price"]),
                    rec["customer_id"],
                    as_pg_text(rec["country"]),
                    as_pg_text(rec["source_sheet"]),
                    loaded_at,
                ])
                next_id += 1
            logger.info("Prepared chunk %s (cumulative %s rows)", chunk_idx, next_id - 1)
    return next_id - 1


def load(csv_path, keep_file=False):
    from google.cloud import bigquery

    settings = bq_settings()
    client = bq_client(settings)
    ds_id = f"{settings['project']}.{dataset_name(settings, 'raw')}"
    table_id = f"{ds_id}.{RAW_TABLE}"

    start = time.time()
    tmp_dir = tempfile.mkdtemp(prefix="retail_bq_")
    gz_path = os.path.join(tmp_dir, "retail_transactions.csv.gz")
    n_rows = prepare_file(csv_path, gz_path)
    logger.info("Prepared %s rows -> %s (%.1f MB gzip) in %.1fs",
                n_rows, gz_path, os.path.getsize(gz_path) / 1e6, time.time() - start)

    dataset = bigquery.Dataset(ds_id)
    dataset.location = settings["location"]
    dataset.description = "Retail analytics raw layer (twin of Postgres schema raw)"
    client.create_dataset(dataset, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=bq_schema(),
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        max_bad_records=0,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    logger.info("Starting load job into %s (location %s)", table_id, settings["location"])
    with open(gz_path, "rb") as fh:
        job = client.load_table_from_file(fh, table_id, job_config=job_config,
                                          location=settings["location"])
    job.result()  # raises on failure

    table = client.get_table(table_id)
    logger.info("Load job %s done: %s rows in %s (%.1fs total)",
                job.job_id, table.num_rows, table_id, time.time() - start)
    if table.expires:
        logger.info("Table expires %s (sandbox limit: 60 days). Re-run this script after that.",
                    table.expires.isoformat())

    if keep_file:
        logger.info("Kept prepared file: %s", gz_path)
    else:
        os.remove(gz_path)
        os.rmdir(tmp_dir)

    if table.num_rows != n_rows:
        raise SystemExit(f"Row count mismatch: prepared {n_rows}, BigQuery has {table.num_rows}")
    return n_rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", default=str(DEFAULT_CSV))
    parser.add_argument("--keep-file", action="store_true", help="keep the prepared .csv.gz for inspection")
    args = parser.parse_args()
    n = load(args.csv, keep_file=args.keep_file)
    print(f"Loaded {n} rows into BigQuery raw.{RAW_TABLE}")
