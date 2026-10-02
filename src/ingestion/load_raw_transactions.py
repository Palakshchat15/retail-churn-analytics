"""
Chunked ingestion of the UCI Online Retail II combined CSV into
raw.retail_transactions in Postgres.

Handles ~1.07M rows in chunks to keep memory bounded, matching the
architecture pattern used in the sibling sales-forecasting project.
"""
import argparse
import logging
import os
import time

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CSV = "/opt/airflow/data/raw/online_retail_ii.csv"

COLUMNS = [
    "invoice", "stock_code", "description", "quantity",
    "invoice_date", "price", "customer_id", "country", "source_sheet",
]

INSERT_SQL = f"""
    INSERT INTO raw.retail_transactions
        ({", ".join(COLUMNS)})
    VALUES %s
"""


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "retail_user"),
        password=os.environ.get("POSTGRES_PASSWORD", "retail_pass"),
        dbname=os.environ.get("POSTGRES_DB", "retail_analytics"),
    )


def load(csv_path: str, chunksize: int = 50_000, truncate: bool = True):
    start = time.time()
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    if truncate:
        logger.info("Truncating raw.retail_transactions before load")
        cur.execute("TRUNCATE TABLE raw.retail_transactions RESTART IDENTITY")
        conn.commit()

    total_rows = 0
    rename_map = {
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

    for chunk_idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize)):
        chunk = chunk.rename(columns=rename_map)
        chunk["customer_id"] = chunk["customer_id"].apply(
            lambda v: None if pd.isna(v) else str(int(v)) if isinstance(v, float) else str(v)
        )
        chunk["invoice_date"] = pd.to_datetime(chunk["invoice_date"])
        chunk = chunk[COLUMNS]

        records = [tuple(x) for x in chunk.to_numpy()]
        # Replace pandas NaT/NaN with None for psycopg2
        cleaned = []
        for row in records:
            cleaned.append(tuple(None if (isinstance(v, float) and pd.isna(v)) else v for v in row))

        execute_values(cur, INSERT_SQL, cleaned, page_size=chunksize)
        conn.commit()
        total_rows += len(chunk)
        logger.info("Loaded chunk %s: %s rows (cumulative %s)", chunk_idx, len(chunk), total_rows)

    cur.close()
    conn.close()
    elapsed = time.time() - start
    logger.info("Done. Total rows loaded: %s in %.1fs", total_rows, elapsed)
    return total_rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=DEFAULT_CSV)
    parser.add_argument("--chunksize", type=int, default=50_000)
    parser.add_argument("--no-truncate", action="store_true")
    args = parser.parse_args()

    n = load(args.csv, chunksize=args.chunksize, truncate=not args.no_truncate)
    print(f"Loaded {n} rows into raw.retail_transactions")
