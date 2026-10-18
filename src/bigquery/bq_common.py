"""
Shared settings for the BigQuery scripts (run on the host, not in Airflow).

Environment variables (same names as the `bigquery` target in dbt/profiles.yml):
  BQ_PROJECT   Google Cloud project ID
  BQ_KEYFILE   optional: path to a service-account JSON key (never committed). If unset, the scripts
               use your own Google sign-in (Application Default Credentials from
               `gcloud auth application-default login`). Many accounts now block key creation by policy.
  BQ_DATASET   dataset prefix, default "retail"
  BQ_LOCATION  dataset location, default "US"
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_TABLE = "retail_transactions"


def bq_settings():
    settings = {
        "project": os.environ.get("BQ_PROJECT", ""),
        "keyfile": os.environ.get("BQ_KEYFILE", ""),
        "dataset": os.environ.get("BQ_DATASET", "retail"),
        "location": os.environ.get("BQ_LOCATION", "US"),
    }
    if not settings["project"]:
        sys.exit("Set BQ_PROJECT first (see README_PIPELINE.md, 'Cloud warehouse (BigQuery)').")
    if settings["keyfile"] and not Path(settings["keyfile"]).is_file():
        sys.exit("BQ_KEYFILE is set but does not point to an existing file.")
    return settings


def dataset_name(settings, layer):
    """BigQuery dataset for a layer: raw -> retail_raw, warehouse -> retail_warehouse, ..."""
    return f"{settings['dataset']}_{layer}"


def bq_client(settings):
    from google.cloud import bigquery

    if settings["keyfile"]:
        return bigquery.Client.from_service_account_json(
            settings["keyfile"], project=settings["project"], location=settings["location"]
        )
    # Your own Google sign-in (gcloud auth application-default login); no key file needed.
    return bigquery.Client(project=settings["project"], location=settings["location"])
