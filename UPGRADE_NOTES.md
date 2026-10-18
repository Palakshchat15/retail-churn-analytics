# Upgrade notes: Google BigQuery as a second dbt target (2026-10-17)

Status: **run and verified 2026-10-18.** Postgres: DAG success, `outputs/`
byte-identical, dbt test 58/58. BigQuery: 1,067,371 rows loaded, dbt run 11/11,
dbt test 58/58, parity 46/46 checks. Auth is your own Google sign-in (gcloud
ADC, dbt method `oauth`) because the account blocks service-account keys, so
`BQ_KEYFILE` is optional. The fixes made during the run (BigQuery casts, a
mode/max ranking CTE in `int_product_agg`, a stale dbt partial-parse cache) are
in `README_PIPELINE.md`, section "Cloud warehouse (BigQuery)". The sections
below are the original pre-run plan.

## What was added
- A `bigquery` output in the dbt profile (free sandbox, service-account key
  file from an env var). The `dev` (Postgres) output and the default target are
  unchanged.
- Portable models: the PostgreSQL-only SQL now goes through dbt cross-database
  macros (`dbt.bool_or`, `dbt.hash`, `dbt.concat`) or `adapter.dispatch`
  macros with `postgres__` and `bigquery__` versions. The `postgres__` versions
  render exactly the SQL the models had before (checked by rendering every
  model and test for both adapters with a stand-in for dbt's Jinja context).
- A raw loader for BigQuery: one batch load job from a local gzip CSV, values
  prepared exactly like the Postgres loader (same chunks, same id order, same
  customer_id and text conversions, price rounded like NUMERIC(12, 4)).
- A parity script: row counts, revenue and order totals, RFM sums and segment
  sizes, ABC class sizes, churn-rate inputs, and row-level diffs (RFM scores,
  customer country, product description, ABC class), written to
  `results/bigquery_parity.csv`.
- Host-only requirements (`requirements-bigquery.txt`); the Airflow image is
  untouched.

## Files changed
New:
- `dbt/retail_dbt/macros/cross_db.sql`: `retail_cast`, `filtered_agg`,
  `mode_where`, `elapsed_days`, `month_start`, `starts_with_literal`
- `src/bigquery/bq_common.py`, `src/bigquery/load_raw_to_bigquery.py`,
  `src/bigquery/verify_bigquery_parity.py`
- `requirements-bigquery.txt`, `UPGRADE_NOTES.md`

Changed:
- `dbt/profiles.yml`: added the `bigquery` output
- `dbt/retail_dbt/macros/generate_schema_name.sql`: dispatches; Postgres
  behaviour unchanged, BigQuery prefixes datasets (`retail_staging`, ...)
- `dbt/retail_dbt/models/staging/sources.yml`: raw schema is `raw` on Postgres,
  `<BQ_DATASET>_raw` on BigQuery
- Models using macros instead of Postgres-only syntax:
  `stg_retail_transactions`, `int_customer_orders`, `int_customer_rfm_base`,
  `int_monthly_country_sales`, `int_product_agg`, `fact_orders`,
  `mart_customer_rfm`
- `dbt/retail_dbt/tests/assert_rfm_ties_share_scores.sql`: `::text` via `retail_cast`
- `dbt/retail_dbt/models/marts/schema.yml`: `quote: false` on the three integer
  `accepted_values` tests (r/f/m_score)
- `README_PIPELINE.md`: new section "Cloud warehouse (BigQuery)", two lines in
  "Folder structure"
- `.gitignore`: key files (`secrets/`, `*service-account*.json`, `gcp-key*.json`),
  `.venv*/`, `dbt/*/target_bq/`

Unchanged: the DAG, docker-compose.yml, the Airflow Dockerfile,
requirements-airflow.txt, the Postgres loader, the model, dashboards and all
existing results.

## How to run (in order)
One-time setup (owner): Google Cloud project in sandbox mode, BigQuery API
enabled, service account with BigQuery Data Editor + BigQuery Job User, JSON
key saved outside the repo, Python 3.11 installed. Details in the README section.

PowerShell, from `retail_analytics_project`:
```powershell
py -3.11 -m venv .venv-bq
.venv-bq\Scripts\python -m pip install -r requirements-bigquery.txt
$env:BQ_PROJECT  = "<your-project-id>"
$env:BQ_KEYFILE  = "C:\Users\<you>\.gcp\retail-dbt.json"
$env:BQ_LOCATION = "US"
$env:DBT_PROFILES_DIR = "$PWD\dbt"
.venv-bq\Scripts\python src\bigquery\load_raw_to_bigquery.py
cd dbt\retail_dbt
..\..\.venv-bq\Scripts\dbt debug --target bigquery
..\..\.venv-bq\Scripts\dbt run   --target bigquery --target-path target_bq
..\..\.venv-bq\Scripts\dbt test  --target bigquery --target-path target_bq
cd ..\..
# Postgres must be up with a finished DAG run (docker compose up -d postgres)
.venv-bq\Scripts\python src\bigquery\verify_bigquery_parity.py
```

## What to verify
1. **Postgres unchanged (do this first).** Run the DAG once as usual. Expect
   `dbt run` 11/11, `dbt test` 58/58, and the same mart row counts and model
   metrics as the README (e.g. 5,878 RFM customers, AUC 0.749).
   `outputs/model_metrics.json` should be byte-identical to the current one.
2. Loader logs `1067371 rows` and no row-count mismatch.
3. `dbt debug --target bigquery` OK; `dbt run` 11/11 and `dbt test` 58/58 on BigQuery.
4. `results/bigquery_parity.csv`: every row `match = True`. The only expected
   possible exception is `rows_differing.mart_product_performance (abc_class)`
   (row_number() over tied net_revenue has no tie-breaker).
5. Fill in the "Results (not run yet)" table in `README_PIPELINE.md` from the
   real outputs.

## Expected run time
- Python 3.11 venv + pip install: about 3-5 min.
- Loader: about 20-30 s to prepare the 1.07M-row file (measured locally for the
  preparation step only), plus the upload of about 20 MB and the load job,
  likely 1-2 min.
- `dbt run` + `dbt test` on BigQuery: a few minutes (each query has some seconds
  of overhead).
- Parity script: under a minute.
Memory: small (the loader streams 50k-row chunks); fine on the 8 GB laptop
with Docker running only Postgres.

## Risks
- **Untested on BigQuery.** The SQL was checked by rendering only. Most likely
  points to fail first: the `mode_where` BigQuery form (a scalar subquery over
  `unnest(array_agg(...))` inside a GROUP BY query), and the parameterised
  casts `cast(x as numeric(12, 4))`. Both are documented BigQuery features.
- **Postgres SQL is unchanged except two harmless spots:** `order_key` is now
  `md5(cast(customer_id || '-' || invoice as <text type>))` (dbt.hash adds the
  cast; same hash values), and the integer `accepted_values` tests compare to
  `1, 2, ...` instead of `'1', '2', ...` (same result).
- **Collation.** Postgres (en_US.utf8) and BigQuery (byte order) sort text
  differently. This matters only for `mode()` ties on product descriptions.
  The BigQuery tie-break imitates en_US; checked offline on the 4 tied products
  in this data against the last Postgres export (`data/processed/dim_product.csv`),
  all 4 match. `max(country)` for the 13 customers with two countries gives the
  same answer under both orders.
- **Numeric types.** Ratios use BIGNUMERIC on BigQuery (38 decimals before
  `round(..., 4)`); Postgres numeric uses at least 16 significant digits. A
  difference would need a value within 1e-16 of a rounding boundary.
- **dbt-core 1.7 needs Python 3.11 or lower.** The laptop currently has only
  Python 3.14; install 3.11 for the BigQuery venv (or `uv venv --python 3.11 .venv-bq`).
- **Sandbox expiry.** Tables vanish after 60 days; re-run the loader and dbt.
- **Key file.** Gitignored patterns cover `secrets/` and common key names, but a
  key saved elsewhere inside the repo under another name would not be ignored.
  Keep it outside the project folder.
