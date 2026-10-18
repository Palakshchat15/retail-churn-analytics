"""
Compare the PostgreSQL warehouse (Docker stack, localhost:5434) with the
BigQuery warehouse built by `dbt run --target bigquery`, and write
results/bigquery_parity.csv.

Checks:
  * row counts of raw, staging and all six marts;
  * key aggregates: revenue totals, order counts, RFM sums and segment sizes,
    ABC class sizes;
  * churn-rate inputs: scored customers and churned customers for the headline
    snapshot (feature window to 2011-06-09, active 2010-12-10..2011-06-09,
    outcome 2011-06-10..2011-12-09; same rules as train_churn_model.py). This
    is an exact-decimal SQL version, compared PG vs BQ; it is not meant to
    reproduce the model's float-based numbers to the last customer;
  * row-level: per-customer RFM scores and segment, customer country, product
    description and ABC class.

Runs on the host (requirements-bigquery.txt). Needs the Postgres container up
(`docker compose up -d postgres`) after a full DAG run, plus BQ_PROJECT and
BQ_KEYFILE. Postgres credentials come from POSTGRES_USER / POSTGRES_PASSWORD /
POSTGRES_DB, or, if unset, from the project's .env file (only those three keys
are read, nothing is printed). Host and port default to localhost:5434.

Usage (from the project root):
    python src/bigquery/verify_bigquery_parity.py
Exit code 0 = every check matches, 1 = at least one mismatch.
"""
import csv
import os
from decimal import Decimal

from bq_common import PROJECT_ROOT, bq_client, bq_settings, dataset_name

OUT_PATH = PROJECT_ROOT / "results" / "bigquery_parity.csv"
TOLERANCE = Decimal("0.0001")

MARTS = ["dim_customer", "dim_product", "fact_orders", "mart_customer_rfm",
         "mart_product_performance", "mart_monthly_sales_country"]
SEGMENTS = ["Champions", "Loyal", "Potential Loyalist", "At Risk", "Lost", "Others"]

# Headline churn snapshot (train_churn_model.py: HEADLINE_CUTOFF 2011-06-09, 6 + 6 months)
CHURN_TS = {
    "feature_start": "2009-12-01 00:00:00",
    "active_start": "2010-12-10 00:00:00",
    "feature_end": "2011-06-10 00:00:00",
    "outcome_end": "2011-12-10 00:00:00",
}


# ------------------------------------------------------------------ dialects
class Postgres:
    name = "postgres"

    def __init__(self):
        import psycopg2

        creds = pg_credentials()
        self.conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5434"),
            user=creds["POSTGRES_USER"], password=creds["POSTGRES_PASSWORD"],
            dbname=creds["POSTGRES_DB"],
        )

    def rel(self, layer, table):
        return f"{layer}.{table}"

    def ts(self, value):
        return f"timestamp '{value}'"

    def rows(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()]


class BigQuery:
    name = "bigquery"

    def __init__(self):
        self.settings = bq_settings()
        self.client = bq_client(self.settings)

    def rel(self, layer, table):
        return f"`{self.settings['project']}.{dataset_name(self.settings, layer)}.{table}`"

    def ts(self, value):
        return f"datetime '{value}'"

    def rows(self, sql):
        return [tuple(r.values()) for r in self.client.query(sql).result()]


def pg_credentials():
    keys = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
    creds = {k: os.environ.get(k) for k in keys}
    env_file = PROJECT_ROOT / ".env"
    if not all(creds.values()) and env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            k, sep, v = line.strip().partition("=")
            if sep and k in keys and not creds[k]:
                creds[k] = v.strip().strip('"').strip("'")
    missing = [k for k in keys if not creds[k]]
    if missing:
        raise SystemExit(f"Postgres credentials missing: {', '.join(missing)}")
    return creds


# -------------------------------------------------------------- aggregates
def scalar_checks(db):
    wh = lambda t: db.rel("warehouse", t)
    stg = db.rel("staging", "stg_retail_transactions")
    checks = [
        ("rows.raw.retail_transactions", f"select count(*) from {db.rel('raw', 'retail_transactions')}"),
        ("rows.staging.stg_retail_transactions", f"select count(*) from {stg}"),
    ]
    checks += [(f"rows.warehouse.{t}", f"select count(*) from {wh(t)}") for t in MARTS]
    checks += [
        ("staging.return_lines", f"select count(*) from {stg} where is_return"),
        ("staging.sum_line_amount", f"select sum(line_amount) from {stg}"),
        ("fact_orders.sale_orders", f"select count(*) from {wh('fact_orders')} where order_type = 'sale'"),
        ("fact_orders.credit_notes", f"select count(*) from {wh('fact_orders')} where order_type = 'credit_note'"),
        ("fact_orders.sum_net_amount (revenue KPI)", f"select sum(net_amount) from {wh('fact_orders')}"),
        ("fact_orders.sum_gross_sale_amount", f"select sum(gross_sale_amount) from {wh('fact_orders')}"),
        ("fact_orders.distinct_order_keys", f"select count(distinct order_key) from {wh('fact_orders')}"),
        ("monthly.sum_net_revenue (all sales)", f"select sum(net_revenue) from {wh('mart_monthly_sales_country')}"),
        ("monthly.sum_gross_revenue", f"select sum(gross_revenue) from {wh('mart_monthly_sales_country')}"),
        ("monthly.sum_num_orders", f"select sum(num_orders) from {wh('mart_monthly_sales_country')}"),
        ("rfm.customers", f"select count(distinct customer_id) from {wh('mart_customer_rfm')}"),
        ("rfm.sum_recency_days", f"select sum(recency_days) from {wh('mart_customer_rfm')}"),
        ("rfm.sum_frequency", f"select sum(frequency) from {wh('mart_customer_rfm')}"),
        ("rfm.sum_monetary", f"select sum(monetary) from {wh('mart_customer_rfm')}"),
        ("rfm.sum_return_rate", f"select sum(return_rate) from {wh('mart_customer_rfm')}"),
        ("rfm.sum_r_score", f"select sum(r_score) from {wh('mart_customer_rfm')}"),
        ("rfm.sum_f_score", f"select sum(f_score) from {wh('mart_customer_rfm')}"),
        ("rfm.sum_m_score", f"select sum(m_score) from {wh('mart_customer_rfm')}"),
        ("dim_customer.sum_tenure_days", f"select sum(customer_tenure_days) from {wh('dim_customer')}"),
        ("product.sum_net_revenue", f"select sum(net_revenue) from {wh('mart_product_performance')}"),
        ("product.sum_quantity_sold", f"select sum(quantity_sold) from {wh('mart_product_performance')}"),
        ("product.sum_return_rate_revenue", f"select sum(return_rate_revenue) from {wh('mart_product_performance')}"),
    ]
    checks += [(f"rfm.segment.{s}", f"select count(*) from {wh('mart_customer_rfm')} where customer_segment = '{s}'")
               for s in SEGMENTS]
    checks += [(f"product.abc.{c}", f"select count(*) from {wh('mart_product_performance')} where abc_class = '{c}'")
               for c in "ABC"]
    return checks


def churn_inputs_sql(db):
    t = {k: db.ts(v) for k, v in CHURN_TS.items()}
    stg = db.rel("staging", "stg_retail_transactions")
    return f"""
        with lines as (
            select customer_id, invoice, invoice_date, is_sale, is_return, line_amount
            from {stg}
            where customer_id is not null
        ),
        sale_orders as (
            select customer_id, invoice, min(invoice_date) as order_date
            from lines
            where is_sale and invoice_date >= {t['feature_start']} and invoice_date < {t['feature_end']}
            group by customer_id, invoice
        ),
        scored as (
            select customer_id
            from sale_orders
            group by customer_id
            having max(order_date) >= {t['active_start']}
        ),
        outcome as (
            select customer_id,
                   sum(case when is_sale then line_amount else 0 end)
                   - sum(case when is_return then abs(line_amount) else 0 end) as net_sales
            from lines
            where invoice_date >= {t['feature_end']} and invoice_date < {t['outcome_end']}
            group by customer_id
        )
        select count(*) as scored_customers,
               sum(case when coalesce(o.net_sales, 0) <= 0 then 1 else 0 end) as churned_customers
        from scored s
        left join outcome o on o.customer_id = s.customer_id
    """


# ---------------------------------------------------------- row-level diffs
ROW_CHECKS = [
    # (name, table, key column, compared columns)
    ("rows_differing.mart_customer_rfm (scores + segment)", "mart_customer_rfm", "customer_id",
     ["recency_days", "frequency", "monetary", "r_score", "f_score", "m_score", "customer_segment"]),
    ("rows_differing.dim_customer (country, dates)", "dim_customer", "customer_id",
     ["country", "first_order_date", "last_order_date", "customer_tenure_days"]),
    ("rows_differing.dim_product (description)", "dim_product", "stock_code",
     ["description", "quantity_sold", "net_revenue"]),
    ("rows_differing.mart_product_performance (abc_class)", "mart_product_performance", "stock_code",
     ["abc_class"]),
]


def keyed_rows(db, table, key, cols):
    sql = f"select {key}, {', '.join(cols)} from {db.rel('warehouse', table)}"
    return {r[0]: tuple(norm(v) for v in r[1:]) for r in db.rows(sql)}


# ------------------------------------------------------------------ helpers
def norm(v):
    """Make PG and BQ values comparable: numbers -> Decimal, datetimes -> ISO text."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, Decimal)):
        return Decimal(str(v))
    if hasattr(v, "isoformat"):
        return v.isoformat(sep=" ") if hasattr(v, "hour") else v.isoformat()
    return v


def compare(pg, bq):
    if pg is None or bq is None:
        return (pg is None and bq is None), ""
    if isinstance(pg, Decimal) and isinstance(bq, Decimal):
        diff = abs(pg - bq)
        return diff <= TOLERANCE, diff
    return pg == bq, ""


def main():
    pg, bq = Postgres(), BigQuery()
    results = []

    for name, pg_sql in scalar_checks(pg):
        results.append([name, norm(pg.rows(pg_sql)[0][0]), None])
    for i, (name, bq_sql) in enumerate(scalar_checks(bq)):
        results[i][2] = norm(bq.rows(bq_sql)[0][0])

    pg_churn = [norm(v) for v in pg.rows(churn_inputs_sql(pg))[0]]
    bq_churn = [norm(v) for v in bq.rows(churn_inputs_sql(bq))[0]]
    results.append(["churn_inputs.scored_customers", pg_churn[0], bq_churn[0]])
    results.append(["churn_inputs.churned_customers", pg_churn[1], bq_churn[1]])
    rate = lambda c: (c[1] / c[0]).quantize(Decimal("0.0001")) if c[0] else None
    results.append(["churn_inputs.churn_rate", rate(pg_churn), rate(bq_churn)])

    for name, table, key, cols in ROW_CHECKS:
        a, b = keyed_rows(pg, table, key, cols), keyed_rows(bq, table, key, cols)
        differing = sum(
            1 for k in set(a) | set(b)
            if k not in a or k not in b
            or not all(compare(x, y)[0] for x, y in zip(a[k], b[k]))
        )
        results.append([name, Decimal(0), Decimal(differing)])

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    n_bad = 0
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["check", "postgres", "bigquery", "abs_diff", "match"])
        for name, p, b in results:
            ok, diff = compare(p, b)
            n_bad += not ok
            writer.writerow([name, p, b, diff, ok])
            print(f"{'OK  ' if ok else 'DIFF'} {name}: postgres={p} bigquery={b}")
    print(f"\n{len(results) - n_bad}/{len(results)} checks match -> {OUT_PATH}")
    if any(r[0].startswith("rows_differing.mart_product_performance") and r[2] for r in results):
        print("Note: revenue_rank / abc_class use row_number() over net_revenue, and products with "
              "equal net_revenue have no tie-breaker in the model, so a few ABC boundary products "
              "can differ between engines (and between Postgres runs).")
    raise SystemExit(0 if n_bad == 0 else 1)


if __name__ == "__main__":
    main()
