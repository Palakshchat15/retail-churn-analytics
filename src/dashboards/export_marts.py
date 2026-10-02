"""
Export warehouse marts from Postgres to CSV files under data/processed/.

These CSVs feed: the churn model training script, the Excel dashboard
builder, and the Tableau .twb text-file datasources.
"""
import os

import pandas as pd
import sqlalchemy

OUT_DIR = os.environ.get("PROCESSED_DIR", "/opt/airflow/data/processed")


def get_engine():
    user = os.environ.get("POSTGRES_USER", "retail_user")
    password = os.environ.get("POSTGRES_PASSWORD", "retail_pass")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5434")
    db = os.environ.get("POSTGRES_DB", "retail_analytics")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return sqlalchemy.create_engine(url)


TABLES = {
    "mart_customer_rfm": "customer_rfm.csv",
    "mart_product_performance": "product_performance.csv",
    "mart_monthly_sales_country": "monthly_sales_country.csv",
    "fact_orders": "fact_orders.csv",
    "dim_customer": "dim_customer.csv",
    "dim_product": "dim_product.csv",
}


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    engine = get_engine()
    for table, filename in TABLES.items():
        df = pd.read_sql(f"SELECT * FROM warehouse.{table}", engine)
        out_path = os.path.join(OUT_DIR, filename)
        df.to_csv(out_path, index=False)
        print(f"Exported warehouse.{table} -> {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
