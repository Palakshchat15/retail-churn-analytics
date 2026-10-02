"""
Build outputs/Retail_Analytics_Dashboard.xlsx with openpyxl using REAL data
exported from the warehouse marts (data/processed/*.csv) and the churn
model outputs (outputs/customer_churn_predictions.csv).

Sheets:
  Dashboard        - KPI cards + 4 charts
  Summary          - pre-aggregated tables the Dashboard sheet's charts reference
  RawData          - fact_orders as an Excel Table (sampled if very large, but headers real)
  CustomerRFM      - mart_customer_rfm as an Excel Table
  ChurnPredictions - customer_churn_predictions as an Excel Table
"""
import json
import os
import sys

import pandas as pd
from openpyxl import Workbook
from openpyxl.chart import LineChart, BarChart, PieChart, Reference
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kpis import headline_kpis  # noqa: E402  (shared with the Tableau builder)

PROCESSED_DIR = os.environ.get("PROCESSED_DIR", "/opt/airflow/data/processed")
OUTPUTS_DIR = os.environ.get("OUTPUTS_DIR", "/opt/airflow/outputs")
XLSX_PATH = os.path.join(OUTPUTS_DIR, "Retail_Analytics_Dashboard.xlsx")

HEADER_FILL = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)
KPI_FILL = PatternFill(start_color="EFF6FC", end_color="EFF6FC", fill_type="solid")
KPI_LABEL_FONT = Font(size=10, color="44546A", bold=True)
KPI_VALUE_FONT = Font(size=18, bold=True, color="1F4E78")
THIN_BORDER = Border(*(Side(style="thin", color="CCCCCC"),) * 4)


def add_table(ws, df, start_row=1, start_col=1, table_name="Table1"):
    for j, col in enumerate(df.columns):
        cell = ws.cell(row=start_row, column=start_col + j, value=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
    for i, row in enumerate(df.itertuples(index=False), start=1):
        for j, val in enumerate(row):
            ws.cell(row=start_row + i, column=start_col + j, value=val)

    n_rows = len(df)
    n_cols = len(df.columns)
    last_col_letter = get_column_letter(start_col + n_cols - 1)
    first_col_letter = get_column_letter(start_col)
    ref = f"{first_col_letter}{start_row}:{last_col_letter}{start_row + n_rows}"
    tab = Table(displayName=table_name, ref=ref)
    tab.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium9", showRowStripes=True
    )
    ws.add_table(tab)
    for j, col in enumerate(df.columns):
        col_letter = get_column_letter(start_col + j)
        max_len = max([len(str(col))] + [len(str(v)) for v in df[col].astype(str).head(200)])
        ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 40)
    return start_row + n_rows + 1


def main():
    os.makedirs(OUTPUTS_DIR, exist_ok=True)

    fact_orders = pd.read_csv(os.path.join(PROCESSED_DIR, "fact_orders.csv"), parse_dates=["order_date"])
    rfm = pd.read_csv(os.path.join(PROCESSED_DIR, "customer_rfm.csv"))
    products = pd.read_csv(os.path.join(PROCESSED_DIR, "product_performance.csv"))
    monthly_country = pd.read_csv(os.path.join(PROCESSED_DIR, "monthly_sales_country.csv"), parse_dates=["sales_month"])
    churn_preds = pd.read_csv(os.path.join(OUTPUTS_DIR, "customer_churn_predictions.csv"))
    churn_labels = pd.read_csv(os.path.join(OUTPUTS_DIR, "customer_churn_labels.csv"))
    with open(os.path.join(OUTPUTS_DIR, "model_metrics.json")) as f:
        metrics = json.load(f)

    # ---- Pre-aggregations (real data) ----
    # Same definitions as the Tableau dashboard (src/dashboards/kpis.py): orders are
    # sale invoices only, revenue is net of credit notes, churn is over ALL scored
    # customers (not just the test split).
    kpi = headline_kpis(fact_orders, rfm, churn_labels, metrics)
    total_revenue = kpi["net_revenue"]
    total_customers = kpi["total_customers"]
    total_orders = kpi["total_orders"]
    avg_order_value = kpi["avg_order_value"]
    churn_rate = kpi["churn_rate"]

    country_rev = fact_orders.groupby("country")["net_amount"].sum().sort_values(ascending=False)
    top_country = country_rev.index[0]
    top_country_rev = float(country_rev.iloc[0])

    monthly_rev = (
        monthly_country.groupby("sales_month")["net_revenue"].sum().sort_index()
    )
    monthly_rev_df = monthly_rev.reset_index()
    monthly_rev_df.columns = ["Month", "NetRevenue"]
    monthly_rev_df["Month"] = monthly_rev_df["Month"].dt.strftime("%Y-%m")

    top_products = products.sort_values("net_revenue", ascending=False).head(10)[
        ["description", "net_revenue"]
    ].copy()
    top_products.columns = ["Product", "NetRevenue"]

    segment_counts = rfm["customer_segment"].value_counts().reset_index()
    segment_counts.columns = ["Segment", "CustomerCount"]

    # churn probability distribution bucketed
    bins = [0, 0.2, 0.4, 0.6, 0.8, 1.01]
    labels = ["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"]
    churn_preds["prob_bucket"] = pd.cut(churn_preds["churn_probability"], bins=bins, labels=labels, right=False)
    churn_dist = churn_preds["prob_bucket"].value_counts().reindex(labels).fillna(0).reset_index()
    churn_dist.columns = ["ChurnProbabilityRange", "CustomerCount"]

    at_risk = churn_preds[churn_preds["churn_probability"] >= 0.6].sort_values(
        "churn_probability", ascending=False
    ).head(10)[["customer_id", "churn_probability"]]

    # ---- Workbook ----
    wb = Workbook()

    # Summary sheet
    ws_sum = wb.active
    ws_sum.title = "Summary"
    row = add_table(ws_sum, monthly_rev_df, start_row=1, start_col=1, table_name="TblMonthlyRevenue")
    row = add_table(ws_sum, top_products, start_row=1, start_col=4, table_name="TblTopProducts")
    row = add_table(ws_sum, segment_counts, start_row=1, start_col=7, table_name="TblSegments")
    row = add_table(ws_sum, churn_dist, start_row=1, start_col=10, table_name="TblChurnDist")
    add_table(ws_sum, at_risk, start_row=1, start_col=13, table_name="TblAtRisk")

    # Dashboard sheet
    ws_dash = wb.create_sheet("Dashboard")
    ws_dash.sheet_view.showGridLines = False
    ws_dash["A1"] = "Retail E-commerce Analytics Dashboard"
    ws_dash["A1"].font = Font(size=20, bold=True, color="1F4E78")
    ws_dash.merge_cells("A1:H1")

    kpis = [
        ("Total Revenue", f"£{total_revenue:,.0f}"),
        ("Total Customers", f"{total_customers:,}"),
        ("Total Orders", f"{total_orders:,}"),
        ("Avg Order Value", f"£{avg_order_value:,.2f}"),
        ("Churn Rate", f"{churn_rate:.1%}"),
        ("Top Country by Revenue", f"{top_country} (£{top_country_rev:,.0f})"),
    ]
    for i, (label, value) in enumerate(kpis):
        col = 1 + (i % 3) * 3
        r = 3 + (i // 3) * 4
        cell_label = ws_dash.cell(row=r, column=col, value=label)
        cell_label.font = KPI_LABEL_FONT
        cell_value = ws_dash.cell(row=r + 1, column=col, value=value)
        cell_value.font = KPI_VALUE_FONT
        for rr in (r, r + 1):
            for cc in (col, col + 1):
                ws_dash.cell(row=rr, column=cc).fill = KPI_FILL
                ws_dash.cell(row=rr, column=cc).border = THIN_BORDER

    chart_anchor_row = 13

    # Chart 1: monthly revenue trend (line)
    line = LineChart()
    line.title = "Monthly Revenue Trend"
    line.style = 12
    line.y_axis.title = "Net Revenue (GBP)"
    line.x_axis.title = "Month"
    data = Reference(ws_sum, min_col=2, min_row=1, max_row=1 + len(monthly_rev_df))
    cats = Reference(ws_sum, min_col=1, min_row=2, max_row=1 + len(monthly_rev_df))
    line.add_data(data, titles_from_data=True)
    line.set_categories(cats)
    line.width, line.height = 16, 9
    ws_dash.add_chart(line, f"A{chart_anchor_row}")

    # Chart 2: top 10 products by revenue (bar)
    bar = BarChart()
    bar.type = "bar"
    bar.title = "Top 10 Products by Revenue"
    bar.style = 10
    data = Reference(ws_sum, min_col=5, min_row=1, max_row=1 + len(top_products))
    cats = Reference(ws_sum, min_col=4, min_row=2, max_row=1 + len(top_products))
    bar.add_data(data, titles_from_data=True)
    bar.set_categories(cats)
    bar.width, bar.height = 16, 9
    ws_dash.add_chart(bar, f"J{chart_anchor_row}")

    chart_anchor_row2 = chart_anchor_row + 19

    # Chart 3: customer segment breakdown (pie)
    pie = PieChart()
    pie.title = "Customer Segment Breakdown"
    data = Reference(ws_sum, min_col=8, min_row=1, max_row=1 + len(segment_counts))
    cats = Reference(ws_sum, min_col=7, min_row=2, max_row=1 + len(segment_counts))
    pie.add_data(data, titles_from_data=True)
    pie.set_categories(cats)
    pie.width, pie.height = 16, 9
    ws_dash.add_chart(pie, f"A{chart_anchor_row2}")

    # Chart 4: churn probability distribution (bar)
    bar2 = BarChart()
    bar2.type = "col"
    bar2.title = "Churn Probability Distribution (Test Set)"
    bar2.style = 11
    data = Reference(ws_sum, min_col=11, min_row=1, max_row=1 + len(churn_dist))
    cats = Reference(ws_sum, min_col=10, min_row=2, max_row=1 + len(churn_dist))
    bar2.add_data(data, titles_from_data=True)
    bar2.set_categories(cats)
    bar2.width, bar2.height = 16, 9
    ws_dash.add_chart(bar2, f"J{chart_anchor_row2}")

    for col, width in zip("ABCDEFGHIJKLMNO", [16] * 15):
        ws_dash.column_dimensions[col].width = width

    # RawData sheet (fact_orders) - full table, real transaction-level data
    ws_raw = wb.create_sheet("RawData")
    fact_orders_display = fact_orders.copy()
    fact_orders_display["order_date"] = fact_orders_display["order_date"].astype(str)
    add_table(ws_raw, fact_orders_display, table_name="TblFactOrders")

    # CustomerRFM sheet
    ws_rfm = wb.create_sheet("CustomerRFM")
    add_table(ws_rfm, rfm, table_name="TblCustomerRFM")

    # ChurnPredictions sheet
    ws_churn = wb.create_sheet("ChurnPredictions")
    add_table(ws_churn, churn_preds.drop(columns=["prob_bucket"]), table_name="TblChurnPredictions")

    wb.save(XLSX_PATH)
    print(f"Saved workbook: {XLSX_PATH}")
    print(f"Sheets: {wb.sheetnames}")

    return {
        "total_revenue": total_revenue,
        "total_customers": total_customers,
        "total_orders": total_orders,
        "avg_order_value": avg_order_value,
        "churn_rate": churn_rate,
        "top_country": top_country,
    }


if __name__ == "__main__":
    main()
