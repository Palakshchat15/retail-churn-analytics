"""
Generates tableau/Retail_Analytics.twbx: a dark-themed dashboard with a row of
KPI tiles and six charts, built from the pipeline's outputs.

Tableau Public only opens workbooks whose data sources are extracts, so every
input table is written to a .hyper file (Tableau Hyper API) and packaged with
the workbook XML into a .twbx.

Runs on the Windows host or inside the Airflow container. The original CSV path
recorded in the workbook (used only for "Refresh Extract") should be the host
path: set TABLEAU_DATA_DIR when running in Docker.
"""
import csv
import json
import os
import re
import shutil
import tempfile
import zipfile
import sys
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr

import pandas as pd
from tableauhyperapi import (Connection, CreateMode, HyperProcess, SqlType, TableDefinition,
                             TableName, Telemetry, escape_string_literal)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kpis import headline_kpis  # noqa: E402  (shared with the Excel builder)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = Path(os.environ.get("PROCESSED_DIR", PROJECT_ROOT / "data" / "processed"))
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
TABLEAU_DIR = Path(os.environ.get("TABLEAU_DIR", PROJECT_ROOT / "tableau"))
WORKBOOK_NAME = "Retail_Analytics"
TWBX_PATH = TABLEAU_DIR / f"{WORKBOOK_NAME}.twbx"
DATA_DIR_IN_WORKBOOK = os.environ.get("TABLEAU_DATA_DIR", str(PROCESSED_DIR))

THEME = {
    "background": "#0f1b2d",
    "text": "#9fb9d8",
    "title": "#ffffff",
    "kpi_label": "#7fb2e5",
    "kpi_value": "#ffffff",
    "mark": "#2f6db5",
    "palette": "blue_10_0",
    "status": {"Churned": "#8ec5f0", "Retained": "#1f4e8c"},
}

HYPER_TYPES = {
    "integer": SqlType.big_int(),
    "real": SqlType.double(),
    "date": SqlType.date(),
    "datetime": SqlType.timestamp(),
    "string": SqlType.text(),
}
EXTRACT_TABLE = TableName("Extract", "Extract")

# prefix -> (derivation, column-instance type)
AGGREGATIONS = {
    "none": ("None", "nominal"),
    "sum": ("Sum", "quantitative"),
    "avg": ("Avg", "quantitative"),
    "cnt": ("Count", "quantitative"),
    "tmn": ("Month-Trunc", "quantitative"),
}

CAPTIONS = {
    "spend_band": "Spend Before Churn Window",
    "churn_rate": "Churn Rate",
    "tenure_bucket": "Customer Tenure",
    "status": "Churn Status",
    "customers": "Customers",
    "sales_month": "Month",
    "net_revenue": "Net Revenue (£)",
    "description": "Product",
    "country": "Country",
    "frequency_band": "Orders Placed",
    "frequency": "Orders Placed",
    "avg_order_value": "Avg Order Value (£)",
}

TENURE_ORDER = ["Single order", "1-3 months", "3-6 months", "6-12 months", "12+ months"]
SPEND_ORDER = ["Under £250", "£250-1K", "£1K-5K", "£5K+"]
FREQUENCY_ORDER = ["1 order", "2-3 orders", "4-6 orders", "7-12 orders", "13+ orders"]

KPIS = [  # (column in kpis.csv, tile label)
    ("total_customers", "TOTAL CUSTOMERS"),
    ("churn_rate", "CHURN RATE"),
    ("net_revenue", "NET REVENUE"),
    ("total_orders", "TOTAL ORDERS"),
    ("avg_order_value", "AVG ORDER VALUE"),
    ("return_rate", "RETURN RATE"),
    ("champions", "CHAMPIONS"),
    ("model_auc", "CHURN MODEL AUC"),
]

DATASOURCES = {
    "kpi": {"caption": "KPIs", "csv": "dash_kpis.csv"},
    "heat": {"caption": "Churn by Spend and Orders", "csv": "dash_churn_heatmap.csv"},
    "tenure": {"caption": "Churn by Tenure", "csv": "dash_churn_by_tenure.csv"},
    "scatter": {"caption": "Scored Customers", "csv": "dash_customer_scatter.csv"},
    "country": {"caption": "Churn by Country", "csv": "dash_churn_by_country.csv"},
    "monthly": {"caption": "Monthly Sales by Country", "csv": "monthly_sales_country.csv"},
    "product": {"caption": "Product Performance", "csv": "product_performance.csv"},
}

CHARTS = [
    # Heatmap (highlight table): where spend and order count combine to drive churn.
    {"name": "Churn Rate: Spend x Orders Placed", "ds": "heat", "mark": "Square",
     "rows": [("none", "spend_band")], "cols": [("none", "frequency_band")],
     "color": ("sum", "churn_rate"), "text": [("sum", "churn_rate")], "labels": True,
     "manual_order": [("spend_band", SPEND_ORDER), ("frequency_band", FREQUENCY_ORDER)]},
    {"name": "Churned (light) vs Retained (dark) by Tenure", "ds": "tenure", "mark": "Bar",
     "rows": [("sum", "customers")], "cols": [("none", "tenure_bucket")],
     "color": ("avg", "retained_flag"), "detail": ("none", "status"), "labels": True,
     "manual_order": [("tenure_bucket", TENURE_ORDER)]},
    {"name": "Monthly Net Revenue (incl. Guest Checkouts)", "ds": "monthly", "mark": "Area",
     "rows": [("sum", "net_revenue")], "cols": [("tmn", "sales_month")]},
    # Treemap: each product's share of revenue, sized and shaded by revenue.
    {"name": "Top 15 Products: Share of Revenue", "ds": "product", "mark": "Square",
     "rows": [], "cols": [], "size": ("sum", "net_revenue"), "color": ("sum", "net_revenue"),
     "text": [("none", "description"), ("sum", "net_revenue")], "labels": True,
     "top_n": {"field": "description", "by": "net_revenue", "n": 15}},
    # Scatter: one dot per scored customer; churners cluster at few orders.
    {"name": "Customers: Orders vs Avg Order Value", "ds": "scatter", "mark": "Circle",
     "rows": [("sum", "avg_order_value")], "cols": [("sum", "frequency")],
     "color": ("avg", "retained_flag"), "detail": ("none", "customer_id")},
    {"name": "Churn Rate by Country (10+ Customers)", "ds": "country", "mark": "Bar",
     "rows": [("none", "country")], "cols": [("sum", "churn_rate")],
     "color": ("sum", "churn_rate"), "labels": True,
     "sort_desc_by": ("country", ("sum", "churn_rate"))},
]

DASHBOARD_NAME = "Retail Customer Analytics"
KPI_HEIGHT, GAP = 12000, 600
CHART_W = (100000 - 4 * GAP) // 3
CHART_H = (100000 - KPI_HEIGHT - 4 * GAP) // 2
KPI_W = (100000 - (len(KPIS) + 1) * GAP) // len(KPIS)

CLEAN_RULES = """
          <style-rule element='gridline'>
            <format attr='line-visibility' scope='rows' value='off' />
            <format attr='line-visibility' scope='cols' value='off' />
          </style-rule>
          <style-rule element='zeroline'>
            <format attr='line-visibility' value='off' />
          </style-rule>
          <style-rule element='axis'>
            <format attr='line-visibility' value='off' />
          </style-rule>
          <style-rule element='table-div'>
            <format attr='line-visibility' scope='rows' value='off' />
            <format attr='line-visibility' scope='cols' value='off' />
          </style-rule>"""

FORMATS = {"churn_rate": "p0.0%"}  # Tableau number formats by column

ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ---------------------------------------------------------------- dashboard data

def fmt_money(value):
    return f"£{value / 1e6:.1f}M" if abs(value) >= 1e6 else f"£{value:,.0f}"


def prepare_dashboard_data():
    """Aggregates the pipeline outputs into the small tables the dashboard plots."""
    rfm = pd.read_csv(PROCESSED_DIR / "customer_rfm.csv")
    labels = pd.read_csv(OUTPUTS_DIR / "customer_churn_labels.csv")
    orders = pd.read_csv(PROCESSED_DIR / "fact_orders.csv")
    metrics = json.loads((OUTPUTS_DIR / "model_metrics.json").read_text())

    scored = labels.merge(rfm[["customer_id", "country", "customer_segment"]], on="customer_id", how="left")

    def churn_table(df, key):
        out = (df.groupby(key)["churned"].agg(customers="size", churned="sum").reset_index())
        out["churn_rate"] = (out["churned"] / out["customers"]).round(4)
        return out

    # Spend is measured in the feature window only. RFM segments are not used here:
    # they are computed over the full period, so "Lost" customers are churned by definition.
    scored["spend_band"] = pd.cut(scored["monetary"], [-float("inf"), 250, 1000, 5000, float("inf")],
                                  labels=SPEND_ORDER)

    tenure_days = scored["customer_tenure_days"]
    scored["tenure_bucket"] = pd.cut(tenure_days, [-1, 0, 90, 180, 365, float("inf")], labels=TENURE_ORDER)
    tenure = (scored.assign(status=scored["churned"].map({1: "Churned", 0: "Retained"}))
              .groupby(["tenure_bucket", "status"], observed=True).size().rename("customers").reset_index())
    # Numeric flag drives a two-shade blue gradient (light = churned, dark = retained).
    tenure["retained_flag"] = (tenure["status"] == "Retained").astype(int)
    tenure.to_csv(PROCESSED_DIR / "dash_churn_by_tenure.csv", index=False)

    scored["frequency_band"] = pd.cut(scored["frequency"], [0, 1, 3, 6, 12, float("inf")], labels=FREQUENCY_ORDER)
    heat = scored.groupby(["spend_band", "frequency_band"], observed=True)["churned"].agg(
        customers="size", churned="sum").reset_index()
    heat["churn_rate"] = (heat["churned"] / heat["customers"]).round(4)
    heat = heat[heat["customers"] >= 15]  # tiny cells give noisy rates
    heat.to_csv(PROCESSED_DIR / "dash_churn_heatmap.csv", index=False)

    # Scatter: positive order values only, capped at the 99th percentile so a few
    # wholesale-sized customers don't squash everyone else into a corner.
    dots = scored[scored["avg_order_value"] > 0].copy()
    for col in ("frequency", "avg_order_value"):
        dots[col] = dots[col].clip(upper=dots[col].quantile(0.99)).round(2)
    dots["customer_id"] = "C" + dots["customer_id"].astype(str)
    dots["status"] = dots["churned"].map({1: "Churned", 0: "Retained"})
    dots["retained_flag"] = 1 - dots["churned"]
    dots[["customer_id", "frequency", "avg_order_value", "status", "retained_flag"]].to_csv(
        PROCESSED_DIR / "dash_customer_scatter.csv", index=False)

    by_country = churn_table(scored, "country")
    by_country = by_country[by_country["customers"] >= 10].nlargest(10, "customers")
    by_country.to_csv(PROCESSED_DIR / "dash_churn_by_country.csv", index=False)

    # Customer orders only (fact_orders), matching the Excel dashboard. The monthly mart
    # also includes guest checkouts with no customer ID, so it must not be divided by
    # the customer order count. fact_orders also holds credit notes ('C' invoices):
    # they count towards net revenue and the return rate, never as orders.
    kpi = headline_kpis(orders, rfm, labels, metrics)
    kpis = {
        "total_customers": f"{kpi['total_customers']:,}",
        "churn_rate": f"{kpi['churn_rate']:.1%}",
        "net_revenue": fmt_money(kpi["net_revenue"]),
        "total_orders": f"{kpi['total_orders']:,}",
        "avg_order_value": f"£{kpi['avg_order_value']:,.0f}",
        "return_rate": f"{kpi['return_rate']:.1%}",
        "champions": f"{kpi['champions']:,}",
        "model_auc": kpi["model_auc"],
    }
    pd.DataFrame([kpis]).to_csv(PROCESSED_DIR / "dash_kpis.csv", index=False)
    print("KPIs:", kpis)


# ---------------------------------------------------------------- workbook XML

def infer_type(values):
    """Returns (datatype, role, type) for a CSV column from sample values."""
    vals = [v for v in values if v not in ("", None)]
    if vals and all(re.fullmatch(r"-?\d+", v) for v in vals):
        return "integer", "measure", "quantitative"
    if vals and all(re.fullmatch(r"-?\d+(\.\d+)?([eE][-+]?\d+)?", v) for v in vals):
        return "real", "measure", "quantitative"
    if vals and all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v) for v in vals):
        return "date", "dimension", "ordinal"
    if vals and all(re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?", v) for v in vals):
        return "datetime", "dimension", "ordinal"
    return "string", "dimension", "nominal"


def read_schema(csv_path, sample_rows=1000):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        samples = [row for _, row in zip(range(sample_rows), reader)]
    schema = []
    for i, name in enumerate(header):
        if not ID_RE.match(name):
            raise ValueError(f"{csv_path.name}: column {name!r} needs quoting support")
        schema.append((name, i, *infer_type([r[i] for r in samples if i < len(r)])))
    return schema


def caption(col):
    return CAPTIONS.get(col, col.replace("_", " ").title())


def ds_name(key):
    return f"federated.{key}"


def instance_name(prefix, col):
    kind = "nk" if AGGREGATIONS[prefix][1] == "nominal" else "qk"
    return f"[{prefix}:{col}:{kind}]"


def field_ref(ds_key, prefix, col):
    return f"[{ds_name(ds_key)}].{instance_name(prefix, col)}"


def write_hyper(csv_path, schema, hyper_path, hyper):
    """Loads csv_path into Extract.Extract in a new .hyper file; returns row count."""
    table = TableDefinition(EXTRACT_TABLE, [
        TableDefinition.Column(name, HYPER_TYPES[dt]) for name, _, dt, _, _ in schema
    ])
    with Connection(hyper.endpoint, hyper_path, CreateMode.CREATE_AND_REPLACE) as conn:
        conn.catalog.create_schema(EXTRACT_TABLE.schema_name)
        conn.catalog.create_table(table)
        return conn.execute_command(
            f"COPY {EXTRACT_TABLE} FROM {escape_string_literal(str(csv_path))} "
            "WITH (format csv, header, NULL '')"
        )


def extract_xml(key, row_count):
    now = datetime.now()
    return f"""      <extract count='-1' enabled='true' units='records'>
        <connection access_mode='readonly' authentication='auth-none' author-locale='en_US' class='hyper' dbname='Data/Extracts/{key}.hyper' default-settings='hyper' schema='Extract' sslmode='' tablename='Extract' update-time={quoteattr(now.strftime('%m/%d/%Y %I:%M:%S %p'))} username='tableau'>
          <relation name='Extract' table='[Extract].[Extract]' type='table' />
          <refresh>
            <refresh-event add-from-file-path={quoteattr(key)} increment-value='%null%' refresh-type='create' rows-inserted='{row_count}' timestamp-start={quoteattr(now.strftime('%Y-%m-%d %H:%M:%S.000'))} />
          </refresh>
        </connection>
      </extract>"""


def color_map_rule(field):
    """Fixed colours for the churn-status categories, matching the theme."""
    maps = "\n".join(
        f"              <map to='{color}'>\n                <bucket>{escape(chr(34) + status + chr(34))}</bucket>\n              </map>"
        for status, color in THEME["status"].items()
    )
    return f"""
          <style-rule element='mark'>
            <encoding attr='color' field='{field}' type='palette'>
{maps}
            </encoding>
          </style-rule>"""


def datasource_xml(key, spec, schema, row_count):
    csv_file = spec["csv"]
    stem = Path(csv_file).stem
    relation_cols = "\n".join(
        f"            <column datatype='{dt}' name={quoteattr(n)} ordinal='{i}' />"
        for n, i, dt, _, _ in schema
    )
    ds_cols = "\n".join(
        f"      <column caption={quoteattr(caption(n))} datatype='{dt}'"
        + (f" default-format='{FORMATS[n]}'" if n in FORMATS else "")
        + f" name='[{n}]' role='{role}' type='{typ}' />"
        for n, _, dt, role, typ in schema
    )
    return f"""    <datasource caption={quoteattr(spec['caption'])} inline='true' name='{ds_name(key)}' version='18.1'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption={quoteattr(stem)} name='textscan.{key}'>
            <connection class='textscan' directory={quoteattr(DATA_DIR_IN_WORKBOOK.replace(chr(92), '/'))} filename={quoteattr(csv_file)} password='' server='' />
          </named-connection>
        </named-connections>
        <relation connection='textscan.{key}' name={quoteattr(csv_file)} table='[{stem}#csv]' type='table'>
          <columns character-set='UTF-8' header='yes' locale='en_US' separator=','>
{relation_cols}
          </columns>
        </relation>
      </connection>
      <aliases enabled='yes' />
{ds_cols}
{extract_xml(key, row_count)}
    </datasource>"""


def title_xml(text, color, size, align=None):
    align_attr = " fontalignment='1'" if align == "center" else ""
    return f"""      <layout-options>
        <title>
          <formatted-text>
            <run bold='true' fontcolor='{color}' fontsize='{size}'{align_attr}>{escape(text)}</run>
          </formatted-text>
        </title>
      </layout-options>"""


def sheet_style_xml(extra_rules=""):
    return f"""        <style>
          <style-rule element='table'>
            <format attr='background-color' value='{THEME["background"]}' />
          </style-rule>
          <style-rule element='worksheet'>
            <format attr='color' value='{THEME["text"]}' />
          </style-rule>{CLEAN_RULES}{extra_rules}
        </style>"""


def pane_style_xml(labels, fixed_color, color_map=None):
    formats = []
    if labels:
        formats.append("<format attr='mark-labels-show' value='true' />")
    if fixed_color:
        formats.append(f"<format attr='mark-color' value='{THEME['mark']}' />")
    if not formats and not color_map:
        return ""
    body = "\n".join(f"                {f}" for f in formats)
    return f"""
            <style>
              <style-rule element='mark'>
{body}
              </style-rule>{color_map_rule(color_map) if color_map else ""}
            </style>"""


def shelf(key, fields):
    """Multiple fields on one shelf are nested (Tableau's '/' operator); empty -> ''."""
    refs = [field_ref(key, p, c) for p, c in fields]
    if not refs:
        return ""
    return refs[0] if len(refs) == 1 else "(" + " / ".join(refs) + ")"


def shelf_xml(tag, key, fields):
    value = shelf(key, fields)
    return f"<{tag}>{value}</{tag}>" if value else f"<{tag} />"


def dependencies_xml(key, types, used):
    deps = []
    for col in dict.fromkeys(c for _, c in used):
        dt, role, typ = types[col]
        deps.append(f"            <column caption={quoteattr(caption(col))} datatype='{dt}' "
                    f"name='[{col}]' role='{role}' type='{typ}' />")
        for prefix, c in used:
            if c == col:
                derivation, itype = AGGREGATIONS[prefix]
                deps.append(
                    f"            <column-instance column='[{col}]' derivation='{derivation}' "
                    f"name='{instance_name(prefix, col)}' pivot='key' type='{itype}' />"
                )
    return "\n".join(deps)


def chart_xml(ws, schemas):
    key = ws["ds"]
    types = {n: (dt, role, typ) for n, _, dt, role, typ in schemas[key]}
    extra = [ws[k] for k in ("color", "detail", "size") if k in ws] + ws.get("text", [])
    used = list(dict.fromkeys(ws["rows"] + ws["cols"] + extra))
    filter_xml, slices_xml = "", ""
    if "top_n" in ws:
        t = ws["top_n"]
        level = instance_name("none", t["field"])
        filter_xml = f"""
          <filter class='categorical' column='{field_ref(key, "none", t["field"])}'>
            <groupfilter count='{t["n"]}' end='top' function='end' units='records' user:ui-marker='end' user:ui-top-by-field='true'>
              <groupfilter direction='DESC' expression='SUM([{t["by"]}])' function='order' user:ui-marker='order'>
                <groupfilter function='level-members' level='{level}' user:ui-enumeration='all' user:ui-marker='enumerate' />
              </groupfilter>
            </groupfilter>
          </filter>"""
        slices_xml = f"""
          <slices>
            <column>{field_ref(key, "none", t["field"])}</column>
          </slices>"""
    if "sort_desc_by" in ws:
        dim, measure = ws["sort_desc_by"]
        filter_xml += f"""
          <sort class='computed' column='{field_ref(key, "none", dim)}' direction='DESC' using='{field_ref(key, *measure)}' />"""
    for dim, values in ws.get("manual_order", []):
        buckets = "\n".join(f"              <bucket>{escape(chr(34) + v + chr(34))}</bucket>" for v in values)
        filter_xml += f"""
          <sort class='manual' column='{field_ref(key, "none", dim)}' direction='ASC'>
            <dictionary>
{buckets}
            </dictionary>
          </sort>"""
    enc = []
    if "color" in ws:
        enc.append(f"<color column='{field_ref(key, *ws['color'])}' />")
    if "size" in ws:
        enc.append(f"<size column='{field_ref(key, *ws['size'])}' />")
    enc += [f"<text column='{field_ref(key, *t)}' />" for t in ws.get("text", [])]
    if "detail" in ws:
        enc.append(f"<lod column='{field_ref(key, *ws['detail'])}' />")
    encodings = ""
    if enc:
        body = "\n".join(f"              {e}" for e in enc)
        encodings = f"""
            <encodings>
{body}
            </encodings>"""
    encodings += pane_style_xml(ws.get("labels", False), fixed_color="color" not in ws,
                                color_map=field_ref(key, *ws["color"]) if ws.get("status_colors") else None)
    return f"""    <worksheet name={quoteattr(ws['name'])}>
{title_xml(ws['name'], THEME['title'], 12)}
      <table>
        <view>
          <datasources>
            <datasource caption={quoteattr(DATASOURCES[key]['caption'])} name='{ds_name(key)}' />
          </datasources>
          <datasource-dependencies datasource='{ds_name(key)}'>
{dependencies_xml(key, types, used)}
          </datasource-dependencies>{filter_xml}{slices_xml}
          <aggregation value='true' />
        </view>
{sheet_style_xml()}
        <panes>
          <pane selection-relaxation-option='selection-relaxation-allow'>
            <view>
              <breakdown value='auto' />
            </view>
            <mark class='{ws['mark']}' />{encodings}
          </pane>
        </panes>
        {shelf_xml('rows', key, ws['rows'])}
        {shelf_xml('cols', key, ws['cols'])}
      </table>
    </worksheet>"""


def kpi_style_xml():
    return f"""        <style>
          <style-rule element='table'>
            <format attr='background-color' value='{THEME["background"]}' />
          </style-rule>
          <style-rule element='cell'>
            <format attr='font-size' value='20' />
            <format attr='font-weight' value='bold' />
            <format attr='color' value='{THEME["kpi_value"]}' />
            <format attr='text-align' value='center' />
            <format attr='vertical-align' value='center' />
          </style-rule>{CLEAN_RULES}
        </style>"""


def kpi_xml(column, label, schemas):
    """A big-number tile: one text mark showing a preformatted value, titled with the label."""
    types = {n: (dt, role, typ) for n, _, dt, role, typ in schemas["kpi"]}
    ref = field_ref("kpi", "none", column)
    return f"""    <worksheet name={quoteattr(label)}>
{title_xml(label, THEME['kpi_label'], 9, align='center')}
      <table>
        <view>
          <datasources>
            <datasource caption='KPIs' name='{ds_name("kpi")}' />
          </datasources>
          <datasource-dependencies datasource='{ds_name("kpi")}'>
{dependencies_xml("kpi", types, [("none", column)])}
          </datasource-dependencies>
          <aggregation value='true' />
        </view>
{kpi_style_xml()}
        <panes>
          <pane selection-relaxation-option='selection-relaxation-allow'>
            <view>
              <breakdown value='auto' />
            </view>
            <mark class='Text' />
            <encodings>
              <text column='{ref}' />
            </encodings>
            <customized-label>
              <formatted-text>
                <run bold='true' fontalignment='1' fontcolor='{THEME["kpi_value"]}' fontsize='24'>&lt;{ref}&gt;</run>
              </formatted-text>
            </customized-label>
          </pane>
        </panes>
        <rows />
        <cols />
      </table>
    </worksheet>"""


def zones():
    """KPI tiles across the top, charts in a 3 x 2 grid below."""
    placed, zone_id = [], 2
    for i, (_, label) in enumerate(KPIS):
        x = GAP + i * (KPI_W + GAP)
        placed.append((zone_id, label, x, GAP, KPI_W, KPI_HEIGHT)); zone_id += 1
    for i, chart in enumerate(CHARTS):
        col, row = i % 3, i // 3
        x = GAP + col * (CHART_W + GAP)
        y = KPI_HEIGHT + 2 * GAP + row * (CHART_H + GAP)
        placed.append((zone_id, chart["name"], x, y, CHART_W, CHART_H)); zone_id += 1
    zone_style = ("            <zone-style>\n"
                  "              <format attr='border-style' value='none' />\n"
                  "              <format attr='border-width' value='0' />\n"
                  "              <format attr='margin' value='4' />\n"
                  "            </zone-style>\n")
    return "\n".join(
        f"          <zone h='{h}' id='{zid}' name={quoteattr(name)} w='{w}' x='{x}' y='{y}'>\n"
        f"{zone_style}          </zone>"
        for zid, name, x, y, w, h in placed
    )


def dashboard_xml():
    return f"""    <dashboard name={quoteattr(DASHBOARD_NAME)}>
      <style>
        <style-rule element='table'>
          <format attr='background-color' value='{THEME["background"]}' />
        </style-rule>
      </style>
      <size maxheight='900' maxwidth='1600' minheight='900' minwidth='1600' />
      <zones>
        <zone h='100000' id='1' type-v2='layout-basic' w='100000' x='0' y='0'>
{zones()}
        </zone>
      </zones>
    </dashboard>"""


def windows_xml():
    names = [label for _, label in KPIS] + [c["name"] for c in CHARTS]
    viewpoints = "\n".join(
        f"        <viewpoint name={quoteattr(n)}>\n          <zoom type='entire-view' />\n        </viewpoint>"
        for n in names
    )
    return f"""  <windows source-height='30'>
    <window class='dashboard' maximized='true' name={quoteattr(DASHBOARD_NAME)}>
      <viewpoints>
{viewpoints}
      </viewpoints>
      <active id='-1' />
    </window>
  </windows>"""


def build():
    prepare_dashboard_data()
    TABLEAU_DIR.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="twbx_"))
    schemas, row_counts, hyper_files = {}, {}, {}
    # log_dir keeps hyperd.log out of the working directory; work_dir is deleted below.
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
                      parameters={"log_dir": str(work_dir)}) as hyper:
        for key, spec in DATASOURCES.items():
            path = PROCESSED_DIR / spec["csv"]
            if not path.exists():
                raise FileNotFoundError(f"{path} not found; run export_marts.py first")
            schemas[key] = read_schema(path)
            hyper_files[key] = work_dir / f"{key}.hyper"
            row_counts[key] = write_hyper(path, schemas[key], hyper_files[key], hyper)
            print(f"  {spec['csv']}: {row_counts[key]} rows -> {key}.hyper")
    datasources = "\n".join(
        datasource_xml(k, s, schemas[k], row_counts[k]) for k, s in DATASOURCES.items()
    )
    worksheets = "\n".join(
        [kpi_xml(col, label, schemas) for col, label in KPIS] + [chart_xml(c, schemas) for c in CHARTS]
    )
    xml = f"""<?xml version='1.0' encoding='utf-8' ?>
<workbook original-version='18.1' source-build='2026.2.0 (20262.26.0819.2015)' source-platform='win' version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>
  <preferences>
    <preference name='ui.encoding.shelf.height' value='24' />
    <preference name='ui.shelf.height' value='26' />
  </preferences>
  <datasources>
{datasources}
  </datasources>
  <worksheets>
{worksheets}
  </worksheets>
  <dashboards>
{dashboard_xml()}
  </dashboards>
{windows_xml()}
</workbook>
"""
    with zipfile.ZipFile(TWBX_PATH, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{WORKBOOK_NAME}.twb", xml)
        for key, hyper_path in hyper_files.items():
            z.write(hyper_path, f"Data/Extracts/{key}.hyper")
    shutil.rmtree(work_dir, ignore_errors=True)
    print(f"Wrote {TWBX_PATH}")


if __name__ == "__main__":
    build()
