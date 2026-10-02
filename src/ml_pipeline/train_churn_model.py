"""
Customer churn/reactivation classification for the UCI Online Retail II
dataset (a non-contractual retail setting - there's no explicit
cancellation event, so "churn" must be operationally defined).

CHURN DEFINITION (documented also in README_PIPELINE.md). A snapshot is
defined by a cutoff date C (headline model: C = 2011-06-09):
    - FEATURE WINDOW: 2009-12-01 -> C. All features use only these rows.
    - ACTIVE (who is scored): >= 1 SALE order in the 6 calendar months ending
      on C (for C = 2011-06-09: 2010-12-10 -> 2011-06-09). Credit notes
      (returns) do not count as activity.
    - OUTCOME WINDOW: the 6 calendar months after C (2011-06-10 -> 2011-12-09).
    - LABEL: RETAINED (0) if the customer's outcome-window NET sales (sales
      minus credit notes/returns) are > 0, else CHURNED (1). An order that is
      fully reversed by a credit note therefore does not count as retention.
  Cold-start customers who first appear in the outcome window are out of scope.

  SEASONALITY: the headline outcome window contains the Nov/Dec Christmas
  peak, so its churn rate is low for seasonal reasons. The out-of-time check
  (C = 2010-12-09, outcome Dec 2010 -> Jun 2011) has a much higher churn rate
  under the exact same definitions.

VALIDATION:
  - Headline: random stratified 75/25 split (seed 42) of the C = 2011-06-09
    snapshot.
  - Out-of-time: the same model spec trained on the whole C = 2010-12-09
    snapshot, then evaluated on the C = 2011-06-09 snapshot (all scored
    customers, and the headline test split for a like-for-like comparison).
    Its training labels only use data up to 2011-06-09, so nothing from the
    evaluation outcome window leaks in.

FEATURES (feature window only; orders = sale invoices, never credit notes):
  recency_days, frequency (sale orders), monetary (net of returns),
  avg_order_value (gross sales / sale orders), total_items_purchased,
  return_rate (items returned / items bought), avg_distinct_products,
  customer_tenure_days, credit_notes_per_order.

MODEL: XGBoost classifier, scale_pos_weight computed from the training data.

OUTPUTS:
  outputs/model_metrics.json       - headline + out-of-time metrics
  outputs/model_report.txt         - human-readable summary + honest discussion of results
  outputs/shap_summary.png         - SHAP global feature-importance summary plot
  outputs/customer_churn_predictions.csv - test-set customer_id, churn_probability, actual_segment, top SHAP reasons
  outputs/customer_churn_labels.csv - every scored customer with features + observed label
"""
import json
import os

import numpy as np
import pandas as pd
import sqlalchemy
import xgboost as xgb
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report,
)
from sklearn.model_selection import train_test_split

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUTPUTS_DIR = os.environ.get("OUTPUTS_DIR", "/opt/airflow/outputs")

DATASET_START = pd.Timestamp("2009-12-01")
HEADLINE_CUTOFF = pd.Timestamp("2011-06-09")   # last day of the headline feature window
OOT_CUTOFF = pd.Timestamp("2010-12-09")        # earlier snapshot for the out-of-time check
# Only customers with a sale in the last 6 calendar months of the feature window
# are scored. Including long-dormant customers inflated churn and made it
# trivially predictable from recency.
ACTIVE_MONTHS = 6
OUTCOME_MONTHS = 6
SEED = 42

FEATURE_COLS = [
    "recency_days", "frequency", "monetary", "avg_order_value",
    "total_items_purchased", "return_rate", "avg_distinct_products",
    "customer_tenure_days", "credit_notes_per_order",
]

MODEL_PARAMS = dict(
    n_estimators=300, max_depth=4, learning_rate=0.05, subsample=0.8,
    colsample_bytree=0.8, eval_metric="auc", random_state=SEED, n_jobs=4,
)


def get_engine():
    user = os.environ.get("POSTGRES_USER", "retail_user")
    password = os.environ.get("POSTGRES_PASSWORD", "retail_pass")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5434")
    db = os.environ.get("POSTGRES_DB", "retail_analytics")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return sqlalchemy.create_engine(url)


def load_transactions(engine):
    """Customer-attributable staging lines, read in a stable order so the
    seeded train/test split is reproducible run to run."""
    txn = pd.read_sql(
        """
        select transaction_id, customer_id, invoice, invoice_date, is_sale,
               is_return, is_credit_note, line_amount, quantity, stock_code
        from staging.stg_retail_transactions
        where customer_id is not null
        order by transaction_id
        """,
        engine,
        parse_dates=["invoice_date"],
    )
    txn["line_amount"] = txn["line_amount"].astype(float)
    print(f"Loaded {len(txn)} customer-attributable transaction lines")
    return txn


def windows(cutoff):
    """Half-open [start, end) timestamps for a snapshot whose feature window ends on `cutoff`."""
    feature_end = cutoff + pd.Timedelta(days=1)
    return {
        "feature": (DATASET_START, feature_end),
        "active": (feature_end - pd.DateOffset(months=ACTIVE_MONTHS), feature_end),
        "outcome": (feature_end, feature_end + pd.DateOffset(months=OUTCOME_MONTHS)),
    }


def describe_windows(w):
    fmt = lambda a, b: [str(a.date()), str((b - pd.Timedelta(days=1)).date())]  # inclusive dates
    return {k: fmt(*v) for k, v in w.items()}


def sales_and_returns(lines):
    """Per-customer gross sales and returned value (same rules as dbt int_customer_orders)."""
    sales = lines.loc[lines["is_sale"]].groupby("customer_id")["line_amount"].sum()
    returns = lines.loc[lines["is_return"]].groupby("customer_id")["line_amount"].apply(lambda s: s.abs().sum())
    return sales, returns


def build_snapshot(txn, cutoff):
    """Feature table + churn label for one snapshot (see module docstring)."""
    w = windows(cutoff)
    in_window = lambda k: (txn["invoice_date"] >= w[k][0]) & (txn["invoice_date"] < w[k][1])
    feat, outcome = txn[in_window("feature")], txn[in_window("outcome")]

    # Orders = sale invoices only. Credit notes are handled separately as returns.
    orders = (
        feat[feat["is_sale"]].groupby(["customer_id", "invoice"])
        .agg(order_date=("invoice_date", "min"), gross=("line_amount", "sum"),
             items=("quantity", "sum"), distinct_products=("stock_code", "nunique"))
        .reset_index()
    )
    cust = (
        orders.groupby("customer_id")
        .agg(first_order_date=("order_date", "min"), last_order_date=("order_date", "max"),
             frequency=("invoice", "nunique"), gross_sales=("gross", "sum"),
             total_items_purchased=("items", "sum"), avg_distinct_products=("distinct_products", "mean"))
    )
    _, returned = sales_and_returns(feat)
    rets = feat[feat["is_return"]]
    cust["returned_amount"] = returned.reindex(cust.index).fillna(0.0)
    cust["total_items_returned"] = rets.groupby("customer_id")["quantity"].apply(lambda s: s.abs().sum()).reindex(cust.index).fillna(0)
    cust["n_credit_notes"] = (feat[feat["is_credit_note"]].groupby("customer_id")["invoice"].nunique()
                              .reindex(cust.index).fillna(0))
    cust = cust.reset_index()

    cust["recency_days"] = (cutoff - cust["last_order_date"].dt.normalize()).dt.days
    cust["customer_tenure_days"] = (cust["last_order_date"] - cust["first_order_date"]).dt.days
    cust["monetary"] = cust["gross_sales"] - cust["returned_amount"]           # net of returns
    cust["avg_order_value"] = cust["gross_sales"] / cust["frequency"]
    cust["return_rate"] = (cust["total_items_returned"] / cust["total_items_purchased"].replace(0, np.nan)).fillna(0)
    cust["credit_notes_per_order"] = cust["n_credit_notes"] / cust["frequency"]

    # Label: retained only if outcome-window NET sales (sales - returns) > 0, so an
    # order reversed by a credit note does not count as retention.
    o_sales, o_returns = sales_and_returns(outcome)
    o_net = o_sales.sub(o_returns, fill_value=0.0)
    cust["outcome_net_sales"] = cust["customer_id"].map(o_net).fillna(0.0)
    cust["bought_in_outcome"] = cust["customer_id"].isin(o_sales.index).astype(int)
    cust["churned"] = (cust["outcome_net_sales"] <= 0).astype(int)

    # Scored population: at least one SALE in the last 6 months of the feature window.
    cust = cust[cust["last_order_date"] >= w["active"][0]]
    cust = cust.dropna(subset=FEATURE_COLS).sort_values("customer_id").reset_index(drop=True)
    return cust, w


def evaluate(y_true, proba):
    pred = (proba >= 0.5).astype(int)
    return {
        "n": int(len(y_true)),
        "churn_rate": round(float(np.mean(y_true)), 4),
        "auc_roc": round(float(roc_auc_score(y_true, proba)), 4),
        "precision": round(float(precision_score(y_true, pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, pred, zero_division=0)), 4),
        "f1_score": round(float(f1_score(y_true, pred, zero_division=0)), 4),
        "accuracy": round(float(accuracy_score(y_true, pred)), 4),
        "confusion_matrix": confusion_matrix(y_true, pred, labels=[0, 1]).tolist(),
    }


def fit_model(X, y):
    n_pos, n_neg = int((y == 1).sum()), int((y == 0).sum())
    spw = n_neg / max(n_pos, 1)
    model = xgb.XGBClassifier(scale_pos_weight=spw, **MODEL_PARAMS)
    model.fit(X, y)
    return model, spw


def main():
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    engine = get_engine()
    txn = load_transactions(engine)

    cust, w = build_snapshot(txn, HEADLINE_CUTOFF)
    n_total = len(cust)
    churn_rate = cust["churned"].mean()
    flipped = int(((cust["bought_in_outcome"] == 1) & (cust["churned"] == 1)).sum())
    print(f"Headline snapshot windows: {describe_windows(w)}")
    print(f"Scored customers (sale in the active window): {n_total}")
    print(f"Observed churn rate (outcome net sales <= 0): {churn_rate:.4f}")
    print(f"Customers who bought in the outcome window but are churned (net <= 0): {flipped}")
    # All scored customers with their observed label, for the dashboards.
    cust[["customer_id", "frequency", "monetary", "recency_days", "customer_tenure_days",
          "avg_order_value", "return_rate", "churned"]].to_csv(
        os.path.join(OUTPUTS_DIR, "customer_churn_labels.csv"), index=False)

    X, y = cust[FEATURE_COLS], cust["churned"]
    X_train, X_test, y_train, y_test, cust_train, cust_test = train_test_split(
        X, y, cust, test_size=0.25, random_state=SEED, stratify=y
    )
    model, scale_pos_weight = fit_model(X_train, y_train)
    print(f"Train class counts: churned={int(y_train.sum())}, retained={int((y_train == 0).sum())}, "
          f"scale_pos_weight={scale_pos_weight:.3f}")
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_pred_proba >= 0.5).astype(int)
    headline = evaluate(y_test, y_pred_proba)

    # ---- Out-of-time check: train on the earlier snapshot, score the current one.
    oot_cust, oot_w = build_snapshot(txn, OOT_CUTOFF)
    oot_model, oot_spw = fit_model(oot_cust[FEATURE_COLS], oot_cust["churned"])
    oot_all = evaluate(y, oot_model.predict_proba(X)[:, 1])
    oot_test = evaluate(y_test, oot_model.predict_proba(X_test)[:, 1])
    print(f"OOT training snapshot windows: {describe_windows(oot_w)}")
    print(f"OOT training customers: {len(oot_cust)}, churn rate {oot_cust['churned'].mean():.4f}")

    metrics = {
        "n_customers_scored": int(n_total),
        "observed_churn_rate": round(float(churn_rate), 4),
        "train_size": int(len(X_train)),
        "test_size": int(len(X_test)),
        "scale_pos_weight": round(float(scale_pos_weight), 4),
        "auc_roc": headline["auc_roc"],
        "precision": headline["precision"],
        "recall": headline["recall"],
        "f1_score": headline["f1_score"],
        "accuracy": headline["accuracy"],
        "confusion_matrix": {"labels": ["retained(0)", "churned(1)"], "matrix": headline["confusion_matrix"]},
        "feature_window": describe_windows(w)["feature"],
        "active_window": describe_windows(w)["active"],
        "outcome_window": describe_windows(w)["outcome"],
        "label_rule": "churned = 1 unless outcome-window net sales (sales - returns) > 0",
        "customers_bought_in_outcome_but_net_nonpositive": flipped,
        "out_of_time": {
            "description": ("Same features/label/hyperparameters, trained on the whole earlier snapshot "
                            "and evaluated on the headline snapshot"),
            "train_snapshot": {**describe_windows(oot_w), "n_customers": int(len(oot_cust)),
                               "churn_rate": round(float(oot_cust["churned"].mean()), 4),
                               "scale_pos_weight": round(float(oot_spw), 4)},
            "eval_all_scored_customers": oot_all,
            "eval_headline_test_split": oot_test,
        },
    }

    with open(os.path.join(OUTPUTS_DIR, "model_metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    ow = metrics["out_of_time"]
    with open(os.path.join(OUTPUTS_DIR, "model_report.txt"), "w") as f:
        f.write("Retail Customer Churn Model - Report\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Feature window: {metrics['feature_window'][0]} -> {metrics['feature_window'][1]}\n")
        f.write(f"Scored = >=1 sale order in {metrics['active_window'][0]} -> {metrics['active_window'][1]}: {n_total} customers\n")
        f.write(f"Outcome window: {metrics['outcome_window'][0]} -> {metrics['outcome_window'][1]}\n")
        f.write(f"Label: {metrics['label_rule']}\n")
        f.write(f"Observed churn rate: {churn_rate:.2%}\n\n")
        f.write("Headline metrics (random stratified 25% holdout of the same snapshot):\n")
        for k in ("auc_roc", "accuracy", "precision", "recall", "f1_score"):
            f.write(f"  {k:10s} {headline[k]:.4f}\n")
        f.write(f"\nConfusion Matrix [rows=actual, cols=predicted] (0=retained, 1=churned):\n{np.array(headline['confusion_matrix'])}\n\n")
        f.write(classification_report(y_test, y_pred, zero_division=0))
        f.write("\nOut-of-time check (model trained on the snapshot ending "
                f"{ow['train_snapshot']['feature'][1]}, outcome {ow['train_snapshot']['outcome'][0]} -> "
                f"{ow['train_snapshot']['outcome'][1]}; {ow['train_snapshot']['n_customers']} customers, "
                f"churn {ow['train_snapshot']['churn_rate']:.2%}):\n")
        for name, m in (("all scored customers", oot_all), ("headline test split", oot_test)):
            f.write(f"  on {name} (n={m['n']}): AUC {m['auc_roc']:.4f}, precision {m['precision']:.4f}, "
                    f"recall {m['recall']:.4f}, F1 {m['f1_score']:.4f}, accuracy {m['accuracy']:.4f}\n")
        f.write("\n\nHonest discussion:\n")
        f.write(
            "This is a non-contractual retail dataset with no explicit cancellation event, "
            "so churn is inferred from a 6-month outcome window with no positive net sales. "
            f"The headline outcome window ({metrics['outcome_window'][0]} -> {metrics['outcome_window'][1]}) "
            "contains the Nov/Dec Christmas peak, so its churn rate is seasonally low; the same "
            f"definition one year earlier gives {ow['train_snapshot']['churn_rate']:.1%}. The out-of-time "
            "check is the better estimate of how a model trained on past data performs on a later period: "
            "AUC transfers reasonably, but precision/recall at the 0.5 threshold depend on the base rate "
            "the model was trained on. The model has no visibility into external factors "
            "(marketing, competitors, stockouts). These numbers are reported exactly as computed.\n"
        )

    # SHAP global summary plot
    import shap
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test)
    plt.figure()
    shap.summary_plot(shap_values, X_test, show=False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUTS_DIR, "shap_summary.png"), dpi=150)
    plt.close()

    # per-customer top SHAP reasons
    shap_df = pd.DataFrame(shap_values, columns=FEATURE_COLS, index=X_test.index)

    def top_reasons(row, n=3):
        ordered = row.abs().sort_values(ascending=False).index[:n]
        parts = []
        for feat in ordered:
            direction = "+" if row[feat] > 0 else "-"
            parts.append(f"{feat}({direction}{abs(row[feat]):.3f})")
        return "; ".join(parts)

    top_reasons_series = shap_df.apply(top_reasons, axis=1)

    # merge in customer_segment from mart_customer_rfm if available (full dataset, not feature-window)
    try:
        rfm = pd.read_sql("select customer_id, customer_segment from warehouse.mart_customer_rfm", engine)
    except Exception as e:
        print(f"Could not load mart_customer_rfm for segment enrichment: {e}")
        rfm = pd.DataFrame(columns=["customer_id", "customer_segment"])

    preds_df = cust_test[["customer_id"]].copy()
    preds_df["churn_probability"] = y_pred_proba
    preds_df["predicted_churn"] = y_pred
    preds_df["actual_churned_label"] = y_test.values
    preds_df["top_shap_reasons"] = top_reasons_series.values
    preds_df = preds_df.merge(rfm, on="customer_id", how="left")
    preds_df = preds_df.rename(columns={"customer_segment": "actual_segment"})
    preds_df = preds_df.sort_values(["churn_probability", "customer_id"], ascending=[False, True])
    preds_df.to_csv(os.path.join(OUTPUTS_DIR, "customer_churn_predictions.csv"), index=False)

    print("\n=== FINAL METRICS ===")
    print(json.dumps(metrics, indent=2))
    print(f"\nWrote outputs to {OUTPUTS_DIR}")


if __name__ == "__main__":
    main()
