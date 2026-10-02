"""KPI definitions shared by the Excel and Tableau dashboard builders, so the two
dashboards always agree on every shared number."""
from decimal import ROUND_HALF_UP, Decimal


def headline_kpis(orders, rfm, labels, metrics):
    """orders  = fact_orders (sale orders AND credit notes, see order_type)
    rfm     = mart_customer_rfm (customers with >= 1 sale order)
    labels  = outputs/customer_churn_labels.csv (every scored customer)
    metrics = outputs/model_metrics.json"""
    sale = orders[orders["order_type"] == "sale"]
    n_orders = len(sale)
    return {
        "total_customers": int(rfm["customer_id"].nunique()),
        "churn_rate": float(labels["churned"].mean()),
        # net revenue = sales minus credit notes, customer-attributable orders only
        "net_revenue": float(orders["net_amount"].sum()),
        # orders = sale invoices only; credit notes are not orders
        "total_orders": int(n_orders),
        # value of an order as placed; returns are reported separately as return_rate
        "avg_order_value": float(sale["gross_sale_amount"].sum() / n_orders) if n_orders else 0.0,
        "return_rate": float(orders["return_amount"].sum() / orders["gross_sale_amount"].sum()),
        "champions": int((rfm["customer_segment"] == "Champions").sum()),
        # 3 d.p., rounded half-up from the 4 d.p. value in model_metrics.json;
        # README_PIPELINE.md quotes this same string
        "model_auc": str(Decimal(str(metrics["auc_roc"])).quantize(Decimal("0.001"), ROUND_HALF_UP)),
    }
