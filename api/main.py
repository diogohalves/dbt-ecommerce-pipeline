from fastapi import FastAPI, Query, HTTPException
from typing import Literal
import psycopg2
import psycopg2.extras
from datetime import date
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(
    title="dbt E-commerce API",
    description="KPIs extraídos da pipeline dbt sobre dados Olist",
    version="1.0.0"
)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

VALID_GROUP_BY = {
    "status":           "o.status",
    "payment_type":     "o.main_payment_type",
    "state":            "c.state",
    "city":             "c.city",
}

VALID_METRICS = {
    "payment_value":    "o.payment_value_brl",
    "gross_value":      "o.gross_value_brl",
    "freight_value":    "o.freight_value_brl",
    "items_value":      "o.items_value_brl",
    "gross_value_usd":  "o.gross_value_usd",
}

VALID_AGGREGATIONS = ["sum", "avg", "min", "max", "count"]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@app.get("/")
def root():
    return {"message": "dbt E-commerce API", "docs": "/docs"}


@app.get("/kpi/orders")
def kpi_orders(
    date_from: date = Query(default=date(2016, 1, 1), description="Data inicial (YYYY-MM-DD)"),
    date_to:   date = Query(default=date(2018, 12, 31), description="Data final (YYYY-MM-DD)"),
    metric:    Literal["payment_value", "gross_value", "freight_value", "items_value", "gross_value_usd"] = Query(
        default="payment_value", description="Métrica a agregar"
    ),
    aggregation: Literal["sum", "avg", "min", "max", "count"] = Query(
        default="sum", description="Tipo de agregação"
    ),
    group_by: Literal["status", "payment_type", "state", "city"] = Query(
        default="status", description="Dimensão de agrupamento"
    ),
    limit: int = Query(default=20, ge=1, le=100, description="Limite de resultados"),
):
    if date_from > date_to:
        raise HTTPException(status_code=400, detail="date_from deve ser anterior a date_to")

    group_col   = VALID_GROUP_BY[group_by]
    metric_col  = VALID_METRICS[metric]
    needs_join  = group_by in ("state", "city")

    join_clause = (
        "LEFT JOIN dim_customers c ON o.customer_id = c.customer_id"
        if needs_join else ""
    )

    query = f"""
        SELECT
            {group_col}                             AS group_value,
            {aggregation}({metric_col})::numeric(18,2) AS value,
            count(o.order_id)                       AS order_count
        FROM fct_orders o
        {join_clause}
        WHERE o.purchased_at::date BETWEEN %(date_from)s AND %(date_to)s
          AND {metric_col} IS NOT NULL
        GROUP BY {group_col}
        ORDER BY value DESC
        LIMIT %(limit)s
    """

    try:
        conn = get_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, {
            "date_from": date_from,
            "date_to":   date_to,
            "limit":     limit
        })
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "params": {
            "date_from":   str(date_from),
            "date_to":     str(date_to),
            "metric":      metric,
            "aggregation": aggregation,
            "group_by":    group_by,
            "limit":       limit
        },
        "total_groups": len(rows),
        "data": [dict(r) for r in rows]
    }


@app.get("/kpi/orders/summary")
def kpi_summary(
    date_from: date = Query(default=date(2016, 1, 1)),
    date_to:   date = Query(default=date(2018, 12, 31)),
):
    query = """
        SELECT
            count(order_id)                     AS total_orders,
            sum(payment_value_brl)::numeric(18,2) AS total_payment_brl,
            sum(gross_value_brl)::numeric(18,2)   AS total_gross_brl,
            avg(gross_value_brl)::numeric(18,2)   AS avg_order_value,
            sum(gross_value_usd)::numeric(18,2)   AS total_gross_usd
        FROM fct_orders
        WHERE purchased_at::date BETWEEN %(date_from)s AND %(date_to)s
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, {"date_from": date_from, "date_to": date_to})
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "params": {"date_from": str(date_from), "date_to": str(date_to)},
        "data": dict(row)
    }