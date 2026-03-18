from fastapi import FastAPI, Query, HTTPException
from typing import Literal, Optional
from datetime import date
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import os

load_dotenv()

def get_db_config():
    config = {
        "host":     os.getenv("DB_HOST"),
        "port":     int(os.getenv("DB_PORT", 5432)),
        "dbname":   os.getenv("DB_NAME"),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }
    if not all(config.values()):
        raise RuntimeError("Variáveis de ambiente do banco não definidas. Verifique o .env")
    return config

app = FastAPI(
    title="dbt E-commerce API",
    description="KPIs extraídos da pipeline dbt sobre dados Olist",
    version="3.0.0"
)

METRICS = {
    "receita_total_brl": "SUM(payment_value_brl)",
    "receita_total_usd": "SUM(gross_value_usd)",
    "total_pedidos":     "COUNT(order_id)",
    "ticket_medio":      "AVG(gross_value_brl)",
}

DIMENSIONS = {
    "order__status":        "status",
    "order__payment_type":  "main_payment_type",
    "metric_time__day":     "purchased_at::date",
    "metric_time__month":   "DATE_TRUNC('month', purchased_at)",
    "metric_time__year":    "DATE_TRUNC('year', purchased_at)",
}


def get_connection():
    return psycopg2.connect(**get_db_config())


@app.get("/")
def root():
    return {
        "message": "dbt E-commerce API v3",
        "docs": "/docs",
        "metrics": list(METRICS.keys()),
        "dimensions": list(DIMENSIONS.keys()),
    }


@app.get("/metrics")
def list_metrics():
    return {
        "metrics": list(METRICS.keys()),
        "dimensions": list(DIMENSIONS.keys()),
    }


@app.get("/kpi")
def kpi(
    metrics: str = Query(
        default="receita_total_brl",
        description="Métricas separadas por vírgula. Ex: receita_total_brl,total_pedidos"
    ),
    group_by: str = Query(
        default="order__status",
        description="Dimensões separadas por vírgula. Ex: order__status,order__payment_type"
    ),
    date_from: Optional[date] = Query(default=None, description="Data inicial (YYYY-MM-DD)"),
    date_to:   Optional[date] = Query(default=None, description="Data final (YYYY-MM-DD)"),
    limit: int = Query(default=20, ge=1, le=100),
):
    metrics_list   = [m.strip() for m in metrics.split(",")]
    group_by_list  = [g.strip() for g in group_by.split(",")]

    # Validar métricas
    invalid_metrics = [m for m in metrics_list if m not in METRICS]
    if invalid_metrics:
        raise HTTPException(
            status_code=400,
            detail=f"Métricas inválidas: {invalid_metrics}. Disponíveis: {list(METRICS.keys())}"
        )

    # Validar dimensões
    invalid_dims = [g for g in group_by_list if g not in DIMENSIONS]
    if invalid_dims:
        raise HTTPException(
            status_code=400,
            detail=f"Dimensões inválidas: {invalid_dims}. Disponíveis: {list(DIMENSIONS.keys())}"
        )

    # Montar SELECT
    select_metrics = ", ".join(
        [f"{METRICS[m]} AS {m}" for m in metrics_list]
    )
    select_dims = ", ".join(
        [f"{DIMENSIONS[g]} AS {g.replace('__', '_')}" for g in group_by_list]
    )
    group_by_sql = ", ".join(
        [DIMENSIONS[g] for g in group_by_list]
    )

    where_clauses = []
    params = {}
    if date_from:
        where_clauses.append("purchased_at::date >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        where_clauses.append("purchased_at::date <= %(date_to)s")
        params["date_to"] = date_to

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    params["limit"] = limit

    query = f"""
        SELECT {select_dims}, {select_metrics}
        FROM fct_orders
        {where_sql}
        GROUP BY {group_by_sql}
        ORDER BY {list(METRICS.values())[0].replace('SUM', 'SUM').split('(')[0]}({list(METRICS.keys())[0]
            .replace('receita_total_brl', 'payment_value_brl')
            .replace('receita_total_usd', 'gross_value_usd')
            .replace('total_pedidos', 'order_id')
            .replace('ticket_medio', 'gross_value_brl')}) DESC
        LIMIT %(limit)s
    """

    try:
        conn = get_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "params": {
            "metrics":   metrics_list,
            "group_by":  group_by_list,
            "date_from": str(date_from) if date_from else None,
            "date_to":   str(date_to) if date_to else None,
            "limit":     limit,
        },
        "total_rows": len(rows),
        "data": [dict(r) for r in rows],
    }


@app.get("/kpi/summary")
def kpi_summary(
    date_from: Optional[date] = Query(default=None),
    date_to:   Optional[date] = Query(default=None),
):
    where_clauses = []
    params = {}
    if date_from:
        where_clauses.append("purchased_at::date >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        where_clauses.append("purchased_at::date <= %(date_to)s")
        params["date_to"] = date_to

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT
            COUNT(order_id)                       AS total_pedidos,
            SUM(payment_value_brl)::numeric(18,2) AS receita_total_brl,
            SUM(gross_value_usd)::numeric(18,2)   AS receita_total_usd,
            AVG(gross_value_brl)::numeric(18,2)   AS ticket_medio
        FROM fct_orders
        {where_sql}
    """

    try:
        conn = get_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params)
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "params": {
            "date_from": str(date_from) if date_from else None,
            "date_to":   str(date_to) if date_to else None,
        },
        "data": dict(row),
    }