from fastapi import FastAPI, Query, HTTPException
from typing import Literal, Optional
from datetime import date
import subprocess
import json
from dotenv import load_dotenv
import os
import shutil

load_dotenv()

# Gera o profiles.yml dinamicamente a partir das variáveis de ambiente
def create_dbt_profiles():
    profiles_dir = os.getenv("DBT_PROFILES_DIR", os.path.expanduser("~/.dbt"))
    os.makedirs(profiles_dir, exist_ok=True)
    profiles_path = os.path.join(profiles_dir, "profiles.yml")

    profiles_content = f"""ecommerce:
  target: prod
  outputs:
    prod:
      type: postgres
      host: {os.getenv("DB_HOST")}
      port: {os.getenv("DB_PORT", 5432)}
      user: {os.getenv("DB_USER")}
      password: {os.getenv("DB_PASSWORD")}
      dbname: {os.getenv("DB_NAME")}
      schema: public
      threads: 1
"""
    with open(profiles_path, "w") as f:
        f.write(profiles_content)

create_dbt_profiles()

# Encontra o binário mf dinamicamente
MF_BIN = os.getenv("MF_BIN") or shutil.which("mf")
if not MF_BIN:
    raise RuntimeError("Binário 'mf' não encontrado. Verifique o PATH ou defina MF_BIN no .env")

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")

# Validar que as variáveis obrigatórias estão definidas
if not all([DBT_PROJECT_DIR, DBT_PROFILES_DIR, MF_BIN]):
    raise RuntimeError(
        "Variáveis de ambiente obrigatórias não definidas. "
        "Verifique o arquivo .env: DBT_PROJECT_DIR, DBT_PROFILES_DIR, MF_BIN"
    )

app = FastAPI(
    title="dbt E-commerce API",
    description="KPIs extraídos via MetricFlow sobre dados Olist",
    version="2.0.0"
)

# Métricas disponíveis na Semantic Layer
AVAILABLE_METRICS = [
    "receita_total_brl",
    "receita_total_usd",
    "total_pedidos",
    "ticket_medio",
]

# Dimensões disponíveis
AVAILABLE_DIMENSIONS = [
    "order__status",
    "order__payment_type",
    "metric_time__day",
    "metric_time__month",
    "metric_time__year",
]


def run_mf_query(
    metrics: list[str],
    group_by: list[str],
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 20,
    order: Optional[str] = None,
) -> list[dict]:
    """Executa mf query e retorna os resultados como lista de dicts."""

    cmd = [MF_BIN, "query"]

    for m in metrics:
        cmd += ["--metrics", m]

    for g in group_by:
        cmd += ["--group-by", g]

    if start_time:
        cmd += ["--start-time", start_time]
    if end_time:
        cmd += ["--end-time", end_time]
    if order:
        cmd += ["--order", order]

    cmd += ["--limit", str(limit)]
    cmd += ["--csv", "/tmp/mf_output.csv"]  # salva em CSV para parsear

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = DBT_PROFILES_DIR

    result = subprocess.run(
        cmd,
        cwd=DBT_PROJECT_DIR,
        env=env,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"MetricFlow error: {result.stderr or result.stdout}"
        )

    # Ler o CSV gerado
    import csv
    rows = []
    try:
        with open("/tmp/mf_output.csv", "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Converter valores numéricos
                parsed = {}
                for k, v in row.items():
                    try:
                        parsed[k] = float(v) if "." in v else int(v)
                    except (ValueError, TypeError):
                        parsed[k] = v
                rows.append(parsed)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="MetricFlow não gerou output.")

    return rows


@app.get("/")
def root():
    return {
        "message": "dbt E-commerce API v2 — powered by MetricFlow",
        "docs": "/docs",
        "metrics": AVAILABLE_METRICS,
        "dimensions": AVAILABLE_DIMENSIONS,
    }


@app.get("/metrics")
def list_metrics():
    """Lista todas as métricas disponíveis na Semantic Layer."""
    return {
        "metrics": AVAILABLE_METRICS,
        "dimensions": AVAILABLE_DIMENSIONS,
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
    date_from: Optional[date] = Query(
        default=None,
        description="Data inicial (YYYY-MM-DD)"
    ),
    date_to: Optional[date] = Query(
        default=None,
        description="Data final (YYYY-MM-DD)"
    ),
    limit: int = Query(default=20, ge=1, le=100),
    order: Optional[str] = Query(
        default=None,
        description="Dimensão ou métrica para ordenar. Ex: order__status"
    ),
):
    metrics_list = [m.strip() for m in metrics.split(",")]
    group_by_list = [g.strip() for g in group_by.split(",")]

    # Validar métricas
    invalid_metrics = [m for m in metrics_list if m not in AVAILABLE_METRICS]
    if invalid_metrics:
        raise HTTPException(
            status_code=400,
            detail=f"Métricas inválidas: {invalid_metrics}. Disponíveis: {AVAILABLE_METRICS}"
        )

    # Validar dimensões
    invalid_dims = [g for g in group_by_list if g not in AVAILABLE_DIMENSIONS]
    if invalid_dims:
        raise HTTPException(
            status_code=400,
            detail=f"Dimensões inválidas: {invalid_dims}. Disponíveis: {AVAILABLE_DIMENSIONS}"
        )

    data = run_mf_query(
        metrics=metrics_list,
        group_by=group_by_list,
        start_time=str(date_from) if date_from else None,
        end_time=str(date_to) if date_to else None,
        limit=limit,
        order=order,
    )

    return {
        "params": {
            "metrics": metrics_list,
            "group_by": group_by_list,
            "date_from": str(date_from) if date_from else None,
            "date_to": str(date_to) if date_to else None,
            "limit": limit,
        },
        "total_rows": len(data),
        "data": data,
    }