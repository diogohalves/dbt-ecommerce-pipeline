# 🛒 dbt E-commerce Pipeline

Pipeline de dados end-to-end construída com dados reais de e-commerce brasileiro, integrando ingestão, transformação, testes, orquestração, API REST e visualização analítica.

![CI](https://github.com/diogohalves/dbt-ecommerce-pipeline/actions/workflows/dbt_ci.yml/badge.svg)
![Docs](https://github.com/diogohalves/dbt-ecommerce-pipeline/actions/workflows/dbt_docs.yml/badge.svg)

---

## 🔗 Links

| | URL |
|---|---|
| 📡 **API** | https://dbt-ecommerce-pipeline-production.up.railway.app/docs |
| 📖 **dbt Docs** | https://diogohalves.github.io/dbt-ecommerce-pipeline |
| 💻 **Código** | https://github.com/diogohalves/dbt-ecommerce-pipeline |

---

## 🏗️ Arquitetura

```
Kaggle (Olist Dataset)          ExchangeRate API
        │                              │
        ▼                              ▼
   dbt seeds                    Python script
(CSVs → PostgreSQL)          (cotação BRL → CSV)
        │                              │
        └──────────────┬───────────────┘
                       ▼
              models/staging/
         (views — limpeza e tipagem)
                       │
                       ▼
               models/marts/
         (tables — regras de negócio)
          ┌────────────┬────────────┐
          ▼            ▼            ▼
    dim_customers  dim_products  fct_orders
                                fct_orders_incremental
                       │
                       ▼
            Semantic Layer (MetricFlow)
         (métricas de negócio centralizadas)
                       │
                       ▼
              FastAPI + Supabase
            (API REST em produção)
                       │
                       ▼
            Apache Airflow + Cosmos
          (orquestração agendada diária)
                       │
                       ▼
         GitHub Actions CI/CD + GitHub Pages
       (testes automáticos + dbt docs publicado)
                       │
                       ▼
                  Power BI
            (dashboard analítico)
```

---

## 🛠️ Tecnologias

| Tecnologia | Versão | Função |
|---|---|---|
| Python | 3.12 | Scripts de ingestão e automação |
| PostgreSQL | 16 | Banco de dados relacional |
| dbt Core | 1.10 | Transformação, testes e documentação |
| MetricFlow | 0.11 | Semantic Layer — métricas centralizadas |
| FastAPI | 0.135 | API REST de KPIs |
| Supabase | — | PostgreSQL gerenciado na nuvem |
| Railway | — | Deploy da API em produção |
| Apache Airflow | 2.9.3 | Orquestração da pipeline |
| Astronomer Cosmos | 1.4.0 | Integração nativa dbt + Airflow |
| GitHub Actions | — | CI/CD automatizado |
| GitHub Pages | — | Publicação automática do dbt docs |
| Power BI | — | Dashboard analítico final |

---

## 📁 Estrutura do Projeto

```
ecommerce/
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml          # CI/CD — roda dbt test a cada push
│       └── dbt_docs.yml        # Publica dbt docs no GitHub Pages
├── api/
│   ├── main.py                 # FastAPI — endpoints de KPI
│   ├── requirements.txt
│   ├── .env.example
│   └── .python-version
├── dags/
│   └── dbt_ecommerce_dag.py    # DAG do Airflow com Cosmos
├── models/
│   ├── staging/                # Camada de limpeza (materialização: view)
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_order_payments.sql
│   │   └── schema.yml
│   └── marts/                  # Camada analítica (materialização: table)
│       ├── dim_customers.sql
│       ├── dim_products.sql
│       ├── fct_orders.sql
│       ├── fct_orders_incremental.sql
│       ├── schema.yml          # Modelos, testes e Semantic Layer
│       └── time_spine/
│           ├── metricflow_time_spine.sql
│           └── schema.yml
├── seeds/
│   └── fetch_exchange_rates.py # Busca cotação BRL via API
├── tests/
│   └── assert_positive_order_values.sql
├── dbt_project.yml
├── packages.yml
└── .gitignore
```

---

## 📊 Modelos dbt

### Staging
Camada de limpeza 1:1 sobre os dados brutos — renomeação de colunas, tipagem e filtros básicos. Materializada como **view**.

| Modelo | Descrição |
|---|---|
| `stg_orders` | Pedidos com timestamps tipados |
| `stg_customers` | Clientes com colunas renomeadas |
| `stg_order_items` | Itens com valor total calculado |
| `stg_order_payments` | Pagamentos por pedido |

### Marts
Camada analítica com regras de negócio, joins e enriquecimento via API. Materializada como **table**.

| Modelo | Descrição |
|---|---|
| `dim_customers` | Clientes com histórico agregado de pedidos |
| `dim_products` | Produtos com categoria traduzida e métricas de venda |
| `fct_orders` | Fato central com pedidos, pagamentos e conversão BRL→USD |
| `fct_orders_incremental` | Versão incremental da fct_orders com upsert por `order_id` |

---

## 🧠 Semantic Layer

Métricas de negócio definidas centralmente via MetricFlow — qualquer ferramenta consome a mesma definição, sem duplicação de lógica.

| Métrica | Tipo | Descrição |
|---|---|---|
| `receita_total_brl` | simple | Soma do valor pago em BRL |
| `receita_total_usd` | simple | Soma do valor bruto em USD |
| `total_pedidos` | simple | Contagem de pedidos |
| `ticket_medio` | derived | Receita total / total de pedidos |

**Dimensões disponíveis:** `order__status`, `order__payment_type`, `metric_time__day`, `metric_time__month`, `metric_time__year`

---

## 📡 API REST

API pública em produção consultando os KPIs do data warehouse.

**Base URL:** `https://dbt-ecommerce-pipeline-production.up.railway.app`

### Endpoints

```
GET /                   → informações da API
GET /metrics            → lista métricas e dimensões disponíveis
GET /kpi                → consulta KPI com parâmetros
GET /kpi/summary        → resumo geral
GET /docs               → documentação interativa (Swagger)
```

### Exemplo de uso

```bash
# Receita total por status de pedido
GET /kpi?metrics=receita_total_brl&group_by=order__status

# Total de pedidos por tipo de pagamento em 2017
GET /kpi?metrics=total_pedidos&group_by=order__payment_type&date_from=2017-01-01&date_to=2017-12-31

# Múltiplas métricas
GET /kpi?metrics=receita_total_brl,total_pedidos&group_by=order__status
```

### Exemplo de resposta

```json
{
  "params": {
    "metrics": ["receita_total_brl"],
    "group_by": ["order__status"],
    "date_from": null,
    "date_to": null,
    "limit": 20
  },
  "total_rows": 8,
  "data": [
    { "order_status": "delivered", "receita_total_brl": 13494051.05 },
    { "order_status": "shipped",   "receita_total_brl": 177213.96 }
  ]
}
```

---

## ✅ Testes de Qualidade

33 testes automatizados cobrindo:
- `unique` — chaves primárias sem duplicatas
- `not_null` — campos obrigatórios sempre preenchidos
- `accepted_values` — status e tipos de pagamento válidos
- `relationships` — integridade referencial entre tabelas
- Teste singular: nenhum pedido com valor negativo

```bash
dbt test
# Done. PASS=33 WARN=0 ERROR=0
```

---

## 🚀 Como Rodar Localmente

### Pré-requisitos
- Python 3.12+
- PostgreSQL 16
- WSL2 (se Windows)

### 1. Clonar o repositório
```bash
git clone https://github.com/diogohalves/dbt-ecommerce-pipeline.git
cd dbt-ecommerce-pipeline
```

### 2. Criar e ativar o ambiente virtual
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install dbt-postgres==1.8.0
```

### 3. Configurar o profiles.yml
Crie o arquivo `~/.dbt/profiles.yml`:
```yaml
ecommerce:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: dbt_user
      password: dbt_pass
      dbname: dbt_ecommerce
      schema: public
      threads: 1
```

### 4. Baixar os dados do Kaggle
```bash
kaggle datasets download -d olistbr/brazilian-ecommerce --unzip -p seeds/
```

### 5. Buscar cotação cambial
```bash
python3 seeds/fetch_exchange_rates.py
```

### 6. Instalar dependências dbt
```bash
dbt deps
```

### 7. Rodar a pipeline
```bash
dbt seed
dbt run
dbt test
```

### 8. Ver a documentação e lineage
```bash
dbt docs generate
dbt docs serve
# Acesse http://localhost:8080
```

### 9. Rodar a API localmente
```bash
cd api
cp .env.example .env  # preencha com suas credenciais
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
# Acesse http://localhost:8000/docs
```

---

## ⚙️ Orquestração com Airflow

A pipeline é orquestrada pelo Apache Airflow com o Cosmos, que converte automaticamente cada modelo dbt em uma tarefa do Airflow.

**Agendamento:** diário às 6h  
**Fluxo:**
```
fetch_exchange_rates → dbt_seed → dbt_models/* → dbt_test
```

---

## 🔄 CI/CD

Dois workflows automatizados no GitHub Actions:

**dbt CI** — a cada push no `main`:
1. Sobe um PostgreSQL efêmero
2. Instala o dbt e dependências
3. Roda `dbt seed → dbt run → dbt test`
4. Reporta ✅ ou ❌

**dbt Docs** — a cada push no `main`:
1. Gera a documentação com `dbt docs generate`
2. Publica automaticamente no GitHub Pages

---

## 📈 Dashboard

Dashboard construído no Power BI conectado diretamente ao PostgreSQL, consumindo as tabelas da camada marts:

- Total de pedidos e receita
- Receita por status do pedido
- Volume de pedidos por estado

---

## 📬 Contato

**Diogo Henrique Alves**  
[LinkedIn](https://linkedin.com/in/diogohalves) · [GitHub](https://github.com/diogohalves)