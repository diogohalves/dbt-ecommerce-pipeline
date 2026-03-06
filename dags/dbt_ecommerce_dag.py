from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/home/diogo/dbt_ecommerce/ecommerce")
DBT_EXECUTABLE   = Path("/home/diogo/dbt_ecommerce/.venv/bin/dbt")

profile_config = ProfileConfig(
    profile_name="ecommerce",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_ecommerce",
        profile_args={"schema": "public"},
    ),
)

with DAG(
    dag_id="dbt_ecommerce",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["dbt", "ecommerce"],
) as dag:

    fetch_rates = BashOperator(
        task_id="fetch_exchange_rates",
        bash_command=(
            "source /home/diogo/dbt_ecommerce/.venv/bin/activate && "
            "cd /home/diogo/dbt_ecommerce/ecommerce && "
            "python3 seeds/fetch_exchange_rates.py"
        ),
    )

    seed_rates = BashOperator(
        task_id="dbt_seed_exchange_rates",
        bash_command=(
            "source /home/diogo/dbt_ecommerce/.venv/bin/activate && "
            "cd /home/diogo/dbt_ecommerce/ecommerce && "
            "dbt seed --select exchange_rates"
        ),
    )

    dbt_run = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "source /home/diogo/dbt_ecommerce/.venv/bin/activate && "
            "cd /home/diogo/dbt_ecommerce/ecommerce && "
            "dbt test"
        ),
    )

    fetch_rates >> seed_rates >> dbt_run >> dbt_test