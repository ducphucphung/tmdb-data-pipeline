import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from dotenv import load_dotenv
load_dotenv()

REPO_ROOT = os.getenv("REPO_ROOT")


default_args = {
    "owner": "dphung",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def repo_bash(command: str) -> str:
    return f"""
set -euo pipefail
cd {REPO_ROOT}
export SNAPSHOT_DATE="{{{{ ds }}}}"
{command}
""".strip()


with DAG(
    dag_id="tmdb_daily_pipeline",
    default_args=default_args,
    description="Daily TMDB pipeline from scraping to dbt models",
    start_date=datetime(2026, 4, 8),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["tmdb", "daily"],
) as dag:

    daily_scrape = BashOperator(
        task_id="daily_scrape",
        bash_command=repo_bash("python src/crawler/daily_scraper.py"),
    )

    upload_raw_to_gcs = BashOperator(
        task_id="upload_raw_to_gcs",
        bash_command=repo_bash("python src/crawler/upload_to_gcs.py"),
    )

    parse_raw_snapshot = BashOperator(
        task_id="parse_raw_snapshot",
        bash_command=repo_bash("python src/spark/parse_all_movies.py"),
    )

    transform_to_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command=repo_bash("python src/spark/transform_raw.py"),
    )

    load_silver_to_bigquery = BashOperator(
        task_id="load_silver_to_bigquery",
        bash_command=repo_bash("python src/move_silver_to_bq.py"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=repo_bash(
            f'dbt run --project-dir {REPO_ROOT} --profiles-dir "${{DBT_PROFILES_DIR:-$HOME/.dbt}}"'
        ),
    )

    daily_scrape >> upload_raw_to_gcs >> parse_raw_snapshot >> transform_to_silver >> load_silver_to_bigquery >> dbt_run
