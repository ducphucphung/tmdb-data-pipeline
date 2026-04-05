from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "dphung",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="tmdb_ingestion_dag",
    default_args=default_args,
    description="Simple TMDB ingestion DAG",
    start_date=datetime(2026, 4, 5),
    schedule="@daily",
    catchup=True,
    tags=["tmdb"],
) as dag:

    ingest_task = BashOperator(
        task_id="run_tmdb_ingestion",
        bash_command="python -m src.crawler.daily_scraper"
    )

    ingest_task