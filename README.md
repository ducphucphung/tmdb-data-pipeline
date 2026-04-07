# TMDB Data Pipeline

TMDB movie data pipeline with daily scraping, GCS raw storage, PySpark transformations, BigQuery loading, dbt models, and Airflow orchestration.

## Overview

This project ingests movie data from TMDB, stores raw JSON in Google Cloud Storage, transforms it with PySpark, loads curated parquet data into BigQuery, and builds analytics models with dbt.

Current workflow:

1. `daily_scraper.py`
2. `parse_all_movies.py`
3. `transform_raw.py`
4. `move_silver_to_bq.py`
5. `dbt run`

## Architecture

Layers used in the pipeline:

- `raw`: TMDB JSON snapshots stored in GCS
- `silver_parsed`: parsed raw movie-detail data in parquet
- `silver`: transformed parquet tables for downstream loading
- `BigQuery`: warehouse layer for dbt models
- `dbt`: staging and marts for analytics consumption

Main components:

- `src/crawler/daily_scraper.py`: fetches daily TMDB data and writes raw JSON snapshots to GCS
- `src/spark/parse_all_movies.py`: parses raw JSON snapshots into structured parquet
- `src/spark/transform_raw.py`: builds silver tables from parsed parquet
- `src/move_silver_to_bq.py`: loads silver parquet data into BigQuery
- `airflow/dags/tmdb_2026.py`: orchestrates the pipeline in Airflow
- `models/`: dbt staging and mart models

## Repository Structure

```text
.
├── airflow/
│   └── dags/
├── models/
├── src/
│   ├── crawler/
│   └── spark/
├── dbt_project.yml
└── README.md
```

## Requirements

You will need:

- Python 3.10+
- Google Cloud Storage bucket
- Google service account credentials
- TMDB API read token
- PySpark
- dbt
- BigQuery dataset
- Airflow for orchestration

## Environment Variables

Create a `.env` file with the values your pipeline needs.

Typical variables:

```env
TMDB_READ_TOKEN=your_tmdb_token
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GCS_BUCKET_NAME=your_bucket_name
GOOGLE_PROJECT_ID=your_gcp_project
GOOGLE_DATASET_ID=your_bigquery_dataset
GOOGLE_LOCATION=your_gcp_region
REPO_ROOT=/absolute/path/to/this/repo
DBT_PROFILES_DIR=/path/to/.dbt
```

## Running the Pipeline Manually

Run one day of the pipeline with a specific snapshot date:

```bash
export SNAPSHOT_DATE=2026-04-08
python src/crawler/daily_scraper.py
python src/spark/parse_all_movies.py
python src/spark/transform_raw.py
python src/move_silver_to_bq.py
dbt run --project-dir . --profiles-dir "${DBT_PROFILES_DIR:-$HOME/.dbt}"
```

If `SNAPSHOT_DATE` is not set, the scripts default to `today()`.

## Airflow

The DAG file is:

- `airflow/dags/tmdb_2026.py`

The DAG uses Airflow's logical date as:

- `SNAPSHOT_DATE={{ ds }}`

Typical Airflow flow:

1. `daily_scrape`
2. `parse_raw_snapshot`
3. `transform_to_silver`
4. `load_silver_to_bigquery`
5. `dbt_run`

Useful commands:

```bash
airflow dags list | grep tmdb_daily_pipeline
airflow dags test tmdb_daily_pipeline 2026-04-08
```

## dbt

This repo includes dbt models for:

- staging models on silver-loaded BigQuery tables
- marts for dimensions, bridges, and facts

Common commands:

```bash
dbt run
dbt test
```

## Notes

- Raw TMDB movie detail files are written directly to GCS.
- Current Spark jobs process a single `SNAPSHOT_DATE` per run.
- Current silver writes use append-style behavior, so rerunning the same date may create duplicate rows.
- Dimension and fact modeling can continue evolving in dbt.
- `move_silver_to_bq.py` currently behaves like a full-load BigQuery step unless you change it to incremental logic.

## Future Improvements

Potential next steps:

- make BigQuery loads incremental by `snapshot_date`
- make Spark reruns idempotent for the same day
- move crawler state from local file storage to cloud storage or a database
- add tests for pipeline validation and data quality
- refine Airflow deployment and environment setup
