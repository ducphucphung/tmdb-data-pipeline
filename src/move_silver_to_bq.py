import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# CONFIG
# =========================================================
PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
DATASET_ID = os.getenv("GOOGLE_DATASET_ID")
LOCATION = os.getenv("GOOGLE_LOCATION")   
bucket_name = os.getenv("GCS_BUCKET_NAME")

# GCS silver root
GCS_SILVER_ROOT = f"gs://{bucket_name}/silver"

# Set True if you want to replace tables each run
OVERWRITE = True

# =========================================================
# TABLE MAP
# key   = BigQuery table name
# value = GCS path containing parquet files
# =========================================================
TABLES = {
    "movies": f"{GCS_SILVER_ROOT}/movies/*",
    "fact_movie_daily_snapshot": f"{GCS_SILVER_ROOT}/fact_movie_daily_snapshot/*.parquet",
    "movie_genres": f"{GCS_SILVER_ROOT}/movie_genres/*.parquet",
    "movie_production_companies": f"{GCS_SILVER_ROOT}/movie_production_companies/*.parquet",
    "movie_production_countries": f"{GCS_SILVER_ROOT}/movie_production_countries/*.parquet",
    "movie_spoken_languages": f"{GCS_SILVER_ROOT}/movie_spoken_languages/*.parquet",
    "movie_origin_countries": f"{GCS_SILVER_ROOT}/movie_origin_countries/*.parquet",
    "movie_cast": f"{GCS_SILVER_ROOT}/movie_cast/*.parquet",
    "movie_crew": f"{GCS_SILVER_ROOT}/movie_crew/*.parquet",
    "movie_keywords": f"{GCS_SILVER_ROOT}/movie_keywords/*.parquet",
    "movie_release_dates": f"{GCS_SILVER_ROOT}/movie_release_dates/*.parquet",
    "movie_videos": f"{GCS_SILVER_ROOT}/movie_videos/*.parquet",
    "dim_genre": f"{GCS_SILVER_ROOT}/dim_genre/*.parquet",
    "dim_company": f"{GCS_SILVER_ROOT}/dim_company/*.parquet",
    "dim_keyword": f"{GCS_SILVER_ROOT}/dim_keyword/*.parquet",
    "dim_language": f"{GCS_SILVER_ROOT}/dim_language/*.parquet",
    "dim_country": f"{GCS_SILVER_ROOT}/dim_country/*.parquet",
    "dim_person": f"{GCS_SILVER_ROOT}/dim_person/*.parquet",
}

# Tables that should be partitioned by snapshot_date in BigQuery
PARTITIONED_TABLES = {
    "movies",
    "fact_movie_daily_snapshot",
    "movie_genres",
    "movie_production_companies",
    "movie_production_countries",
    "movie_spoken_languages",
    "movie_origin_countries",
    "movie_cast",
    "movie_crew",
    "movie_keywords",
    "movie_release_dates",
    "movie_videos",
}

# =========================================================
# CLIENT
# =========================================================
client = bigquery.Client(project=PROJECT_ID)

dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"

# =========================================================
# HELPERS
# =========================================================
def ensure_dataset():
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset exists: {dataset_ref}")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        client.create_dataset(dataset)
        print(f"Created dataset: {dataset_ref}")

def load_parquet_table(table_name: str, gcs_uri: str):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if OVERWRITE else
            bigquery.WriteDisposition.WRITE_APPEND
        ),
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    # Partition by snapshot_date if present/desired
    if table_name in PARTITIONED_TABLES:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="snapshot_date",
        )

    print(f"Loading {gcs_uri} -> {table_id}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
        location=LOCATION,
    )

    load_job.result()  # wait for completion

    table = client.get_table(table_id)
    print(
        f"Loaded {table.num_rows:,} rows into {table_id}"
    )

# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    ensure_dataset()

    for table_name, gcs_uri in TABLES.items():
        try:
            load_parquet_table(table_name, gcs_uri)
        except Exception as e:
            print(f"FAILED loading {table_name}: {e}")