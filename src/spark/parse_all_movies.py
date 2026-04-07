from pyspark.sql import SparkSession, functions as F, types as T
import os
from datetime import date
from dotenv import load_dotenv

load_dotenv()

spark = (
    SparkSession.builder
    .appName("movie-details-raw-to-parsed")
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
    .getOrCreate()
)

spark._jsc.hadoopConfiguration().set(
    "google.cloud.auth.service.account.json.keyfile",
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)

bucket_name = os.getenv("GCS_BUCKET_NAME")
snapshot_date = os.getenv("SNAPSHOT_DATE", date.today().isoformat())

RAW_ROOT = f"gs://{bucket_name}/raw/movie_details"
PARSED_ROOT = f"gs://{bucket_name}/silver_parsed/movie_details"

movie_schema = T.StructType([
    T.StructField("adult", T.BooleanType(), True),
    T.StructField("backdrop_path", T.StringType(), True),
    T.StructField("belongs_to_collection", T.StringType(), True),  # can refine later if needed
    T.StructField("budget", T.LongType(), True),

    T.StructField("genres", T.ArrayType(
        T.StructType([
            T.StructField("id", T.LongType(), True),
            T.StructField("name", T.StringType(), True),
        ])
    ), True),

    T.StructField("homepage", T.StringType(), True),
    T.StructField("id", T.LongType(), True),
    T.StructField("imdb_id", T.StringType(), True),

    T.StructField("origin_country", T.ArrayType(T.StringType()), True),

    T.StructField("original_language", T.StringType(), True),
    T.StructField("original_title", T.StringType(), True),
    T.StructField("overview", T.StringType(), True),
    T.StructField("popularity", T.DoubleType(), True),
    T.StructField("poster_path", T.StringType(), True),

    T.StructField("production_companies", T.ArrayType(
        T.StructType([
            T.StructField("id", T.LongType(), True),
            T.StructField("logo_path", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("origin_country", T.StringType(), True),
        ])
    ), True),

    T.StructField("production_countries", T.ArrayType(
        T.StructType([
            T.StructField("iso_3166_1", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
        ])
    ), True),

    T.StructField("release_date", T.StringType(), True),
    T.StructField("revenue", T.LongType(), True),
    T.StructField("runtime", T.LongType(), True),

    T.StructField("spoken_languages", T.ArrayType(
        T.StructType([
            T.StructField("english_name", T.StringType(), True),
            T.StructField("iso_639_1", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
        ])
    ), True),

    T.StructField("status", T.StringType(), True),
    T.StructField("tagline", T.StringType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("video", T.BooleanType(), True),
    T.StructField("vote_average", T.DoubleType(), True),
    T.StructField("vote_count", T.LongType(), True),

    T.StructField("credits", T.StructType([
        T.StructField("cast", T.ArrayType(
            T.StructType([
                T.StructField("adult", T.BooleanType(), True),
                T.StructField("cast_id", T.LongType(), True),
                T.StructField("character", T.StringType(), True),
                T.StructField("credit_id", T.StringType(), True),
                T.StructField("gender", T.LongType(), True),
                T.StructField("id", T.LongType(), True),
                T.StructField("known_for_department", T.StringType(), True),
                T.StructField("name", T.StringType(), True),
                T.StructField("order", T.LongType(), True),
                T.StructField("original_name", T.StringType(), True),
                T.StructField("popularity", T.DoubleType(), True),
                T.StructField("profile_path", T.StringType(), True),
            ])
        ), True),

        T.StructField("crew", T.ArrayType(
            T.StructType([
                T.StructField("adult", T.BooleanType(), True),
                T.StructField("credit_id", T.StringType(), True),
                T.StructField("department", T.StringType(), True),
                T.StructField("gender", T.LongType(), True),
                T.StructField("id", T.LongType(), True),
                T.StructField("job", T.StringType(), True),
                T.StructField("known_for_department", T.StringType(), True),
                T.StructField("name", T.StringType(), True),
                T.StructField("original_name", T.StringType(), True),
                T.StructField("popularity", T.DoubleType(), True),
                T.StructField("profile_path", T.StringType(), True),
            ])
        ), True),
    ]), True),

    T.StructField("keywords", T.StructType([
        T.StructField("keywords", T.ArrayType(
            T.StructType([
                T.StructField("id", T.LongType(), True),
                T.StructField("name", T.StringType(), True),
            ])
        ), True)
    ]), True),

    T.StructField("release_dates", T.StructType([
        T.StructField("results", T.ArrayType(
            T.StructType([
                T.StructField("iso_3166_1", T.StringType(), True),
                T.StructField("release_dates", T.ArrayType(
                    T.StructType([
                        T.StructField("certification", T.StringType(), True),
                        T.StructField("descriptors", T.ArrayType(T.StringType()), True),
                        T.StructField("iso_639_1", T.StringType(), True),
                        T.StructField("note", T.StringType(), True),
                        T.StructField("release_date", T.StringType(), True),
                        T.StructField("type", T.LongType(), True),
                    ])
                ), True)
            ])
        ), True)
    ]), True),

    T.StructField("videos", T.StructType([
        T.StructField("results", T.ArrayType(
            T.StructType([
                T.StructField("id", T.StringType(), True),
                T.StructField("iso_639_1", T.StringType(), True),
                T.StructField("iso_3166_1", T.StringType(), True),
                T.StructField("key", T.StringType(), True),
                T.StructField("name", T.StringType(), True),
                T.StructField("official", T.BooleanType(), True),
                T.StructField("published_at", T.StringType(), True),
                T.StructField("site", T.StringType(), True),
                T.StructField("size", T.LongType(), True),
                T.StructField("type", T.StringType(), True),
            ])
        ), True)
    ]), True),
])

raw_files = (
    spark.read
    .format("text")
    .option("wholetext", "true")
    .load(f"{RAW_ROOT}/snapshot_date={snapshot_date}/movie_id=*.json")
    .withColumnRenamed("value", "raw_json")
    .withColumn("source_file", F.input_file_name())
    .withColumn(
        "snapshot_date",
        F.to_date(
            F.regexp_extract("source_file", r"snapshot_date=(\d{4}-\d{2}-\d{2})", 1),
            "yyyy-MM-dd"
        )
    )
)

parsed = (
    raw_files
    .withColumn(
        "parsed",
        F.from_json(
            F.col("raw_json"),
            movie_schema,
            {"mode": "PERMISSIVE"}
        )
    )
)

df = parsed.select(
    "source_file",
    "snapshot_date",
    "raw_json",
    F.col("parsed.*")
)

parsed_good = df.filter(F.col("id").isNotNull())

# optional but useful: avoid too many tiny files
parsed_good = parsed_good.repartition("snapshot_date")

(
    parsed_good.write
    .mode("append")
    .partitionBy("snapshot_date")
    .parquet(PARSED_ROOT)
)


spark.stop()
