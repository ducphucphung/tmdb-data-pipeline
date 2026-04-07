from pyspark.sql import SparkSession, functions as F
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from dotenv import load_dotenv


load_dotenv()

spark = SparkSession.builder \
    .appName('pyspark-run-with-gcp-bucket') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

# Set GCS credentials if necessary
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# # Define GCS bucket and file path
# bucket_name = os.getenv("GCS_BUCKET_NAME")
# file_name = "employee.csv"
# file_path = f"gs://{bucket_name}/{file_name}"
bucket_name = os.getenv("GCS_BUCKET_NAME")

# ---------------------------
# 2. Read raw JSON
# ---------------------------
# Single file example
input_path = "gs://tmdb-491611-tmdb-datalake/raw/movie_details/snapshot_date=2026-04-01/movie_id=467914.json"

df = spark.read.option("multiline", "true").json(input_path)

# Optional: inspect schema
df.printSchema()

# ---------------------------
# 3. Add snapshot_date
# ---------------------------
# For now, hard-code it. Later you can extract it from folder name like snapshot_date=2026-04-05
snapshot_date = "2026-04-05"

df = df.withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))

# ---------------------------
# 4. Core movie table
# ---------------------------
movies = (
    df.select(
        F.col("id").alias("movie_id"),
        "title",
        "original_title",
        "original_language",
        "overview",
        "homepage",
        "imdb_id",
        "release_date",
        "runtime",
        "status",
        "tagline",
        "adult",
        "video",
        "budget",
        "revenue",
        "popularity",
        "vote_average",
        "vote_count",
        "poster_path",
        "backdrop_path",
        "snapshot_date"
    )
)

# ---------------------------
# 5. Movie -> genre bridge
# ---------------------------
movie_genres = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("genres").alias("genre")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("genre.id").alias("genre_id"),
        F.col("genre.name").alias("genre_name")
    )
)

# ---------------------------
# 6. Movie -> production companies
# ---------------------------
movie_production_companies = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("production_companies").alias("company")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("company.id").alias("company_id"),
        F.col("company.name").alias("company_name"),
        F.col("company.origin_country").alias("company_origin_country"),
        F.col("company.logo_path").alias("company_logo_path")
    )
)

# ---------------------------
# 7. Movie -> production countries
# ---------------------------
movie_production_countries = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("production_countries").alias("country")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("country.iso_3166_1").alias("country_code"),
        F.col("country.name").alias("country_name")
    )
)

# ---------------------------
# 8. Movie -> spoken languages
# ---------------------------
movie_spoken_languages = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("spoken_languages").alias("lang")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("lang.iso_639_1").alias("language_code"),
        F.col("lang.english_name").alias("language_english_name"),
        F.col("lang.name").alias("language_name")
    )
)

# ---------------------------
# 9. Movie -> origin_country (top-level array of codes)
# ---------------------------
movie_origin_countries = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("origin_country").alias("origin_country_code")
    )
)

# ---------------------------
# 10. Cast table / bridge
# ---------------------------
movie_cast = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("credits.cast").alias("cast_member")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("cast_member.id").alias("person_id"),
        F.col("cast_member.name").alias("person_name"),
        F.col("cast_member.original_name").alias("original_name"),
        F.col("cast_member.gender").alias("gender"),
        F.col("cast_member.known_for_department").alias("known_for_department"),
        F.col("cast_member.character").alias("character"),
        F.col("cast_member.order").alias("cast_order"),
        F.col("cast_member.cast_id").alias("cast_id"),
        F.col("cast_member.credit_id").alias("credit_id"),
        F.col("cast_member.profile_path").alias("profile_path"),
        F.col("cast_member.popularity").alias("person_popularity"),
        F.col("cast_member.adult").alias("adult")
    )
)

# ---------------------------
# 11. Crew table / bridge
# ---------------------------
movie_crew = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("credits.crew").alias("crew_member")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("crew_member.id").alias("person_id"),
        F.col("crew_member.name").alias("person_name"),
        F.col("crew_member.original_name").alias("original_name"),
        F.col("crew_member.gender").alias("gender"),
        F.col("crew_member.known_for_department").alias("known_for_department"),
        F.col("crew_member.department").alias("department"),
        F.col("crew_member.job").alias("job"),
        F.col("crew_member.credit_id").alias("credit_id"),
        F.col("crew_member.profile_path").alias("profile_path"),
        F.col("crew_member.popularity").alias("person_popularity"),
        F.col("crew_member.adult").alias("adult")
    )
)

# ---------------------------
# 12. Keywords
# ---------------------------
movie_keywords = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("keywords.keywords").alias("kw")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("kw.id").alias("keyword_id"),
        F.col("kw.name").alias("keyword_name")
    )
)

# ---------------------------
# 13. Release dates (nested twice)
# release_dates.results -> release_dates
# ---------------------------
movie_release_dates = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("release_dates.results").alias("country_release")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("country_release.iso_3166_1").alias("country_code"),
        F.explode_outer("country_release.release_dates").alias("release_item")
    )
    .select(
        "movie_id",
        "snapshot_date",
        "country_code",
        F.col("release_item.certification").alias("certification"),
        F.col("release_item.iso_639_1").alias("language_code"),
        F.col("release_item.note").alias("note"),
        F.col("release_item.release_date").alias("release_datetime"),
        F.col("release_item.type").alias("release_type"),
        F.col("release_item.descriptors").alias("descriptors")
    )
)

# ---------------------------
# 14. Videos
# ---------------------------
movie_videos = (
    df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("videos.results").alias("video_item")
    )
    .select(
        "movie_id",
        "snapshot_date",
        F.col("video_item.id").alias("video_id"),
        F.col("video_item.iso_639_1").alias("language_code"),
        F.col("video_item.iso_3166_1").alias("country_code"),
        F.col("video_item.key").alias("video_key"),
        F.col("video_item.name").alias("video_name"),
        F.col("video_item.site").alias("site"),
        F.col("video_item.size").alias("size"),
        F.col("video_item.type").alias("video_type"),
        F.col("video_item.official").alias("official"),
        F.col("video_item.published_at").alias("published_at")
    )
)

# ---------------------------
# 15. Show samples
# ---------------------------
print("movies")
movies.show(truncate=False)

print("movie_production_companies")
movie_production_companies.show(truncate=False)

print("movie_cast")
movie_cast.show(truncate=False)

print("movie_crew")
movie_crew.show(truncate=False)

print("movie_release_dates")
movie_release_dates.show(truncate=False)

# ---------------------------
# 16. Write output
# ---------------------------
output_root = f"gs://{bucket_name}/test"

movies.write.mode("append").parquet(f"{output_root}/movies")
movie_genres.write.mode("append").parquet(f"{output_root}/movie_genres")
movie_production_companies.write.mode("append").parquet(f"{output_root}/movie_production_companies")
movie_production_countries.write.mode("append").parquet(f"{output_root}/movie_production_countries")
movie_spoken_languages.write.mode("append").parquet(f"{output_root}/movie_spoken_languages")
movie_origin_countries.write.mode("append").parquet(f"{output_root}/movie_origin_countries")
movie_cast.write.mode("append").parquet(f"{output_root}/movie_cast")
movie_crew.write.mode("append").parquet(f"{output_root}/movie_crew")
movie_keywords.write.mode("append").parquet(f"{output_root}/movie_keywords")
movie_release_dates.write.mode("append").parquet(f"{output_root}/movie_release_dates")
movie_videos.write.mode("append").parquet(f"{output_root}/movie_videos")

spark.stop()