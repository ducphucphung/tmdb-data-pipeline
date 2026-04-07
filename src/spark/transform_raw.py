from pyspark.sql import SparkSession, functions as F, types as T
import os
from datetime import date
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from dotenv import load_dotenv
from pyspark.sql.window import Window

load_dotenv()

spark = SparkSession.builder \
    .appName('pyspark-run-with-gcp-bucket') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

# Set GCS credentials if necessary
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

bucket_name = os.getenv("GCS_BUCKET_NAME")

# =========================================================
# 2. Paths
# =========================================================
RAW_ROOT = f"gs://{bucket_name}/raw/movie_details"
OUT_ROOT = f"gs://{bucket_name}/silver"

PARSED_ROOT = f"gs://{bucket_name}/silver_parsed/movie_details"
snapshot_date = os.getenv("SNAPSHOT_DATE", date.today().isoformat())

all_parsed_df = spark.read.parquet(PARSED_ROOT)
good_df = all_parsed_df.filter(F.col("snapshot_date") == F.to_date(F.lit(snapshot_date)))

# =========================================================
# 3. Explicit schema
#    Important: nullable=True for anomaly tolerance
# =========================================================
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


# =========================================================
# 7. Core movie snapshot table
# =========================================================
movies = (
    good_df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        "title",
        "original_title",
        "original_language",
        "overview",
        "homepage",
        "imdb_id",
        F.col("release_date").try_cast("date").alias("release_date"),
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
        "source_file"
    )
    .dropDuplicates(["movie_id", "snapshot_date"])
)

# =========================================================
# 8. Genres
# =========================================================
movie_genres = (
    good_df.select(
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
    .filter(F.col("genre_id").isNotNull() | F.col("genre_name").isNotNull())
    .dropDuplicates()
)

dim_genre = (
    all_parsed_df.select(
        F.explode_outer("genres").alias("genre")
    )
    .select(
        F.col("genre.id").alias("genre_id"),
        F.col("genre.name").alias("genre_name")
    )
    .filter(
        F.col("genre_id").isNotNull() |
        F.col("genre_name").isNotNull()
    )
    .dropDuplicates(["genre_id"])
)

# =========================================================
# 9. Production companies
# =========================================================
movie_production_companies = (
    good_df.select(
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
    .filter(
        F.col("company_id").isNotNull() |
        F.col("company_name").isNotNull()
    )
    .dropDuplicates()
)

dim_company = (
    all_parsed_df.select(
        F.explode_outer("production_companies").alias("company")
    )
    .select(
        F.col("company.id").alias("company_id"),
        F.col("company.name").alias("company_name"),
        F.col("company.origin_country").alias("company_origin_country"),
        F.col("company.logo_path").alias("company_logo_path")
    )
    .filter(
        F.col("company_id").isNotNull() |
        F.col("company_name").isNotNull()
    )
    .dropDuplicates(["company_id"])
)

# =========================================================
# 10. Production countries
#     If weird / missing, fields become null rather than crashing
# =========================================================
movie_production_countries = (
    good_df.select(
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
    .filter(
        F.col("country_code").isNotNull() |
        F.col("country_name").isNotNull()
    )
    .dropDuplicates()
)

# =========================================================
# 11. Spoken languages
# =========================================================
movie_spoken_languages = (
    good_df.select(
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
    .filter(
        F.col("language_code").isNotNull() |
        F.col("language_name").isNotNull() |
        F.col("language_english_name").isNotNull()
    )
    .dropDuplicates()
)

dim_language = (
    all_parsed_df.select(
        F.explode_outer("spoken_languages").alias("lang")
    )
    .select(
        F.col("lang.iso_639_1").alias("language_code"),
        F.col("lang.english_name").alias("language_english_name"),
        F.col("lang.name").alias("language_name")
    )
    .filter(
        F.col("language_code").isNotNull() |
        F.col("language_name").isNotNull() |
        F.col("language_english_name").isNotNull()
    )
    .dropDuplicates(["language_code"])
)

# =========================================================
# 12. Origin countries
#     origin_country is array<string>, not struct
# =========================================================
movie_origin_countries = (
    good_df.select(
        F.col("id").alias("movie_id"),
        "snapshot_date",
        F.explode_outer("origin_country").alias("country_code")
    )
    .filter(F.col("country_code").isNotNull())
    .dropDuplicates()
)

dim_country = (
    all_parsed_df.select(
        F.explode_outer("production_countries").alias("country")
    )
    .select(
        F.col("country.iso_3166_1").alias("country_code"),
        F.col("country.name").alias("country_name")
    )
    .filter(
        F.col("country_code").isNotNull() |
        F.col("country_name").isNotNull()
    )
    .dropDuplicates(["country_code"])
)

# =========================================================
# 13. Cast
# =========================================================
movie_cast = (
    good_df.select(
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
    .filter(
        F.col("person_id").isNotNull() |
        F.col("person_name").isNotNull()
    )
    .dropDuplicates()
)

# =========================================================
# 14. Crew
# =========================================================
movie_crew = (
    good_df.select(
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
    .filter(
        F.col("person_id").isNotNull() |
        F.col("person_name").isNotNull()
    )
    .dropDuplicates()
)

# People come from both cast and crew, so union them first
person_from_cast = good_df.select(
    F.explode_outer("credits.cast").alias("cast_member")
).select(
    F.col("cast_member.id").alias("person_id"),
    F.col("cast_member.name").alias("person_name"),
    F.col("cast_member.original_name").alias("original_name"),
    F.col("cast_member.gender").alias("gender"),
    F.col("cast_member.known_for_department").alias("known_for_department"),
    F.col("cast_member.profile_path").alias("profile_path"),
    F.col("cast_member.popularity").alias("person_popularity"),
    F.col("cast_member.adult").alias("adult")
)

all_person_from_cast = all_parsed_df.select(
    F.explode_outer("credits.cast").alias("cast_member")
).select(
    F.col("cast_member.id").alias("person_id"),
    F.col("cast_member.name").alias("person_name"),
    F.col("cast_member.original_name").alias("original_name"),
    F.col("cast_member.gender").alias("gender"),
    F.col("cast_member.known_for_department").alias("known_for_department"),
    F.col("cast_member.profile_path").alias("profile_path"),
    F.col("cast_member.popularity").alias("person_popularity"),
    F.col("cast_member.adult").alias("adult")
)

person_from_crew = good_df.select(
    F.explode_outer("credits.crew").alias("crew_member")
).select(
    F.col("crew_member.id").alias("person_id"),
    F.col("crew_member.name").alias("person_name"),
    F.col("crew_member.original_name").alias("original_name"),
    F.col("crew_member.gender").alias("gender"),
    F.col("crew_member.known_for_department").alias("known_for_department"),
    F.col("crew_member.profile_path").alias("profile_path"),
    F.col("crew_member.popularity").alias("person_popularity"),
    F.col("crew_member.adult").alias("adult")
)

all_person_from_crew = all_parsed_df.select(
    F.explode_outer("credits.crew").alias("crew_member")
).select(
    F.col("crew_member.id").alias("person_id"),
    F.col("crew_member.name").alias("person_name"),
    F.col("crew_member.original_name").alias("original_name"),
    F.col("crew_member.gender").alias("gender"),
    F.col("crew_member.known_for_department").alias("known_for_department"),
    F.col("crew_member.profile_path").alias("profile_path"),
    F.col("crew_member.popularity").alias("person_popularity"),
    F.col("crew_member.adult").alias("adult")
)

person_union = person_from_cast.unionByName(person_from_crew)
w = Window.partitionBy("person_id").orderBy(F.col("person_popularity").desc_nulls_last())
dim_person = (
    all_person_from_cast
    .unionByName(all_person_from_crew)
    .filter(F.col("person_id").isNotNull())
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# =========================================================
# 15. Keywords
# =========================================================
movie_keywords = (
    good_df.select(
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
    .filter(
        F.col("keyword_id").isNotNull() |
        F.col("keyword_name").isNotNull()
    )
    .dropDuplicates()
)

dim_keyword = (
    all_parsed_df.select(
        F.explode_outer("keywords.keywords").alias("kw")
    )
    .select(
        F.col("kw.id").alias("keyword_id"),
        F.col("kw.name").alias("keyword_name")
    )
    .filter(
        F.col("keyword_id").isNotNull() |
        F.col("keyword_name").isNotNull()
    )
    .dropDuplicates(["keyword_id"])
)

# =========================================================
# 16. Release dates
# =========================================================
movie_release_dates = (
    good_df.select(
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
    .filter(
        F.col("country_code").isNotNull() |
        F.col("release_datetime").isNotNull()
    )
    .dropDuplicates()
)

# =========================================================
# 17. Videos
# =========================================================
movie_videos = (
    good_df.select(
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
    .filter(
        F.col("video_id").isNotNull() |
        F.col("video_key").isNotNull()
    )
    .dropDuplicates()
)

# =========================================================
# 18. Optional: fact-style daily metrics table
# =========================================================
fact_movie_daily_snapshot = (
    movies.select(
        "movie_id",
        "snapshot_date",
        "popularity",
        "vote_average",
        "vote_count",
        "revenue",
        "runtime",
        "status"
    )
)

# =========================================================
# 19. Write outputs
#     Partition by snapshot_date for easier downstream loads
# =========================================================
(
    movies.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movies")
)

(
    fact_movie_daily_snapshot.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/fact_movie_daily_snapshot")
)

(
    movie_genres.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_genres")
)

(
    movie_production_companies.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_production_companies")
)

(
    movie_production_countries.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_production_countries")
)

(
    movie_spoken_languages.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_spoken_languages")
)

(
    movie_origin_countries.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_origin_countries")
)

(
    movie_cast.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_cast")
)

(
    movie_crew.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_crew")
)

(
    movie_keywords.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_keywords")
)

(
    movie_release_dates.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_release_dates")
)

(
    movie_videos.write
    .mode("append")
    .parquet(f"{OUT_ROOT}/movie_videos")
)

(
    dim_genre.write
    .mode("overwrite")
    .parquet(f"{OUT_ROOT}/dim_genre")
)

(
    dim_company.write
    .mode("overwrite")
    .parquet(f"{OUT_ROOT}/dim_company")
)

(
    dim_keyword.write
    .mode("overwrite")
    .parquet(f"{OUT_ROOT}/dim_keyword")
)

(
    dim_language.write
    .mode("overwrite")
    .parquet(f"{OUT_ROOT}/dim_language")
)

(
    dim_country.write
    .mode("overwrite")
    .parquet(f"{OUT_ROOT}/dim_country")
)

(
    dim_person.write
    .mode("overwrite")
    .parquet(f"{OUT_ROOT}/dim_person")
)
spark.stop()
