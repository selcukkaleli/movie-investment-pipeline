from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, count, avg, round
from pyspark.sql.functions import from_unixtime, to_timestamp

# 1. Spark session oluştur
spark = SparkSession.builder \
    .appName("MovieLens Genre Explode") \
    .getOrCreate()

# GCS credentials
spark.conf.set("google.cloud.auth.service.account.json.keyfile", 
               "/credentials/gcp-key.json")
spark.conf.set("fs.gs.impl", 
               "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("fs.AbstractFileSystem.gs.impl", 
               "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

BUCKET = "movie-investment-pipeline-data-lake"
PROJECT = "movie-investment-pipeline"

# 2. GCS'ten oku
print("Reading data from GCS...")
ratings = spark.read.csv(
    f"gs://{BUCKET}/raw/movielens/ratings.csv",
    header=True,
    inferSchema=True
)
movies = spark.read.csv(
    f"gs://{BUCKET}/raw/movielens/movies.csv",
    header=True,
    inferSchema=True
)

print(f"Ratings count: {ratings.count()}")
print(f"Movies count: {movies.count()}")

# 3. Genre exploding
print("Exploding genres...")
movies_exploded = movies.withColumn(
    "genre",
    explode(split(col("genres"), "\\|"))
).drop("genres").filter(col("genre") != "(no genres listed)")

# 4. Ratings ile join
print("Joining ratings with movies...")
df = ratings.join(movies_exploded, on="movieId", how="inner")

# 4.5 Parquet write
df = df.withColumn(
    "rating_timestamp",
    to_timestamp(from_unixtime(col("timestamp")))
).drop("timestamp")

# 5. Parquet olarak GCS'e yaz
print("Writing parquet to GCS...")
df.write.mode("overwrite").parquet(
    f"gs://{BUCKET}/processed/movielens/ratings_with_genres/"
)
print("Parquet written successfully!")

# 6. BigQuery'e yaz
print("Writing to BigQuery...")
df.write \
    .format("bigquery") \
    .option("table", f"{PROJECT}.raw.ratings_with_genres") \
    .option("credentialsFile", "/credentials/gcp-key.json") \
    .option("partitionField", "rating_timestamp") \
    .option("partitionType", "MONTH") \
    .option("temporaryGcsBucket", BUCKET) \
    .mode("overwrite") \
    .save()

print("Done! BigQuery write successful!")
spark.stop()