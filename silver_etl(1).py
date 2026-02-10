from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# -----------------------
# SPARK SESSION
# -----------------------
spark = (
    SparkSession.builder
    .appName("Spotify Silver ETL")
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "16g")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", 200)
    .config("spark.sql.adaptive.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

BRONZE_BASE = "hdfs://localhost:9000/data_lake/bronze"
SILVER_BASE = "hdfs://localhost:9000/data_lake/silver"

artists = spark.read.parquet(f"{BRONZE_BASE}/artists/batch_id=3")
albums = spark.read.parquet(f"{BRONZE_BASE}/albums/batch_id=3")
tracks = spark.read.parquet(f"{BRONZE_BASE}/tracks/batch_id=3")
artist_genres = spark.read.parquet(f"{BRONZE_BASE}/artist_genres")
track_artists = spark.read.parquet(f"{BRONZE_BASE}/track_artists")
artist_albums = spark.read.parquet(f"{BRONZE_BASE}/artist_albums")
available_markets = spark.read.parquet(f"{BRONZE_BASE}/available_markets")

print("✅ Bronze tables loaded (using natural partitioning)")


## Silver

### 1. artists_clean


artists_clean = (
    artists

    .dropDuplicates(["rowid"])
    .withColumn("fetched_ts", to_timestamp(col("fetched_at") / 1000))
    .withColumn("followers_total", coalesce(col("followers_total"), lit(0)))
    .withColumn("popularity", coalesce(col("popularity"), lit(0)))
    .select(
        col("rowid").alias("artist_rowid"),
        col("id").alias("artist_id"),
        "name",
        "followers_total",
        "popularity",
        "fetched_ts"
    )  
)
# 2. REPARTITION AFTER transformation (smaller data now)
# 617 MB / 77 MB per partition = ~8 partitions
artists_clean = artists_clean.repartition(8)

# 3. WRITE
artists_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/artists_clean")
print("artists_clean written (8 partitions)")


### 2. albums_clean

albums_clean = (
    albums
    # 1. TRANSFORM FIRST
    .dropDuplicates(["rowid"])
    
    # Normalize release_date based on precision
    .withColumn(
        "release_date_standardized",
        when(col("release_date_precision") == "year",
             concat(col("release_date"), lit("-01-01")))
        .when(col("release_date_precision") == "month",
             concat(col("release_date"), lit("-01")))
        .otherwise(col("release_date"))
    )
    
    # Safe date conversion
    .withColumn("release_date_parsed", to_date("release_date_standardized"))
    
    # Filter corrupted / ancient Spotify records
    .filter(
        col("release_date_parsed").isNotNull() &
        (year("release_date_parsed") >= 1900)
    )
    
    .withColumn("release_year", year("release_date_parsed"))
    .withColumn("fetched_ts", to_timestamp(col("fetched_at") / 1000))
    .withColumn("popularity", coalesce(col("popularity"), lit(0)))
    .withColumn("total_tracks", coalesce(col("total_tracks"), lit(0)))
    
    .select(
        col("rowid").alias("album_rowid"),
        col("id").alias("album_id"),
        "name",
        "album_type",
        "popularity",
        "release_date_parsed",
        "release_year",
        "total_tracks",
        "available_markets_rowid",
        "fetched_ts"
    )
)
# 2. REPARTITION AFTER transformation
# 4.4 GB / 137 MB per partition = ~32 partitions
albums_clean = albums_clean.repartition(32)
# 3. WRITE
albums_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/albums_clean")
print("albums_clean written (32 partitions)")

### 3. tracks_clean


tracks_clean = (
    tracks
    # 1. TRANSFORM FIRST
    .dropDuplicates(["rowid"])
    .withColumn("fetched_ts", to_timestamp(col("fetched_at") / 1000))
    .withColumn("duration_seconds", col("duration_ms") / 1000)
    .withColumn(
        "duration_bucket",
        when(col("duration_seconds") < 120, "short")
        .when(col("duration_seconds") < 240, "medium")
        .otherwise("long")
    )
    .withColumn("explicit_flag", col("explicit").cast("boolean"))
    .withColumn("popularity", coalesce(col("popularity"), lit(0)))
    .select(
        col("rowid").alias("track_rowid"),
        col("id").alias("track_id"),
        "name",
        "album_rowid",
        "duration_ms",
        "duration_seconds",
        "duration_bucket",
        "explicit_flag",
        "popularity",
        "available_markets_rowid",
        "fetched_ts"
    )
)

# 2. REPARTITION AFTER transformation
# 22.3 GB / 174 MB per partition = ~128 partitions
tracks_clean = tracks_clean.repartition(128)
# 3. WRITE
tracks_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/tracks_clean")
print("tracks_clean written (128 partitions)")

### CACHE REUSED DATAFRAMES


print("\Caching frequently used tables...")

artists_clean = spark.read.parquet(f"{SILVER_BASE}/artists_clean").cache()
albums_clean = spark.read.parquet(f"{SILVER_BASE}/albums_clean").cache()
tracks_clean = spark.read.parquet(f"{SILVER_BASE}/tracks_clean").cache()

# Force caching by triggering an action
artists_clean.count()
albums_clean.count()
tracks_clean.count()

print("Cached: artists_clean, albums_clean, tracks_clean")


### 4. track_artist_expanded


track_artist_expanded = (
    track_artists
    # 1. DEDUPE FIRST (reduces size before join)
    .dropDuplicates()
    
    # 2. JOIN without pre-repartition
    # Let Spark's AQE (Adaptive Query Execution) handle the shuffle
    .join(artists_clean, "artist_rowid", "left")
    
    .select(
        "track_rowid",
        "artist_rowid",
        "artist_id",
        col("popularity").alias("artist_popularity"),
        "followers_total"
    )
)
print("defined")

# 3. SINGLE repartition after join by the key used downstream (track_rowid)
# This is better than repartitioning twice
track_artist_expanded = track_artist_expanded.repartition(128)
# 4. WRITE
track_artist_expanded.write.mode("overwrite").parquet(f"{SILVER_BASE}/track_artist_expanded")
print("track_artist_expanded written (32 partitions)")

### 5. artist_album_expanded


artist_album_expanded = (
    artist_albums
    # 1. DEDUPE
    .dropDuplicates()
    
    # 2. REPARTITION BY JOIN KEY (album_rowid)
    .repartition(16, "album_rowid")
    
    # 3. JOIN with albums_clean (which is cached - fast!)
    .join(albums_clean, "album_rowid", "left")
    
    .select(
        "artist_rowid",
        "album_rowid",
        "release_year",
        col("popularity").alias("album_popularity")
    )
)

# 4. FINAL PARTITIONING 
artist_album_expanded = artist_album_expanded.repartition(128)
# 5. WRITE
artist_album_expanded.write.mode("overwrite").parquet(f"{SILVER_BASE}/artist_album_expanded")
print("artist_album_expanded written (16 partitions)")


### 6. artist_genre_expanded


artist_genre_expanded = (
    artist_genres
    # 1. DEDUPE & TRANSFORM
    .dropDuplicates()
    .withColumn("genre", lower(trim(col("genre"))))
    
    # 2. COALESCE (not repartition) - no shuffle needed for tiny data
    # This just combines partitions, doesn't redistribute
    .coalesce(2)
)

# 3. WRITE
artist_genre_expanded.write.mode("overwrite").parquet(f"{SILVER_BASE}/artist_genre_expanded")
print("artist_genre_expanded written (2 partitions)")


### 7. market_expanded


# STEP 1: Explode markets (this creates billions of rows!)
markets_exploded = (
    available_markets
    .withColumn("market_code", explode(split(col("available_markets"), ",")))
    .select(
        col("rowid").alias("available_markets_rowid"), 
        "market_code"
    )
    # Repartition by market_code for efficient join
    .repartition(64, "market_code")
)

# STEP 2: Join with tracks_clean (cached - good!)
# We partition tracks by available_markets_rowid before join
tracks_for_markets = (
    tracks_clean
    .select("track_rowid", "available_markets_rowid")
    .repartition(64, "available_markets_rowid")
)

market_expanded = (
    tracks_for_markets
    .join(markets_exploded, "available_markets_rowid", "left")
    .select("track_rowid", "market_code")
    # Repartition by market_code for future market-based queries
    .repartition(64, "market_code")
)

# 3. WRITE
market_expanded.write.mode("overwrite").parquet(f"{SILVER_BASE}/market_expanded")
print("market_expanded written (64 partitions)")

### 8. track_artist_aggregates 


track_artist_aggregates = (
    track_artist_expanded
    # 1. GROUP BY (Spark will shuffle based on spark.sql.shuffle.partitions = 200)
    .groupBy("track_rowid")
    .agg(
        countDistinct("artist_rowid").alias("artist_count"),
        avg("artist_popularity").alias("avg_artist_popularity"),
        avg("followers_total").alias("avg_artist_followers")
    )
    # 2. COALESCE after aggregation (data is much smaller now)
    .coalesce(16)
)

# 3. WRITE
track_artist_aggregates.write.mode("overwrite").parquet(f"{SILVER_BASE}/track_artist_aggregates")
print("track_artist_aggregates written (16 partitions)")

### 9. album_track_aggregates


album_track_aggregates = (
    tracks_clean
    # 1. GROUP BY
    .groupBy("album_rowid")
    .agg(
        count("*").alias("tracks_per_album"),
        avg("popularity").alias("avg_track_popularity"),
        sum("duration_seconds").alias("album_duration_total")
    )
    # 2. COALESCE (small result)
    .coalesce(8)
)

# 3. WRITE
album_track_aggregates.write.mode("overwrite").parquet(f"{SILVER_BASE}/album_track_aggregates")
print("album_track_aggregates written (8 partitions)")


### 10. artist_collaboration_metrics


# OPTIMIZATION: Only process tracks with reasonable artist counts (≤ 4)
# This prevents explosion for compilation albums with 20+ artists
tracks_with_few_artists = (
    track_artist_expanded
    .groupBy("track_rowid")
    .agg(count("*").alias("artist_count"))
    .filter(col("artist_count") <= 4)  # Limit to avoid N² explosion
)

# Filter track_artist_expanded to reasonable tracks
track_artist_filtered = (
    track_artist_expanded
    .join(tracks_with_few_artists, "track_rowid", "inner")
    .select("track_rowid", "artist_rowid")
)

# Self-join to find pairs
ta = track_artist_filtered.alias("a")

pairs = (
    ta.join(
        track_artist_filtered.alias("b"), 
        "track_rowid"
    )
    .filter(col("a.artist_rowid") < col("b.artist_rowid"))  # Avoid duplicates (A-B same as B-A)
    .select(
        col("a.artist_rowid").alias("artist_rowid"),
        col("b.artist_rowid").alias("collaborator")
    )
)

artist_collaboration_metrics = (
    pairs
    .groupBy("artist_rowid")
    .agg(
        countDistinct("collaborator").alias("unique_collaborators"),
        count("*").alias("collaboration_count")
    )
    .coalesce(8)
)

# WRITE
artist_collaboration_metrics.write.mode("overwrite").parquet(f"{SILVER_BASE}/artist_collaboration_metrics")
print("artist_collaboration_metrics written (8 partitions)")


if __name__ == "__main__":
    print("Silver ETL")
