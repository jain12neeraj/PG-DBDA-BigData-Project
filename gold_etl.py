from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# -----------------------
# SPARK SESSION
# -----------------------
spark = (
    SparkSession.builder
    .appName("Spotify Gold ETL")
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "16g")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", 200)
    .config("spark.sql.adaptive.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

SILVER_BASE = "hdfs://localhost:9000/data_lake/silver"
GOLD_BASE = "hdfs://localhost:9000/data_lake/gold"

	

# Core cleaned tables
artists_clean = spark.read.parquet(f"{SILVER_BASE}/artists_clean")
albums_clean = spark.read.parquet(f"{SILVER_BASE}/albums_clean")
tracks_clean = spark.read.parquet(f"{SILVER_BASE}/tracks_clean")

# Expanded relationship tables
track_artist_expanded = spark.read.parquet(f"{SILVER_BASE}/track_artist_expanded")
artist_album_expanded = spark.read.parquet(f"{SILVER_BASE}/artist_album_expanded")
artist_genre_expanded = spark.read.parquet(f"{SILVER_BASE}/artist_genre_expanded")
market_expanded = spark.read.parquet(f"{SILVER_BASE}/market_expanded")

# Aggregates
track_artist_aggregates = spark.read.parquet(f"{SILVER_BASE}/track_artist_aggregates")
album_track_aggregates = spark.read.parquet(f"{SILVER_BASE}/album_track_aggregates")
artist_collaboration_metrics = spark.read.parquet(f"{SILVER_BASE}/artist_collaboration_metrics")




## Core cleaned tables
# artists_clean = spark.read.parquet(f"{SILVER_BASE}/artists_clean").cache()
# albums_clean = spark.read.parquet(f"{SILVER_BASE}/albums_clean").cache()
# tracks_clean = spark.read.parquet(f"{SILVER_BASE}/tracks_clean").cache()

## Expanded relationship tables
# track_artist_expanded = spark.read.parquet(f"{SILVER_BASE}/track_artist_expanded").cache()
# artist_album_expanded = spark.read.parquet(f"{SILVER_BASE}/artist_album_expanded").cache()
# artist_genre_expanded = spark.read.parquet(f"{SILVER_BASE}/artist_genre_expanded").cache()
# market_expanded = spark.read.parquet(f"{SILVER_BASE}/market_expanded").cache()

## Aggregates
# track_artist_aggregates = spark.read.parquet(f"{SILVER_BASE}/track_artist_aggregates").cache()
# album_track_aggregates = spark.read.parquet(f"{SILVER_BASE}/album_track_aggregates").cache()
# artist_collaboration_metrics = spark.read.parquet(f"{SILVER_BASE}/artist_collaboration_metrics").cache()

## Force caching
# for df in [artists_clean, albums_clean, tracks_clean, track_artist_expanded, 
#            artist_album_expanded, artist_genre_expanded, market_expanded,
#            track_artist_aggregates, album_track_aggregates, artist_collaboration_metrics]:
#     df.count()

# print("All Silver tables cached")


## 1. track_performance


# Calculate market counts efficiently
market_counts = (
    market_expanded
    .groupBy("track_rowid")
    .agg(countDistinct("market_code").alias("market_count"))
)

market_counts = market_counts.repartition(128)

track_performance = (
    tracks_clean
    # Join 1: Album release context (cached albums_clean)
    .join(
        albums_clean.select("album_rowid", "release_year"), 
        "album_rowid", 
        "left"
    )
    
    # Join 2: Artist aggregates (cached)
    .join(track_artist_aggregates, "track_rowid", "left")
    
    # Join 3: Market reach
    .join(market_counts, "track_rowid", "left")
    
    # Derive track age
    .withColumn(
        "track_age_days",
        datediff(current_date(), col("fetched_ts"))
    )
    
    # Fill nulls from left joins
    .fillna(0, subset=["market_count", "artist_count", "avg_artist_popularity", "avg_artist_followers"])
    
    
)

# Repartition for optimal write (large table)
track_performance = track_performance.repartition(128)

# WRITE
track_performance.write.mode("overwrite").parquet(f"{GOLD_BASE}/track_performance")
print("track_performance written (128 partitions)")

## 2. artist_performance


artist_tracks = (
    track_artist_expanded
    .groupBy("artist_rowid")
    .agg(countDistinct("track_rowid").alias("total_tracks"))
)

# Aggregate 2: Total albums per artist
artist_albums_agg = (
    artist_album_expanded
    .groupBy("artist_rowid")
    .agg(countDistinct("album_rowid").alias("total_albums"))
)

# Aggregate 3: Genre count per artist
artist_genres_agg = (
    artist_genre_expanded
    .groupBy("artist_rowid")
    .agg(countDistinct("genre").alias("genre_count"))
)

# Join all metrics together
artist_performance = (
    artists_clean
    .join(artist_tracks, "artist_rowid", "left")
    .join(artist_albums_agg, "artist_rowid", "left")
    .join(artist_genres_agg, "artist_rowid", "left")
    .join(artist_collaboration_metrics, "artist_rowid", "left")
    
    # Fill nulls
    .fillna(0)
)

# WRITE
artist_performance.write.mode("overwrite").parquet(f"{GOLD_BASE}/artist_performance")
print("artist_performance written (16 partitions)")

artist_performance=artist_performance.repartition(16)
## 3. genre_intelligence



# Aggregate 1: Track metrics per genre
genre_tracks = (
    track_artist_expanded
    .join(artist_genre_expanded, "artist_rowid")
    .join(tracks_clean, "track_rowid")
    .groupBy("genre")
    .agg(
        countDistinct("track_rowid").alias("track_count"),
        avg("popularity").alias("avg_track_popularity")
    )
)

# Aggregate 2: Market spread per genre
genre_markets = (
    track_artist_expanded
    .join(artist_genre_expanded, "artist_rowid")
    .join(market_expanded, "track_rowid")
    .groupBy("genre")
    .agg(countDistinct("market_code").alias("market_spread"))
)

# Join genre metrics
genre_intelligence = (
    genre_tracks
    .join(genre_markets, "genre", "left")
    .fillna(0)
    .coalesce(4)  # Small table
)

# WRITE
genre_intelligence.write.mode("overwrite").parquet(f"{GOLD_BASE}/genre_intelligence")
print("genre_intelligence written (4 partitions)")



## 4. collaboration_network_summary



collaboration_network_summary = (
    artists_clean
    .join(artist_collaboration_metrics, "artist_rowid", "left")
    .fillna(0)
)
collaboration_network_summary=collaboration_network_summary.repartition(16)
# WRITE
collaboration_network_summary.write.mode("overwrite").parquet(f"{GOLD_BASE}/collaboration_network_summary")
print("collaboration_network_summary written (16 partitions)")

# VERIFY
print("\Sample data:")
spark.read.parquet(f"{GOLD_BASE}/collaboration_network_summary").show(5, truncate=False)

print(f"Partition count: {spark.read.parquet(f'{GOLD_BASE}/collaboration_network_summary').rdd.getNumPartitions()}")




## 5. track_success_features


track_success_features = (
    tracks_clean
    # Join 1: Artist aggregates
    .join(track_artist_aggregates, "track_rowid", "left")
    
    # Join 2: Album release year
    .join(
        albums_clean.select("album_rowid", "release_year"), 
        "album_rowid", 
        "left"
    )
    
    # Join 3: Market counts
    .join(market_counts, "track_rowid", "left")
    
    # Fill nulls
    .fillna(0)
    
)

# Repartition for large table
track_success_features=track_success_features.repartition(128)
# WRITE
track_success_features.write.mode("overwrite").parquet(f"{GOLD_BASE}/track_success_features")
print("track_success_features written (128 partitions)")


## 6. release_trends


# Albums per year
album_releases = (
    albums_clean
    .groupBy("release_year")
    .agg(countDistinct("album_rowid").alias("album_releases"))
)

# Tracks per year (via album join)
track_releases = (
    tracks_clean
    .join(albums_clean.select("album_rowid", "release_year"), "album_rowid")
    .groupBy("release_year")
    .agg(countDistinct("track_rowid").alias("track_releases"))
)

release_trends = (
    album_releases
    .join(track_releases, "release_year", "left")
    .orderBy("release_year")
    .coalesce(2)  # Very small table
)

# WRITE
release_trends.write.mode("overwrite").parquet(f"{GOLD_BASE}/release_trends")
print("release_trends written (2 partitions)")

if __name__ == "__main__":
    print("Gold ETL")

