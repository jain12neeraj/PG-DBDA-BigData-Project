from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

BASE_PATH = "/home/void/CDAC/Big-Data-Project/BigDataFiles"
OUT_BASE  = "/home/void/CDAC/Big-Data-Project/BigDataFiles/batches"

FILES = ["tracks.parquet", "albums.parquet", "artists.parquet"]

BATCH_1_TS = [
    1741824000000, 1742428800000, 1743033600000, 1743638400000, 1744243200000, 1744848000000
]

BATCH_2_TS = [
    1745452800000, 1746057600000, 1746662400000, 1747267200000, 1747872000000, 1748476800000,
    1749081600000, 1749686400000, 1750291200000, 1750896000000, 1751500800000, 1752105600000,
    1752710400000, 1753315200000, 1753920000000, 1754524800000, 1755129600000
]

BATCH_3_TS = [
    1755734400000, 1756339200000, 1756944000000, 1757548800000, 1758153600000, 1758758400000,
    1759363200000, 1759968000000, 1760572800000, 1761177600000, 1761782400000, 1762387200000,
    1762992000000
]

BATCHES = {
    "batch_1": BATCH_1_TS,
    "batch_2": BATCH_2_TS,
    "batch_3": BATCH_3_TS,
}

spark = SparkSession.builder \
    .appName("SpotifyBatchSplitterSpark") \
    .getOrCreate()

os.makedirs(OUT_BASE, exist_ok=True)

for filename in FILES:
    print(f"\n=== Processing {filename} ===")

    in_path = os.path.join(BASE_PATH, filename)
    df = spark.read.parquet(in_path)

    # Cache so we don't re-read file 3 times
    df = df.repartition(128).cache()
    df.count()  # materialize cache once

    splits = {
        "batch_1": df.filter(col("fetched_at").isin(BATCH_1_TS)),
        "batch_2": df.filter(col("fetched_at").isin(BATCH_2_TS)),
        "batch_3": df.filter(col("fetched_at").isin(BATCH_3_TS)),
    }

    for batch_name, batch_df in splits.items():
        out_dir = os.path.join(OUT_BASE, batch_name)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, filename)

        print(f"  Writing {batch_name}...")

        (
            batch_df
            .repartition(32)  # control number of output files
            .write
            .mode("overwrite")
            .option("maxRecordsPerFile", 5_000_000)
            .parquet(out_path)
        )

    df.unpersist()

spark.stop()
print("\nDone.")