import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

HDFS_ROOT = "hdfs://localhost:9000"

if len(sys.argv) != 4:
    print("Usage:")
    print("  ingest_bronze.py <entity> <mode> <input_base>")
    print("")
    print("  For batch tables:")
    print("    ingest_bronze.py tracks 1 /home/void/CDAC/Big-Data-Project/BigDataFiles/batches")
    print("")
    print("  For static tables:")
    print("    ingest_bronze.py artist_albums static /home/void/CDAC/Big-Data-Project/BigDataFiles")
    sys.exit(1)

entity = sys.argv[1]    # tracks / albums / artists / artist_albums / ...
mode   = sys.argv[2]    # batch_id (1/2/3) OR "static"
base   = sys.argv[3]

spark = SparkSession.builder \
    .appName(f"BronzeIngest-{entity}-{mode}") \
    .enableHiveSupport() \
    .getOrCreate()

# Resolve paths
if mode == "static":
    input_path = os.path.join(base, f"{entity}.parquet")
    output_path = f"{HDFS_ROOT}/data_lake/bronze/{entity}"
    batch_id_value = -1
else:
    batch_id = int(mode)
    input_path = os.path.join(base, f"batch_{batch_id}", f"{entity}.parquet")
    output_path = f"{HDFS_ROOT}/data_lake/bronze/{entity}/batch_id={batch_id}"
    batch_id_value = batch_id

print(f"Entity: {entity}")
print(f"Mode:   {mode}")
print(f"Read:   {input_path}")
print(f"Write:  {output_path}")

# Read source
df = spark.read.parquet(input_path)

# Add Bronze metadata
df_bronze = df.withColumn("ingestion_ts", current_timestamp()).withColumn("batch_id", lit(batch_id_value))

# Write to HDFS Bronze (idempotent)
df_bronze.write.mode("overwrite").parquet(output_path)


print("Bronze ingestion complete.")

spark.stop()

'''Command to run - 
spark-submit \
  --driver-memory 8g \
  --executor-memory 8g \
  bronze_ingest.py artist_albums static /home/void/CDAC/Big-Data-Project/BigDataFiles

spark-submit \
  --driver-memory 8g \
  --executor-memory 8g \
  bronze_ingest.py tracks 3 /home/void/CDAC/Big-Data-Project/BigDataFiles/batches

'''