import os
import pyarrow.parquet as pq
import pandas as pd 

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 0)
pd.set_option("display.max_colwidth", None)


BASE_DIR = "/home/void/CDAC/Big-Data-Project/BigDataFiles"


for fname in sorted(os.listdir(BASE_DIR)):
    if not fname.endswith(".parquet"):
        continue

    path = os.path.join(BASE_DIR, fname)

    print("\n" + "=" * 120)
    print(f"📄 File: {path}")
    print("=" * 120)

    try:
        pf = pq.ParquetFile(path)

        # Column names
        columns = pf.schema.names
        print("\nColumns:")
        print(columns)

        # Read only first row group (small chunk)
        table = pf.read_row_group(0)

        # Convert to pandas and take first 4 rows
        df = table.to_pandas().head(4)

        print("\nFirst 4 rows:")
        print(df)

    except Exception as e:
        print("❌ Error reading file:", e)


BASE_DIR = "/home/void/CDAC/Big-Data-Project/BigDataFiles/batches/batch_1"

for fname in sorted(os.listdir(BASE_DIR)):
    if not fname.endswith(".parquet"):
        continue

    path = os.path.join(BASE_DIR, fname)

    print("\n" + "=" * 120)
    print(f"📄 File: {path}")
    print("=" * 120)

    try:
        pf = pq.ParquetFile(path)

        # Column names
        columns = pf.schema.names
        print("\nColumns:")
        print(columns)

        # Read only first row group (small chunk)
        table = pf.read_row_group(0)

        # Convert to pandas and take first 4 rows
        df = table.to_pandas().head(4)

        print("\nFirst 4 rows:")
        print(df)

    except Exception as e:
        print("❌ Error reading file:", e)

print("\n✅ Done.")

