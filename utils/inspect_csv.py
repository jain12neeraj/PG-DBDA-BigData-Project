import os
import csv
import pyarrow.parquet as pq

BASE_DIR = "/home/void/BigDataFiles"
OUTPUT_CSV = os.path.join(BASE_DIR, "parquet_inventory.csv")

rows = []

for fname in sorted(os.listdir(BASE_DIR)):
    if not fname.endswith(".parquet"):
        continue

    path = os.path.join(BASE_DIR, fname)

    print(f"Processing: {fname}")

    try:
        pf = pq.ParquetFile(path)

        columns = pf.schema.names
        num_rows = pf.metadata.num_rows
        num_columns = pf.metadata.num_columns
        num_row_groups = pf.metadata.num_row_groups
        schema_str = str(pf.schema).replace("\n", " ").replace("  ", " ")

        rows.append({
            "file_name": fname,
            "path": path,
            "num_rows": num_rows,
            "num_columns": num_columns,
            "num_row_groups": num_row_groups,
            "columns": "|".join(columns),
            "schema": schema_str
        })

    except Exception as e:
        print(f"ERROR reading {fname}: {e}")

# Write CSV
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "file_name",
            "path",
            "num_rows",
            "num_columns",
            "num_row_groups",
            "columns",
            "schema",
        ],
    )
    writer.writeheader()
    writer.writerows(rows)

print("\n✅ CSV written to:")
print(OUTPUT_CSV)
