import pyarrow.parquet as pq
from datetime import datetime
import os

BASE_DIR = "/home/void/BigDataFiles"

FILES = [
    "artists.parquet",
    "albums.parquet",
    "tracks.parquet",
]

for fname in FILES:
    path = os.path.join(BASE_DIR, fname)

    print("\n" + "=" * 120)
    print(f"FILE: {fname}")
    print("=" * 120)

    pf = pq.ParquetFile(path)

    counts = {}

    for rg in range(pf.metadata.num_row_groups):
        table = pf.read_row_group(rg, columns=["fetched_at"])
        arr = table.column(0)

        # Convert this chunk only
        values = arr.to_pylist()

        for v in values:
            counts[v] = counts.get(v, 0) + 1

    # Print results sorted by timestamp
    print("\nFetched_at distribution:")
    print("timestamp -> datetime (UTC) -> row_count")
    print("-" * 80)

    total = 0

    for k in sorted(counts):
        # timestamps are in milliseconds
        dt = datetime.utcfromtimestamp(k / 1000)

        cnt = counts[k]
        total += cnt

        print(f"{k} -> {dt.strftime('%Y-%m-%d %H:%M:%S')} UTC -> {cnt:,}")

    print("-" * 80)
    print("Total rows accounted for:", f"{total:,}")
    print("Total rows in file:      ", f"{pf.metadata.num_rows:,}")

print("\n✅ Done.")
