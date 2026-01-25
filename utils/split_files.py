import os
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc

BASE_PATH = "/home/void/BigDataFiles"
OUT_BASE  = "/home/void/BigDataFiles/batches"

FILES = ["tracks.parquet", "albums.parquet", "artists.parquet"]

BATCH_1_TS = {
    1741824000000, 1742428800000, 1743033600000, 1743638400000, 1744243200000, 1744848000000
}

BATCH_2_TS = {
    1745452800000, 1746057600000, 1746662400000, 1747267200000, 1747872000000, 1748476800000,
    1749081600000, 1749686400000, 1750291200000, 1750896000000, 1751500800000, 1752105600000,
    1752710400000, 1753315200000, 1753920000000, 1754524800000, 1755129600000
}

BATCH_3_TS = {
    1755734400000, 1756339200000, 1756944000000, 1757548800000, 1758153600000, 1758758400000,
    1759363200000, 1759968000000, 1760572800000, 1761177600000, 1761782400000, 1762387200000,
    1762992000000
}

BATCHES = {
    "batch_1": BATCH_1_TS,
    "batch_2": BATCH_2_TS,
    "batch_3": BATCH_3_TS,
}

os.makedirs(OUT_BASE, exist_ok=True)

def split_file(filename):
    in_path = os.path.join(BASE_PATH, filename)
    print(f"\n=== Processing {filename} ===")

    pf = pq.ParquetFile(in_path)

    writers = {}
    counts = {b: 0 for b in BATCHES}

    for batch_name in BATCHES:
        out_dir = os.path.join(OUT_BASE, batch_name)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, filename)
        writers[batch_name] = None

    for i in range(pf.num_row_groups):
        table = pf.read_row_group(i)
        fetched = table["fetched_at"]

        for batch_name, ts_set in BATCHES.items():
            mask = pc.is_in(fetched, value_set=pa.array(list(ts_set), type=pa.int64()))
            filtered = table.filter(mask)

            if filtered.num_rows == 0:
                continue

            if writers[batch_name] is None:
                writers[batch_name] = pq.ParquetWriter(
                    os.path.join(OUT_BASE, batch_name, filename),
                    filtered.schema
                )

            writers[batch_name].write_table(filtered)
            counts[batch_name] += filtered.num_rows

    for w in writers.values():
        if w:
            w.close()

    for b, c in counts.items():
        print(f"  {b}: {c:,} rows")

for f in FILES:
    split_file(f)

print("\nDone.")
