'''Issues:
    1. What is static files take more time to ingest than batch files? We shouldnt move to Silver ETL but it does right now.
    2. [Low Priority] DAG fails on 4th run - it should skip ingestion for all but it fails it instead
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta
import subprocess

# ------------------------
# CONFIG
# ------------------------

PROJECT_ROOT = "/home/void/CDAC/Big-Data-Project"
HDFS_BRONZE = "/data_lake/bronze"

BATCH_BASE = f"{PROJECT_ROOT}/BigDataFiles/batches"
RAW_BASE   = f"{PROJECT_ROOT}/BigDataFiles"

SPARK_SUBMIT = "spark-submit --driver-memory 8g --executor-memory 8g"

STATIC_TABLES = [
    "artist_albums",
    "artist_genres",
    "track_artists",
    "available_markets"
]

BATCH_TABLES = [
    # "tracks",
    "albums",
    "artists"
]

MAX_BATCH = 3

# ------------------------
# HDFS HELPERS
# ------------------------

def hdfs_exists(path):
    return subprocess.call(["hdfs", "dfs", "-test", "-d", path]) == 0

# ------------------------
# NEXT BATCH DETECTION
# ------------------------

def detect_next_batch():
    for batch_id in range(1, MAX_BATCH + 1):
        path = f"{HDFS_BRONZE}/tracks/batch_id={batch_id}"
        if not hdfs_exists(path):
            print(f"Next batch to process: {batch_id}")
            return batch_id
    print("All batches already processed.")
    return None

# ------------------------
# STATIC GATE
# ------------------------

def should_run_static(table):
    path = f"{HDFS_BRONZE}/{table}"
    exists = hdfs_exists(path)
    print(f"Static check {table}: exists={exists}")
    return not exists

# ------------------------
# BATCH GATE
# ------------------------

def should_run_any_batch():
    return detect_next_batch() is not None

# ------------------------
# DAG
# ------------------------

dag = DAG(
    dag_id="spotify_bronze_pipeline_v4",
    schedule_interval=timedelta(minutes=40),   # or None for manual
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spotify", "bronze"],
)

# ------------------------
# INIT
# ------------------------

init_datalake = BashOperator(
    task_id="init_datalake",
    bash_command=f"python {PROJECT_ROOT}/bronze_check.py",
    dag=dag
)

# ------------------------
# STATIC TABLE INGESTION
# ------------------------

static_ingests = []

for table in STATIC_TABLES:

    gate = ShortCircuitOperator(
        task_id=f"should_ingest_static_{table}",
        python_callable=lambda t=table: should_run_static(t),
        dag=dag
    )

    ingest = BashOperator(
        task_id=f"ingest_static_{table}",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_ROOT}/bronze_ingest.py {table} static {RAW_BASE}",
        dag=dag
    )

    init_datalake >> gate >> ingest
    static_ingests.append(ingest)

# ------------------------
# BATCH BRANCH GATE (SINGLE)
# ------------------------

batch_gate = ShortCircuitOperator(
    task_id="should_ingest_next_batch",
    python_callable=should_run_any_batch,
    dag=dag
)

init_datalake >> batch_gate

# ------------------------
# BATCH TABLE INGESTION
# ------------------------

batch_ingests = []

for table in BATCH_TABLES:

    ingest = BashOperator(
        task_id=f"ingest_{table}_batch",
        bash_command=f"""
BATCH_ID=$(python - << 'EOF'
from subprocess import call
def hdfs_exists(p): return call(["hdfs","dfs","-test","-d",p])==0
for b in [1,2,3]:
    if not hdfs_exists(f"/data_lake/bronze/albums/batch_id={{b}}"):
        print(b)
        break
EOF
)
echo "Processing batch $BATCH_ID for {table}"
{SPARK_SUBMIT} {PROJECT_ROOT}/bronze_ingest.py {table} $BATCH_ID {BATCH_BASE}
""",
        dag=dag
    )

    batch_gate >> ingest
    batch_ingests.append(ingest)

# ------------------------
# SILVER PLACEHOLDER
# ------------------------

silver = BashOperator(
    task_id="silver_etl_starts",
    bash_command="echo 'Silver ETL starts here...'",
    trigger_rule="none_failed_min_one_success",
    dag=dag
)

for b in batch_ingests:
    b >> silver
