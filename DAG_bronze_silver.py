from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess

# ------------------------
# CONFIG
# ------------------------

PROJECT_ROOT = "/home/void/CDAC/Big-Data-Project"
HDFS_BRONZE = "/data_lake/bronze"

BATCH_BASE = f"{PROJECT_ROOT}/BigDataFiles/batches"
RAW_BASE   = f"{PROJECT_ROOT}/BigDataFiles"

SPARK_SUBMIT = "spark-submit --driver-memory 16g --executor-memory 16g --conf spark.ui.enabled=false"

STATIC_TABLES = [
    "artist_albums",
    "artist_genres",
    "track_artists",
    "available_markets"
]

BATCH_TABLES = [
    "tracks",
    "albums",
    "artists"
]

MAX_BATCH = 3

# ------------------------
# HDFS HELPERS
# ------------------------

def hdfs_exists(path):
    """
    Check if HDFS path exists AND has _SUCCESS marker.
    This ensures data was completely written by Spark.
    """
    dir_exists = subprocess.call(["hdfs", "dfs", "-test", "-d", path]) == 0
    success_exists = subprocess.call(["hdfs", "dfs", "-test", "-f", f"{path}/_SUCCESS"]) == 0
    
    if dir_exists and not success_exists:
        print(f"WARNING: Directory {path} exists but _SUCCESS marker missing (incomplete write?)")
    
    return dir_exists and success_exists
    
   
# ------------------------
# BATCH DETECTION (CORRECT)
# ------------------------

def batch_done_for_all_tables(batch_id):
    for t in BATCH_TABLES:
        path = f"{HDFS_BRONZE}/{t}/batch_id={batch_id}"
        if not hdfs_exists(path):
            return False
    return True

def detect_next_batch():
    for batch_id in range(1, MAX_BATCH + 1):
        if not batch_done_for_all_tables(batch_id):
            print(f"Next batch to process: {batch_id}")
            return batch_id
    print("All batches already processed.")
    return None

# ------------------------
# STATIC CHECK
# ------------------------


# ------------------------
# BRANCHING
# ------------------------

def branch_batch():
    return "run_batch" if detect_next_batch() is not None else "end_pipeline"

# ------------------------
# XCOM: PUSH BATCH ID
# ------------------------

def push_next_batch(**ctx):
    b = detect_next_batch()
    if b is None:
        raise ValueError("push_next_batch called but no batch exists!")
    ctx["ti"].xcom_push(key="batch_id", value=b)

# ------------------------
# DAG
# ------------------------

dag = DAG(
    dag_id="spotify_bronze_pipeline_v11",
    schedule_interval=timedelta(minutes=60),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=4,
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
# STATIC INGESTION (INDIVIDUAL BRANCHES)
# ------------------------

static_done = EmptyOperator(
    task_id="static_done",
    trigger_rule="none_failed_min_one_success",
    dag=dag
)

# Create individual branch for each static table
for table in STATIC_TABLES:
    
    # Branch function with closure to capture table name
    def make_branch_func(table_name):
        def branch_func():
            path = f"{HDFS_BRONZE}/{table_name}"
            if hdfs_exists(path):
                print(f"{table_name} already exists, skipping")
                return f"skip_static_{table_name}"
            else:
                print(f"{table_name} missing, will ingest")
                return f"ingest_static_{table_name}"
        return branch_func
    
    branch = BranchPythonOperator(
        task_id=f"branch_static_{table}",
        python_callable=make_branch_func(table),
        dag=dag
    )
    
    ingest = BashOperator(
        task_id=f"ingest_static_{table}",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_ROOT}/bronze_ingest.py {table} static {RAW_BASE}",
        dag=dag
    )
    
    skip = EmptyOperator(
        task_id=f"skip_static_{table}",
        dag=dag
    )
    
    # Chain: init_datalake → branch → [ingest or skip] → static_done
    init_datalake >> branch >> [ingest, skip] >> static_done

# ------------------------
# BATCH BRANCH (AFTER STATIC)
# ------------------------

branch_batch_task = BranchPythonOperator(
    task_id="branch_batch",
    python_callable=branch_batch,
    dag=dag
)

static_done >> branch_batch_task

run_batch = EmptyOperator(task_id="run_batch", dag=dag)
end_pipeline = EmptyOperator(task_id="end_pipeline", dag=dag)

branch_batch_task >> [run_batch, end_pipeline]

# ------------------------
# PUSH BATCH ID
# ------------------------

push_batch = PythonOperator(
    task_id="push_batch_id",
    python_callable=push_next_batch,
    provide_context=True,
    dag=dag
)

run_batch >> push_batch

# ------------------------
# BATCH INGESTION (SEQUENTIAL WITH INDIVIDUAL BRANCHES)
# ------------------------

prev = push_batch

for table in BATCH_TABLES:
    
    # Branch function with closure to capture table name
    def make_batch_branch_func(table_name):
        def branch_func(**ctx):
            batch_id = ctx["ti"].xcom_pull(key="batch_id")
            path = f"{HDFS_BRONZE}/{table_name}/batch_id={batch_id}"
            
            if hdfs_exists(path):
                print(f"{table_name} batch {batch_id} already exists, skipping")
                return f"skip_batch_{table_name}"
            else:
                print(f"{table_name} batch {batch_id} missing, will ingest")
                return f"ingest_batch_{table_name}"
        return branch_func
    
    branch = BranchPythonOperator(
        task_id=f"branch_batch_{table}",
        python_callable=make_batch_branch_func(table),
        provide_context=True,
        dag=dag
    )
    
    ingest = BashOperator(
        task_id=f"ingest_batch_{table}",
        bash_command=f"""
BATCH_ID="{{{{ ti.xcom_pull(key='batch_id') }}}}"
echo "Processing batch $BATCH_ID for {table}"
{SPARK_SUBMIT} {PROJECT_ROOT}/bronze_ingest.py {table} $BATCH_ID {BATCH_BASE}
""",
        dag=dag
    )
    
    skip = EmptyOperator(
        task_id=f"skip_batch_{table}",
        dag=dag
    )
    
    # Convergence point for THIS table (unique per iteration)
    converge = EmptyOperator(
        task_id=f"batch_{table}_done",
        trigger_rule="none_failed_min_one_success",
        dag=dag
    )
    
    # Chain: prev → branch → [ingest or skip] → converge
    prev >> branch >> [ingest, skip] >> converge
    
    # Next iteration starts from this converge point
    prev = converge

# Final batch_done after ALL tables
batch_done = EmptyOperator(
    task_id="batch_done",
    dag=dag
)

prev >> batch_done


# ------------------------
# SILVER (ONLY IF BATCH RAN)
# ------------------------

silver = BashOperator(
    task_id="silver_etl_starts",
    bash_command="echo 'Silver ETL starts here...'",
    dag=dag
)

batch_done >> silver

