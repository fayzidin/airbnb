from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitHiveJobOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago

# Configuration Variables
PROJECT_ID = "airbnb-448411"
REGION = "us-central1"  # Change based on your cluster region
CLUSTER_NAME = "cluster-a7d1"  # Use your existing Dataproc cluster name
BUCKET_NAME = "airbnb-chicago"  # GCS bucket where scripts are stored
HIVE_SCRIPT_URI = f"gs://airbnb-chicago/logic/csvToParquet.hql"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "calendar_to_par_hive",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or define a schedule
    catchup=False,
) as dag:

    # Submit Hive Job to the Existing Cluster
    submit_hive_calendar_job = DataprocSubmitHiveJobOperator(
        task_id="submit_hive_calendar_job",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        query_uri=HIVE_SCRIPT_URI,
    )
    # Define DAG dependencies
    submit_hive_calendar_job
