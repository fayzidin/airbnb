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
PYSPARK_SCRIPT_URI = f"gs://airbnb-chicago/logic/cloudSQL_to_bucket.py"
JDBC_JAR = "gs://airbnb-chicago/jars/postgresql-42.6.2.jar"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "loading_tables_from_SQL_instance",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or define a schedule
    catchup=False,
) as dag:
    
    submit_CloudSQL_job = DataprocSubmitJobOperator(
        task_id="submit_CloudSQL_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_SCRIPT_URI,
                "jar_file_uris": [JDBC_JAR]
            },
        },
    )

    # Define DAG dependencies
    submit_CloudSQL_job