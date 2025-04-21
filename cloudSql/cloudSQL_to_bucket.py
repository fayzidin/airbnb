from pyspark.sql import SparkSession
import os

try:
    from google.cloud import secretmanager
except ImportError:
    import subprocess
    import sys
    print("Installing google-cloud-secret-manager...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "google-cloud-secret-manager"])
    from google.cloud import secretmanager

def get_secret(project_id, secret_id, version="latest"):
    """Retrieve secret from Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(name=name) #(request={"name": name})
    payload = response.payload.data.decode("UTF-8")

    return payload

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Raw Data to GCS") \
    .config("spark.jars", "gs://airbnb-chicago/jars/postgresql-42.6.2.jar") \
    .getOrCreate()

#------------------------------------------------
# secret_ids = {
#     "db_username": f"projects/{project_id}/secrets/db-username/versions/1",
#     "db_password": f"projects/{project_id}/secrets/postgres-defense/versions/1",
#     "jdbc": f"projects/{project_id}/secrets/jdbc/versions/1"
# }

# def get_secret(secret_path):
#     client = secretmanager.SecretManagerServiceClient()
#     response = client.access_secret_version(name=secret_path)
#     return response.payload.data.decode("UTF-8")
#------------------------------------------------


# Database connection details
project_id = "869904895744" #os.environ.get("GOOGLE_CLOUDPROJECT")
db_user = get_secret(project_id,"db_username")
db_url = get_secret(project_id, "jdbc")
db_password = get_secret(project_id, "db_password")

# Tables to extract
tables = ["Sales.SalesOrderDetail", "Sales.SalesTerritory", "Sales.SalesOrderHeader"]

# GCS bucket details
gcs_bucket = "gs://airbnb-chicago/raw_data_adv/"

# Load each table and save as Parquet
for table in tables:
    print(f"Loading table: {table}")
    try:
        df = spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
        # Save as Parquet in GCS
        df.write \
            .mode("overwrite") \
            .parquet(os.path.join(gcs_bucket, table))

        print(f"Raw data {table} loaded to GCS successfully!")

    except Exception as e:
        print(f"Error loading {table}: {str(e)}")

spark.stop()