from pyspark.sql import SparkSession
from google.cloud import secretmanager
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Raw Data to GCS") \
    .config("spark.jars", "gs://airbnb-chicago/jars/postgresql-42.6.2.jar") \
    .getOrCreate()

project_id = os.environ.get("GOOGLE_CLOUDPROJECT")

secret_ids = {
    "db_username": f"projects/{project_id}/secrets/db-username/versions/latest",
    "db_password": f"projects/{project_id}/secrets/postgres-defense/versions/1",
    "jdbc": f"projects/{project_id}/secrets/jdbc/versions/latest"
}

# Database connection details
db_url = get_secret(secret_ids["jdbc"])
db_user = get_secret(secret_ids["db_username"])
db_password = get_secret(secret_ids["db_password"])

# Tables to extract
tables = ["Sales.Customer", "Sales.SalesTerritory", "Person.Person", "Sales.SalesOrderHeader", "Sales.SalesOrderDetail"]

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