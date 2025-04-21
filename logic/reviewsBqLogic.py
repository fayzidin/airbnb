from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BigQuery Table Creation") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Configuration variables
input_path = "gs://airbnb-chicago/reviews-CSV/reviews.csv"
temp_gcs_bucket = "gs://airbnb-chicago/temp/"
output_bq_table = "airbnb-448411.airbnb.reviews_table"


# Read the CSV file into a DataFrame
df = spark.read.csv(input_path, header=True, inferSchema=True)


transformed_df = df

# Write the DataFrame to BigQuery (will create a table if it doesn't exist)
transformed_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket",temp_gcs_bucket) \
    .mode("overwrite") \
    .save(output_bq_table)

# Stop the Spark session
spark.stop()
