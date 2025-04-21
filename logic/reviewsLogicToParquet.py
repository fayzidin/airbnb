from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BQ to Parquet") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

bq_table = "airbnb-448411.airbnb.reviews_table"
output_path = "gs://airbnb-chicago/outputs/reviews/"

df = spark.read.format("bigquery").option("table", bq_table).load()


df.write.parquet(output_path, mode="overwrite")

# Stop the Spark session
spark.stop()
