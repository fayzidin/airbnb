from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('BQ_to_Parquet').getOrCreate()

bq_table = "airbnb-448411.airbnb.listings_table"

df = spark.read.format('bigquery').option('table', bq_table).load()


output_path = "gs://airbnb-chicago/outputs/listings/"
 
df.write.parquet(output_path, mode='overwrite')

# Stop the Spark session
spark.stop()
