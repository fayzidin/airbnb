from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Transform and Load to BigQuery") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

# GCS bucket details
gcs_bucket = "gs://airbnb-chicago/raw_data_adv/"

# Read raw data from GCS
territory_df = spark.read.parquet(os.path.join(gcs_bucket, "Sales.SalesTerritory"))
sales_person_df = spark.read.parquet(os.path.join(gcs_bucket, "Sales.SalesPerson"))
sales_orderHd_df = spark.read.parquet(os.path.join(gcs_bucket, "Sales.SalesOrderHeader"))
sales_orderDt_df = spark.read.parquet(os.path.join(gcs_bucket, "Sales.SalesOrderDetail"))

#Join fact and dim tables
fact_sales_df = sales_orderHd_df \
    .join(sales_person_df, sales_person_df.businessentityid == sales_orderHd_df.salespersonid, "inner") \
    .join(sales_orderDt_df, sales_orderDt_df.salesorderid  == sales_orderHd_df.salesorderid, "inner") \
    .join(territory_df, territory_df.territoryid == sales_orderHd_df.territoryid, "inner") \
    .select(
        sales_orderHd_df.salesorderid,
        sales_orderHd_df.orderdate,
        sales_orderHd_df.subtotal,
        sales_person_df.saleslastyear,
        territory_df.name,
        territory_df.countryregioncode,
        sales_orderDt_df.orderqty
        )


# aggregated fact table
agg_sales_df = fact_sales_df \
    .groupBy(
        date_format(col("orderdate"), "yyyy-MM-dd").alias("order_day"),
        date_format(col("orderdate"), "yyyy-MM").alias("order_month"),
        col("salesorderid"),
        col("subtotal")
    ) \
    .agg(
        sum("orderqty").alias("total_quantity"),
        sum("subtotal").alias("sales_subtotal")
    )

temp_gcs_bucket = "gs://airbnb-chicago/temp/"
sales_table = "airbnb-448411.AdventureWorks.fact_sales"
agg_sales_table = "airbnb-448411.AdventureWorks.agg_sales"
# Write to BigQuery
fact_sales_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", temp_gcs_bucket) \
    .mode("overwrite") \
    .save(sales_table)

agg_sales_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", temp_gcs_bucket) \
    .mode("overwrite") \
    .save(agg_sales_table)

print("Data transformed and loaded to BigQuery successfully!")
spark.stop()