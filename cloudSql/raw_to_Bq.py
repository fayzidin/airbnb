from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Transform and Load to BigQuery") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1") \
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
    .join(sales_person_df, sales_person_df.SalesPersonId == sales_orderHd_df.SalesPersonId, "inner") \
    .join(sales_orderDt_df, sales_orderDt_df.SalesOrderID  == sales_orderHd_df.SalesOrderID, "inner") \
    .join(territory_df, territory_df.TerritoryID == sales_orderHd_df.TerritoryID, "inner") \
    .select(
        sales_orderHd_df.SalesOrderID,
        sales_orderHd_df.OrderDate,
        sales_orderHd_df.SubTotal,
        sales_person_df.SalesLastYear,
        territory_df.Name,
        territory_df.CountryRegionCode,
        sales_orderDt_df.OrderQty
        )


# aggregated fact table
agg_sales_df = fact_sales_df \
    .groupBy(
        date_format(col("OrderDate"), "yyyy-MM-dd").alias("order_day"),
        date_format(col("OrderDate"), "yyyy-MM").alias("order_month"),
        col("SalesOrderID")
        col("SubTotal")
    ) \
    .agg(
        sum("OrderQty").alias("total_quantity"),
        sum("SubTotal").alias("sales_subtotal")
    )

# Write to BigQuery
fact_sales_df.write \
    .format("bigquery") \
    .option("table", "airbnb-448411:AdventureWorks.fact_sales") \
    .mode("overwrite") \
    .save()

agg_sales_df.write \
    .format("bigquery") \
    .option("table", "airbnb-448411:AdventureWorks.agg_sales") \
    .mode("overwrite") \
    .save()

print("Data transformed and loaded to BigQuery successfully!")
spark.stop()