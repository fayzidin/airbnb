DROP TABLE prq_reviews_table_new1;
DROP TABLE csv_reviews_table_new1;

CREATE EXTERNAL TABLE csv_reviews_table_new1 (
    listing_id STRING,
    id STRING,  
    `date` STRING,  
    reviewer_id STRING,  
    reviewer_name STRING,  
    comments STRING  
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://airbnb-chicago/reviews-CSV/';

msck repair TABLE csv_reviews_table_new1;

CREATE TABLE prq_reviews_table_new1 (
    listing_id STRING,
    id STRING,  
    `date` STRING,  
    reviewer_id STRING,  
    reviewer_name STRING,  
    comments STRING
)
STORED AS PARQUET LOCATION 'gs://airbnb-chicago/outputs/parquet_reviews';

INSERT OVERWRITE TABLE prq_reviews_table_new1
SELECT * FROM csv_reviews_table_new1;