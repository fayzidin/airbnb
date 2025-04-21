-- SET hive.exec.dynamic.partition = true;
-- SET hive.exec.dynamic.partition.mode = nonstrict;
-- SET hive.exec.compress.output = true;
-- SET hive.exec.compress.intermediate=true;
-- SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
-- SET parquet.compression=SNAPPY;  -- Ensure correct case
-- SET mapreduce.map.memory.mb=4096;
-- SET mapreduce.reduce.memory.mb=8192;
-- SET hive.auto.convert.join=false;

DROP TABLE csv_calendar_table_new1;
DROP TABLE prq_calendar_table_new1;

CREATE EXTERNAL TABLE csv_calendar_table_new1 (
    listing_id INTEGER, 
    `date` DATE,    
    available BOOLEAN, 
    price INTEGER,
    adjusted_price STRING,
    minimum_nights INTEGER, 
    maximum_nights INTEGER 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'gs://airbnb-chicago/calendar-CSV';

msck repair TABLE csv_calendar_table_new1;


CREATE EXTERNAL TABLE prq_calendar_table_new1 (
    listing_id INTEGER, 
    `date` DATE,    
    available BOOLEAN, 
    price INTEGER,
    adjusted_price STRING,
    minimum_nights INTEGER, 
    maximum_nights INTEGER
)
STORED AS PARQUET LOCATION 'gs://airbnb-chicago/outputs/parquet_calendar';

INSERT OVERWRITE TABLE prq_calendar_table_new1
SELECT * FROM csv_calendar_table_new1;