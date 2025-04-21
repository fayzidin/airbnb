SET bigquery.gcs.temp.bucket="gs://airbnb-chicago/tmp";
SET hive.execution.engine=mr;

CREATE EXTERNAL TABLE calendar_table (
    listing_id INTEGER, 
    `date` DATE,    
    available BOOLEAN, 
    price INTEGER,
    adjusted_price STRING,
    minimum_nights INTEGER, 
    maximum_nights INTEGER 

)
STORED BY 'com.google.cloud.hive.bigquery.BigQueryStorageHandler'
TBLPROPERTIES (
    'projectId'='airbnb-448411',
    'datasetId'='airbnb',
    'tableId'='calendar_table'
);


-- Create External Table for BigQuery: Table 2
CREATE EXTERNAL TABLE listings_table (
    id STRING,  
    listing_url STRING,  
    scrape_id STRING,  
    last_scraped STRING,  
    source STRING,  
    name STRING,  
    description STRING,  
    neighborhood_overview STRING,  
    picture_url STRING,  
    host_id STRING,  
    host_url STRING,  
    host_name STRING,  
    host_since STRING,  
    host_location STRING,  
    host_about STRING,  
    host_response_time STRING,  
    host_response_rate STRING,  
    host_acceptance_rate STRING,  
    host_is_superhost       STRING,  
    host_thumbnail_url      STRING, 
    host_picture_url        STRING,  
    host_neighbourhood      STRING,  
    host_listings_count     STRING,  
    host_total_listings_count       STRING,  
    host_verifications      STRING,  
    host_has_profile_pic        STRING,  
    host_identity_verified      STRING,  
    neighbourhood       STRING,  
    neighbourhood_cleansed      STRING,  
    neighbourhood_group_cleansed STRING,  
    latitude        STRING,  
    longitude       STRING,  
    property_type       STRING,  
    room_type       STRING,  
    accommodates        STRING,  
    bathrooms       STRING,  
    bathrooms_text      STRING,  
    bedrooms        STRING,  
    beds        STRING,  
    amenities       STRING,  
    price       STRING,  
    minimum_nights      STRING,  
    maximum_nights      STRING,  
    minimum_minimum_nights      STRING,  
    maximum_minimum_nights      STRING,  
    minimum_maximum_nights      STRING,  
    maximum_maximum_nights      STRING,  
    minimum_nights_avg_ntm      STRING,  
    maximum_nights_avg_ntm      STRING,  
    calendar_updated        STRING,  
    has_availability        STRING,  
    availability_30     STRING,  
    availability_60     STRING,  
    availability_90     STRING,  
    availability_365        STRING,  
    calendar_last_scraped       STRING,  
    number_of_reviews       STRING,  
    number_of_reviews_ltm       STRING,  
    number_of_reviews_l30d      STRING,  
    first_review        STRING,  
    last_review     STRING,  
    review_scores_rating        STRING,  
    review_scores_accuracy      STRING,  
    review_scores_cleanliness       STRING,  
    review_scores_checkin       STRING,  
    review_scores_communication     STRING,  
    review_scores_location      STRING,  
    review_scores_value     STRING,  
    license     STRING,  
    instant_bookable        STRING,  
    calculated_host_listings_count      STRING,  
    calculated_host_listings_count_entire_homes     STRING,  
    calculated_host_listings_count_private_rooms        STRING,  
    calculated_host_listings_count_shared_rooms     STRING,  
    reviews_per_month       STRING  

)
STORED BY 'com.google.cloud.hive.bigquery.BigQueryStorageHandler'
TBLPROPERTIES (
    'projectId'='airbnb-448411',
    'datasetId'='airbnb',
    'tableId'='listings_table'
);

-- Create External Table for BigQuery: Table 3
CREATE EXTERNAL TABLE reviews_table (
    listing_id STRING,
    id STRING,  
    `date` STRING,  
    reviewer_id STRING,  
    reviewer_name STRING,  
    comments STRING,  

)
STORED BY 'com.google.cloud.hive.bigquery.BigQueryStorageHandler'
TBLPROPERTIES (
    'projectId'='airbnb-448411',
    'datasetId'='airbnb',
    'tableId'='reviews_table'
);

CREATE TABLE par_calendar_table
STORED AS PARQUET
LOCATION "gs://airbnb-chicago/outputs/parquet_calendar/" AS
SELECT * FROM calendar_table;

-- Create Table in GCS (Parquet) and Transfer Data for Table 2
CREATE TABLE par_listings_table
STORED AS PARQUET
LOCATION "gs://your-bucket/outputs/parquet_listings/" AS
SELECT * FROM listings_table;

-- Create Table in GCS (Parquet) and Transfer Data for Table 3
CREATE TABLE par_reviews_table
STORED AS PARQUET
LOCATION "gs://your-bucket/outputs/parquet_reviews/" AS
SELECT * FROM reviews_table;

