CREATE DATABASE IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.tmdb_bronze_ch SYNC;

CREATE TABLE bronze.tmdb_bronze_ch
(
    imdb_id String,
    title String,
    vote_avg Float64,
    vote_count Int32,
    release_date DateTime64,
    revenue Float64
)
ENGINE = S3(
    'http://minio:9000/practice-bucket/bronze/tmdb_bronze/data/00000-0-30f80b1c-921b-4022-b9ed-4a2949350601.parquet',
    'minioadmin',
    'minioadmin',
    'Parquet'
)
SETTINGS
    s3_use_environment_credentials = 0,
    s3_region = 'us-east-1';
