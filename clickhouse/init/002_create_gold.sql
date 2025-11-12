CREATE TABLE IF NOT EXISTS gold.dim_movie
(
    movie_id UInt64,
    imdb_id String,
    movie_title String,
    movie_runtime Int32,
    movie_popularity Float32
)
ENGINE = MergeTree
ORDER BY movie_id;

CREATE TABLE IF NOT EXISTS gold.dim_genre
(
    genre_id UInt32,
    genre_name String
)
ENGINE = MergeTree
ORDER BY genre_id;

CREATE TABLE IF NOT EXISTS gold.dim_date
(
    release_id UInt32,
    date_full Date32,
    release_year UInt16,
    release_month UInt8,
    release_day UInt8
)
ENGINE = MergeTree
ORDER BY release_id;

CREATE TABLE IF NOT EXISTS gold.dim_production
(
    prod_id UInt32,
    prod_name String,
    prod_role String
)
ENGINE = MergeTree
ORDER BY prod_id;

CREATE TABLE IF NOT EXISTS gold.fact_movie_performance
(
    fact_id UInt64,
    movie_id UInt64,
    release_id UInt32,
    vote_count UInt32,
    vote_avg Float32,
    revenue Int64,
    budget Int64,
    release_year UInt16
)
ENGINE = MergeTree
PARTITION BY release_year

ORDER BY (movie_id, release_id, fact_id);
