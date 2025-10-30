CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.tmdb_raw
(
    imdb_id String,
    title String,
    release_date Date32,
    budget Int64,
    revenue Int64,
    vote_average Float32,
    vote_count UInt32,
    runtime Int32,
    genres String
)
ENGINE = MergeTree
ORDER BY (imdb_id);

CREATE TABLE IF NOT EXISTS bronze.imdb_title_basics_raw
(
    tconst String,
    primaryTitle String,
    startYear Int32,
    runtimeMinutes Int32
)
ENGINE = MergeTree
ORDER BY (tconst);

CREATE TABLE IF NOT EXISTS bronze.imdb_title_crew_raw
(
    tconst String,
    directors String
)
ENGINE = MergeTree
ORDER BY (tconst);

CREATE TABLE IF NOT EXISTS bronze.imdb_name_basics_raw
(
    nconst String,
    primaryName String,
    primaryProfession String
)
ENGINE = MergeTree
ORDER BY (nconst);
