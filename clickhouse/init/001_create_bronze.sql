CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.tmdb_raw
(
    id UInt64,
    title String,
    release_date Date32,
    revenue Int64,
    budget Int64,
    popularity Float32,
    vote_average Float32,
    vote_count UInt32,
    runtime Int32,
    genres String,
    ingestion_date Date
)
ENGINE = MergeTree
ORDER BY (id, ingestion_date);

CREATE TABLE IF NOT EXISTS bronze.imdb_title_basics_raw
(
    tconst String,
    titleType String,
    primaryTitle String,
    originalTitle String,
    isAdult UInt8,
    startYear Int32,
    endYear Int32,
    runtimeMinutes Int32,
    genres String,
    ingestion_date Date
)
ENGINE = MergeTree
ORDER BY (tconst, ingestion_date);

CREATE TABLE IF NOT EXISTS bronze.imdb_title_crew_raw
(
    tconst String,
    directors String,
    writers String,
    ingestion_date Date
)
ENGINE = MergeTree
ORDER BY (tconst, ingestion_date);

-- NEW TABLE: IMDb Name Basics
CREATE TABLE IF NOT EXISTS bronze.imdb_name_basics_raw
(
    nconst String,
    primaryName String,
    birthYear Nullable(UInt16),
    deathYear Nullable(UInt16),
    primaryProfession String,
    knownForTitles String,
    ingestion_date Date
)
ENGINE = MergeTree
ORDER BY (nconst, ingestion_date);
