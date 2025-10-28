{{ config(materialized='table', schema='gold') }}

SELECT
    tmdb.id            AS movie_id,
    b.tconst           AS imdb_id,
    tmdb.title         AS movie_title,
    tmdb.runtime       AS movie_runtime,
    tmdb.popularity    AS movie_popularity
FROM bronze.tmdb_raw tmdb
LEFT JOIN bronze.imdb_title_basics_raw b
    ON b.primaryTitle = tmdb.title
    AND toInt32OrZero(b.startYear) = toYear(tmdb.release_date)
;
