{{ config(materialized='table', schema='gold') }}

WITH exploded AS (
    SELECT
        id AS movie_id,
        arrayJoin(splitByChar(',', replaceAll(genres, ' ', ''))) AS genre_name_raw
    FROM bronze.tmdb_raw
)
SELECT
    row_number() OVER (ORDER BY genre_name_raw) AS genre_id,
    genre_name_raw                              AS genre_name
FROM (
    SELECT DISTINCT genre_name_raw
    FROM exploded
    WHERE genre_name_raw != ''
)
;
