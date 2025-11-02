-- ...existing code...
{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by='movie_id'
) }}

WITH movie_src AS (
    SELECT
        TRIM(title) AS movie_title,
        imdb_id,
        release_date,
        runtime AS movie_runtime,
        original_language AS language
    FROM {{ source('bronze', 'tmdb_raw') }}
    WHERE title IS NOT NULL AND title != ''
)
SELECT
    cityHash64(movie_title) AS movie_id,
    any(imdb_id) AS imdb_id,
    movie_title,
    any(release_date) AS release_date,
    any(movie_runtime) AS movie_runtime,
    any(language) AS language
FROM movie_src
GROUP BY movie_title
ORDER BY movie_title