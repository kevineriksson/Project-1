{{ config(
    materialized = 'table',
    schema = 'gold',
    engine = 'MergeTree()',
    order_by = 'movie_id'
) }}

SELECT
    row_number() OVER (ORDER BY title) AS movie_id,
    imdb_id,
    title AS movie_title,
    release_date,
    runtime AS movie_runtime,
    original_language AS language
FROM {{ source('bronze', 'tmdb_raw') }}
WHERE title IS NOT NULL;