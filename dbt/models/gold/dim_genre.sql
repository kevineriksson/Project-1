{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by='genre_id'
) }}

WITH parsed_genres AS (
    SELECT
        TRIM(splitByChar(',', genres)[1]) AS first_genre
    FROM {{ source('bronze', 'tmdb_raw') }}
    WHERE genres IS NOT NULL AND genres != ''
)
SELECT
    cityHash64(first_genre) AS genre_id,
    first_genre AS genre_name
FROM parsed_genres
GROUP BY first_genre
ORDER BY first_genre