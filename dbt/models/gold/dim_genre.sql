{{ config(materialized='table', schema='gold') }}

SELECT
  ROW_NUMBER() OVER (ORDER BY first_genre) AS genre_id,
  first_genre AS genre_name
FROM (
  SELECT
    TRIM(SPLIT(genres, ',')[0]) AS first_genre
  FROM {{ source('bronze', 'tmdb_raw') }}
  WHERE genres IS NOT NULL
)
GROUP BY first_genre;