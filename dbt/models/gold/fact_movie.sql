{{ config(materialized='table', schema='gold') }}

WITH latest_tmdb AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY id ORDER BY ingestion_date DESC) AS rn
    FROM bronze.tmdb_raw
)

SELECT
    row_number() OVER (ORDER BY lt.id)        AS fact_id,
    lt.id                                     AS movie_id,
    dr.release_id                             AS release_id,
    lt.vote_count,
    lt.vote_average                           AS vote_avg,
    lt.revenue,
    lt.budget,
    toYear(lt.release_date)                   AS release_year
FROM latest_tmdb lt
JOIN {{ ref('dim_release_date') }} dr
    ON dr.date_full = lt.release_date
WHERE rn = 1
;
