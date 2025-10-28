{{ config(materialized='table', schema='gold') }}

SELECT
    row_number() OVER (ORDER BY toYYYYMMDD(release_date)) AS release_id,
    release_date                                        AS date_full,
    toYear(release_date)                                AS release_year,
    toMonth(release_date)                               AS release_month,
    toDayOfMonth(release_date)                          AS release_day
FROM (
    SELECT DISTINCT release_date
    FROM bronze.tmdb_raw
    WHERE release_date IS NOT NULL
)
;
