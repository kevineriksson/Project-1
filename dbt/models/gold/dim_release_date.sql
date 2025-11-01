{{ config(
    materialized = 'table',
    schema = 'gold',
    engine = 'MergeTree()',
    order_by = 'date_id'
) }}

WITH distinct_dates AS (
    SELECT DISTINCT release_date
    FROM {{ source('bronze', 'tmdb_raw') }}
    WHERE release_date IS NOT NULL
)

SELECT
    row_number() OVER (ORDER BY release_date)                   AS date_id,
    release_date                                                AS full_date,
    toYear(release_date)                                        AS year,
    toMonth(release_date)                                       AS month,
    toDayOfMonth(release_date)                                  AS day,
    toISOWeek(release_date)                                     AS week,
    formatDateTime(release_date, '%A')                          AS weekday,
    CASE
        WHEN toMonth(release_date) IN (12,1,2)  THEN 'Winter'
        WHEN toMonth(release_date) IN (3,4,5)   THEN 'Spring'
        WHEN toMonth(release_date) IN (6,7,8)   THEN 'Summer'
        WHEN toMonth(release_date) IN (9,10,11) THEN 'Autumn'
    END                                                         AS season,
    CASE
        WHEN formatDateTime(release_date, '%A') IN ('Saturday','Sunday') THEN 1
        ELSE 0
    END                                                         AS is_weekend,

    /* ---------- holiday tagging ---------- */
    CASE
        WHEN toMonth(release_date)=1  AND toDayOfMonth(release_date)=1  THEN 'New Year\'s Day'
        WHEN toMonth(release_date)=2  AND toDayOfMonth(release_date)=14 THEN 'Valentine\'s Day'
        WHEN toMonth(release_date)=3  AND toDayOfMonth(release_date)=17 THEN 'St. Patrick\'s Day'
        WHEN toMonth(release_date)=4  AND toDayOfMonth(release_date)=1  THEN 'April Fool\'s Day'
        WHEN toMonth(release_date)=4  AND toDayOfMonth(release_date)=22 THEN 'Earth Day'
        WHEN toMonth(release_date)=5  AND toDayOfMonth(release_date)=1  THEN 'May Day'
        WHEN toMonth(release_date)=5  AND toDayOfMonth(release_date)=5  THEN 'Cinco de Mayo'
        WHEN toMonth(release_date)=6  AND toDayOfMonth(release_date) BETWEEN 19 AND 26 THEN 'Midsummer'
        WHEN toMonth(release_date)=7  AND toDayOfMonth(release_date)=4  THEN 'Independence Day'
        WHEN toMonth(release_date)=9  AND toDayOfMonth(release_date) BETWEEN 1 AND 7 
             AND formatDateTime(release_date,'%A')='Monday'             THEN 'Labor Day (US)'
        WHEN toMonth(release_date)=10 AND toDayOfMonth(release_date)=31 THEN 'Halloween'
        WHEN toMonth(release_date)=11 AND formatDateTime(release_date,'%A')='Thursday'
             AND toISOWeek(release_date) >= 47                           THEN 'Thanksgiving'
        WHEN toMonth(release_date)=12 AND toDayOfMonth(release_date)=24 THEN 'Christmas Eve'
        WHEN toMonth(release_date)=12 AND toDayOfMonth(release_date)=25 THEN 'Christmas Day'
        WHEN toMonth(release_date)=12 AND toDayOfMonth(release_date)=31 THEN 'New Year\'s Eve'
        ELSE NULL
    END                                                         AS holiday_name,
    IF(holiday_name IS NOT NULL, 1, 0)                          AS is_holiday,

    /* placeholders for later enhancement */
    NULL                                                        AS days_until_holiday,
    NULL                                                        AS days_after_holiday
FROM distinct_dates
ORDER BY release_date;
