{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by='production_id'
) }}

WITH first_production AS (
    SELECT
        id AS movie_id,
        trim(splitByChar(',', production_companies)[1]) AS production_name
    FROM {{ source('bronze', 'tmdb_raw') }}
    WHERE production_companies IS NOT NULL AND production_companies != ''
)

SELECT
    row_number() OVER (ORDER BY production_name) AS production_id,
    production_name,
    current_date() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM (
    SELECT DISTINCT production_name
    FROM first_production
)
WHERE production_name IS NOT NULL;