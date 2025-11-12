{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='production_id'
) }}

WITH production_list AS (
    SELECT TRIM(splitByChar(',', production_companies)[1]) AS production_name
    FROM {{ source('bronze', 'tmdb_raw') }}
    WHERE production_companies IS NOT NULL AND production_companies != ''
)

SELECT
    cityHash64(production_name) AS production_id,
    production_name,
    current_date() AS valid_from,
    CAST(NULL AS Nullable(Date)) AS valid_to,
    TRUE AS is_current
FROM (
    SELECT DISTINCT production_name
    FROM production_list
)
WHERE production_name IS NOT NULL AND production_name != ''
