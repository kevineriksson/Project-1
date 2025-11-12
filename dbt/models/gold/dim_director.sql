{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='director_id'
) }}

WITH first_dir AS (
    SELECT
        tconst,
        TRIM(splitByChar(',', directors)[1]) AS director_id
    FROM {{ source('bronze', 'imdb_title_crew_raw') }}
    WHERE directors IS NOT NULL AND directors != ''
),
deduped AS (
    SELECT DISTINCT director_id
    FROM first_dir
),
joined AS (
    SELECT
        d.director_id,
        n.primaryName AS director_name
    FROM deduped d
    LEFT JOIN {{ source('bronze', 'imdb_name_basics_raw') }} n
        ON d.director_id = n.nconst
)
SELECT
    cityHash64(director_id) AS imdb_dir_name_id,  -- Use hash instead of row_number
    director_id,
    director_name,
    today() AS valid_from,
    CAST(NULL AS Nullable(Date)) AS valid_to,
    1 AS is_current  -- Use 1/0 instead of TRUE/FALSE
FROM joined
WHERE director_id IS NOT NULL
ORDER BY director_id