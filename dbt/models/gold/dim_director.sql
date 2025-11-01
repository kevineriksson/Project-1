{{ config(materialized='table', schema='gold') }}

WITH first_dir AS (
    SELECT
        tconst,
        TRIM(splitByChar(',', directors)[1]) AS director_id
    FROM imdb_title_crew_raw
    WHERE directors IS NOT NULL AND directors != ''
),
deduped AS (
    SELECT DISTINCT director_id
    FROM first_dir
),
joined AS (
    SELECT
        d.director_id AS dir_id,
        n.primaryName AS dir_name
    FROM deduped d
    LEFT JOIN imdb_name_basics_raw n
        ON d.director_id = n.nconst
)
SELECT
    row_number() OVER (ORDER BY dir_id) AS imdb_dir_name_id,
    dir_id,
    dir_name,
    current_date() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM joined
WHERE dir_id IS NOT NULL;