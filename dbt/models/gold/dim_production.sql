{{ config(materialized='table', schema='gold') }}

WITH exploded AS (
    SELECT
        c.tconst,
        arrayJoin(splitByChar(',', c.directors)) AS director_id
    FROM bronze.imdb_title_crew_raw c
    WHERE c.directors != '' AND c.directors IS NOT NULL
)
SELECT
    row_number() OVER (ORDER BY director_id) AS prod_id,
    director_id                              AS prod_name,
    'Director'                               AS prod_role
FROM (
    SELECT DISTINCT director_id
    FROM exploded
)
;
