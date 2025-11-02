{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by='fact_id'
) }}

WITH filtered_tmdb AS (
    SELECT 
        imdb_id,
        title,
        release_date,
        production_companies,
        budget,
        revenue,
        vote_average,
        vote_count,
        runtime,
        genres,
        original_language,
        
        (vote_average * log(1 + vote_count)) AS movie_popularity
    FROM {{ source('bronze', 'tmdb_raw') }}
),

-- First genre
first_genre AS (
    SELECT
        imdb_id AS movie_id,
        trim(splitByChar(',', genres)[1]) AS genre_name
    FROM filtered_tmdb
),

-- First director
first_director AS (
    SELECT
        t.imdb_id AS movie_id,
        trim(splitByChar(',', c.directors)[1]) AS director_id
    FROM filtered_tmdb t
    LEFT JOIN {{ source('bronze', 'imdb_title_crew_raw') }} c
        ON t.imdb_id = c.tconst
),

-- Join to dim_production to get production_id
movie_production AS (
    SELECT
        t.imdb_id AS movie_id,
        p.production_id
    FROM filtered_tmdb t
    LEFT JOIN {{ ref('dim_production') }} p
        ON trim(splitByChar(',', t.production_companies)[1]) = p.production_name
),

-- Map genre_name to dim_genre to get genre_id
movie_genre AS (
    SELECT
        fg.movie_id,
        g.genre_id
    FROM first_genre fg
    LEFT JOIN {{ ref('dim_genre') }} g
        ON fg.genre_name = g.genre_name
),

base AS (
    SELECT
        t.imdb_id,
        d.director_id,
        mg.genre_id,
        mp.production_id,
        dr.date_id                                AS date_id,
        t.vote_average                            AS vote_avg,
        t.vote_count,
        t.title,
        t.revenue,
        t.budget,
        t.movie_popularity,
        toYear(t.release_date)                    AS release_year
    FROM filtered_tmdb t
    LEFT JOIN movie_genre mg       ON mg.movie_id = t.imdb_id
    LEFT JOIN first_director d     ON d.movie_id = t.imdb_id
    LEFT JOIN movie_production mp  ON mp.movie_id = t.imdb_id
    LEFT JOIN {{ ref('dim_date') }} dr
        ON dr.full_date = t.release_date
),

metrics AS (
    SELECT
        b.*,
        b.revenue - lag(b.revenue, 1) OVER (PARTITION BY b.imdb_id ORDER BY b.release_year) AS revenue_growth,
        b.movie_popularity - lag(b.movie_popularity, 1) OVER (PARTITION BY b.imdb_id ORDER BY b.release_year) AS popularity_change,
        b.vote_avg - lag(b.vote_avg, 1) OVER (PARTITION BY b.imdb_id ORDER BY b.release_year) AS vote_avg_change
    FROM base b
)

SELECT
    row_number() OVER (ORDER BY imdb_id, date_id, director_id, genre_id) AS fact_id,
    cityHash64(TRIM(title)) AS movie_id,
    imdb_id,
    director_id,
    genre_id,
    production_id,
    date_id,
    vote_avg,
    vote_count,
    revenue,
    budget,
    movie_popularity,
    revenue_growth,
    popularity_change,
    vote_avg_change
FROM metrics