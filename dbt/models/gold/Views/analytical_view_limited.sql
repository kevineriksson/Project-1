-- Creating a pseudonymised view for a limited access user
{{ config(
    materialized='view',
    engine='View()'
) }}

SELECT 
    -- Hash the title of the movie
    cityHash64(m.movie_title) AS movie_title,
    -- Mask last two numbers of the release year
    substring(toString(dt.year), 1, 2) || '**' AS release_year,
    -- Mask everything but the director's first name with *
    concat(splitByChar(' ', d.director_name)[1], ' ', '*****') AS director_name,
    g.genre_name,
    p.production_name,
    -- Mask the exact voting numbers as generalisations
    multiIf(
        f.vote_avg < 6, 'LOW',
        f.vote_avg < 8, 'MID',
        f.vote_avg >= 8, 'HIGH',
        'UNKNOWN'
    ) AS vote_avg,
    f.vote_count,
    f.revenue,
    f.budget,
    f.movie_popularity,
    f.revenue_growth,
    f.popularity_change,
    f.vote_avg_change


FROM {{ref('fact_movie_performance')}} f

LEFT JOIN {{ ref('dim_movie') }} m
    ON f.imdb_id = m.imdb_id

LEFT JOIN {{ ref('dim_date') }} dt
    ON f.date_id = dt.date_id

LEFT JOIN {{ ref('dim_director') }} d
    ON f.director_id = d.director_id

LEFT JOIN {{ ref('dim_genre') }} g
    ON f.genre_id = g.genre_id

LEFT JOIN {{ ref('dim_production') }} p
    ON f.production_id = p.production_id