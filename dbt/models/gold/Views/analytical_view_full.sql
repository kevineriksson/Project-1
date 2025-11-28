-- Creating an unaltered view for a full access user
{{ config(
    materialized='view',
    engine='View()'
) }}

SELECT 
    m.movie_title,
    dt.year AS release_year,
    d.director_name,
    g.genre_name,
    p.production_name,

    f.vote_avg,
    f.vote_count,
    f.revenue,
    f.budget,
    f.movie_popularity,
    f.revenue_growth,
    f.popularity_change,
    f.vote_avg_change,


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