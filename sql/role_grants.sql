GRANT SELECT (vote_avg, vote_count, revenue, budget, movie_popularity, revenue_growth, popularity_change, vote_avg_change, 
        date_id, director_id, genre_id, imdb_id, production_id) ON gold.fact_movie_performance TO analyst_limited;
        
GRANT SELECT (movie_title, imdb_id) ON gold.dim_movie TO analyst_limited;
GRANT SELECT (year, date_id) ON gold.dim_date TO analyst_limited;
GRANT SELECT (director_name, director_id) ON gold.dim_director TO analyst_limited;
GRANT SELECT (genre_name, genre_id) ON gold.dim_genre TO analyst_limited;
GRANT SELECT (production_name, production_id) ON gold.dim_production TO analyst_limited;
GRANT SELECT ON gold.analytical_view_limited TO analyst_limited;

GRANT SELECT ON gold.fact_movie_performance TO analyst_full;
GRANT SELECT ON gold.dim_movie TO analyst_full;
GRANT SELECT ON gold.dim_date TO analyst_full;
GRANT SELECT ON gold.dim_director TO analyst_full;
GRANT SELECT ON gold.dim_genre TO analyst_full;
GRANT SELECT ON gold.dim_production TO analyst_full;
GRANT SELECT ON gold.analytical_view_full TO analyst_full;
GRANT SELECT ON gold.analytical_view_limited TO analyst_full;

GRANT analyst_full TO bigbo55;
GRANT analyst_limited TO npc123;