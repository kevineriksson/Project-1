-- ==========================================================
-- DEMO SQL QUERIES — PROJECT 2 (_gold schema)
-- ==========================================================

-- 1. Which genres have the highest average ratings?
SELECT 
    g.genre_name,
    ROUND(AVG(f.vote_avg), 2) AS avg_rating
FROM _gold.fact_movie f
JOIN _gold.dim_genre g ON f.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC;


-- 2. Which directors consistently produce high-rated movies and high revenue?
SELECT 
    p.prod_name AS director,
    COUNT(DISTINCT f.movie_id) AS total_movies,
    ROUND(AVG(f.vote_avg), 2) AS avg_rating,
    ROUND(AVG(f.revenue), 0) AS avg_revenue
FROM _gold.fact_movie f
JOIN _gold.dim_production p ON f.prod_id = p.prod_id
WHERE p.prod_role = 'Director'
GROUP BY p.prod_name
HAVING COUNT(DISTINCT f.movie_id) >= 3
ORDER BY avg_rating DESC, avg_revenue DESC;


-- 3. How does average rating correlate with box-office revenue across release years?
SELECT 
    d.release_year,
    ROUND(AVG(f.vote_avg), 2) AS avg_rating,
    ROUND(AVG(f.revenue), 0) AS avg_revenue
FROM _gold.fact_movie f
JOIN _gold.dim_date d ON f.date_id = d.date_id
GROUP BY d.release_year
ORDER BY d.release_year;


-- 4. What are the top 10 movies by revenue per genre for a given year (example: 2023)?
-- Replace 2023 with another year if needed.
SELECT 
    g.genre_name,
    m.movie_title,
    f.revenue
FROM _gold.fact_movie f
JOIN _gold.dim_movie m ON f.movie_id = m.movie_id
JOIN _gold.dim_genre g ON f.genre_id = g.genre_id
JOIN _gold.dim_date d ON f.date_id = d.date_id
WHERE d.release_year = 2023
QUALIFY ROW_NUMBER() OVER (PARTITION BY g.genre_name ORDER BY f.revenue DESC) <= 10;


-- 5. How does runtime affect box-office revenue?
SELECT 
    ROUND(m.runtime / 10) * 10 AS runtime_bucket,
    ROUND(AVG(f.revenue), 0) AS avg_revenue,
    COUNT() AS movie_count
FROM _gold.fact_movie f
JOIN _gold.dim_movie m ON f.movie_id = m.movie_id
GROUP BY runtime_bucket
ORDER BY runtime_bucket;
