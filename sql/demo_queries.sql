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
    p.director_name,
    COUNT(DISTINCT f.movie_id) AS total_movies,
    ROUND(AVG(f.vote_avg), 2) AS avg_rating,
    ROUND(AVG(f.revenue), 0) AS avg_revenue
FROM _gold.fact_movie f
JOIN _gold.dim_director p ON f.director_id = p.director_id
GROUP BY p.director_name
HAVING COUNT(DISTINCT f.movie_id) >= 3
ORDER BY avg_revenue DESC, avg_rating DESC;


-- 3. How does average rating correlate with box-office revenue across release years?
SELECT 
    d.year,
    ROUND(AVG(f.vote_avg), 2) AS avg_rating,
    ROUND(AVG(f.revenue), 0) AS avg_revenue
FROM _gold.fact_movie f
JOIN _gold.dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;


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
WHERE d.year = 2023
QUALIFY ROW_NUMBER() OVER (PARTITION BY g.genre_name ORDER BY f.revenue DESC) <= 10;


-- 5. How does runtime affect box-office revenue?
SELECT 
    ROUND(m.movie_runtime / 10) * 10 AS runtime_bucket,
    ROUND(AVG(f.revenue), 0) AS avg_revenue,
    COUNT() AS movie_count
FROM _gold.fact_movie f
JOIN _gold.dim_movie m ON f.movie_id = m.movie_id
GROUP BY runtime_bucket
ORDER BY runtime_bucket;
