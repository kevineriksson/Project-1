-- ==========================================================
-- DEMO SQL QUERIES
-- Purpose: Answer key business questions using the star schema
-- ==========================================================

-- 1. Which genres have the highest average ratings?
SELECT 
    g.genre_name,
    ROUND(AVG(f.vote_avg), 2) AS avg_rating
FROM Fact_Movie f
JOIN Movie_Genre mg ON f.movie_ID = mg.movie_ID
JOIN Dim_Genre g ON mg.genre_ID = g.genre_ID
GROUP BY g.genre_name
ORDER BY avg_rating DESC;


-- 2. Which directors consistently produce high-rated movies and high revenue?
SELECT 
    p.prod_name AS director,
    COUNT(DISTINCT f.movie_ID) AS total_movies,
    ROUND(AVG(f.vote_avg), 2) AS avg_rating,
    ROUND(AVG(f.revenue), 0) AS avg_revenue
FROM Fact_Movie f
JOIN Prod_Movie pm ON f.movie_ID = pm.movie_ID
JOIN Dim_Production p ON pm.prod_ID = p.prod_ID
WHERE p.prod_role = 'Director'
GROUP BY p.prod_name
HAVING COUNT(DISTINCT f.movie_ID) >= 3
ORDER BY avg_rating DESC, avg_revenue DESC;


-- 3. How does average rating correlate with box-office revenue across release years?
SELECT 
    r.year,
    ROUND(AVG(f.vote_avg), 2) AS avg_rating,
    ROUND(AVG(f.revenue), 0) AS avg_revenue
FROM Fact_Movie f
JOIN Dim_Release_Date r ON f.release_ID = r.release_ID
GROUP BY r.year
ORDER BY r.year;


-- 4. What are the top 10 movies by revenue per genre for a given year (example: 2023)?
-- Replace 2023 with another year as needed.
SELECT 
    g.genre_name,
    m.movie_title,
    f.revenue
FROM Fact_Movie f
JOIN Dim_Movie m ON f.movie_ID = m.movie_ID
JOIN Movie_Genre mg ON f.movie_ID = mg.movie_ID
JOIN Dim_Genre g ON mg.genre_ID = g.genre_ID
JOIN Dim_Release_Date r ON f.release_ID = r.release_ID
WHERE r.year = 2023
QUALIFY ROW_NUMBER() OVER (PARTITION BY g.genre_name ORDER BY f.revenue DESC) <= 10;


-- 5. How does the runtime affect changes to the overall box-office revenue?
SELECT 
    ROUND(m.movie_runtime / 10) * 10 AS runtime_bucket,  -- group by 10-minute bins
    ROUND(AVG(f.revenue), 0) AS avg_revenue,
    COUNT(*) AS movie_count
FROM Fact_Movie f
JOIN Dim_Movie m ON f.movie_ID = m.movie_ID
GROUP BY runtime_bucket
ORDER BY runtime_bucket;