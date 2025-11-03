-- ==========================================================
-- DEMO SQL QUERIES — PROJECT 2 (_gold schema)
-- ==========================================================

-- 1. Which genres have the highest average ratings?
SELECT
    g.genre_name,
    round(AVG(fm.vote_avg), 2) AS avg_rating
FROM _gold.fact_movie AS fm
INNER JOIN _gold.dim_genre AS g
    ON fm.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC;


-- 2. Which directors consistently produce high-rated movies and high revenue?
SELECT
    d.director_name,
    round(AVG(fm.vote_avg), 2) AS avg_rating,
    round(AVG(fm.revenue), 2) AS avg_revenue,
    COUNT(fm.movie_id) AS movie_count
FROM _gold.fact_movie AS fm
INNER JOIN _gold.dim_director AS d
    ON fm.director_id = d.director_id
GROUP BY d.director_name
HAVING 
    COUNT(fm.movie_id) >= 5
    AND AVG(fm.vote_avg) > 6.5
    AND AVG(fm.revenue) > 10000000
ORDER BY avg_rating DESC, avg_revenue DESC
LIMIT 10;


-- 3. How does average rating correlate with box-office revenue across release years?
SELECT
    toYear(m.release_date) AS release_year,
    round(AVG(fm.vote_avg), 2) AS avg_rating,
    round(AVG(fm.revenue), 0) AS avg_revenue
FROM _gold.fact_movie AS fm
INNER JOIN _gold.dim_movie AS m
    ON fm.movie_id = m.imdb_id
GROUP BY release_year
ORDER BY release_year ASC;


-- 4. What are the top 10 movies by revenue per genre for a given year (example: 2023)?
-- Replace 2023 with another year if needed.
SELECT
    g.genre_name,
    anyHeavy(m.movie_title) AS top_movie,
    max(fm.revenue) AS top_revenue
FROM _gold.fact_movie AS fm
INNER JOIN _gold.dim_movie AS m
    ON fm.movie_id = m.imdb_id
INNER JOIN _gold.dim_genre AS g
    ON fm.genre_id = g.genre_id
WHERE toYear(m.release_date) = 2005
GROUP BY g.genre_name
ORDER BY top_revenue DESC
LIMIT 10;


-- 5. How does runtime affect box-office revenue?
SELECT
    multiIf(
        m.movie_runtime <= 60, '0-60 min',
        m.movie_runtime <= 90, '61-90 min',
        m.movie_runtime <= 120, '91-120 min',
        m.movie_runtime <= 150, '121-150 min',
        '151+ min'
    ) AS runtime_bucket,
    COUNT(fm.movie_id) AS movie_count,
    AVG(fm.revenue) AS avg_revenue
FROM _gold.fact_movie AS fm
INNER JOIN _gold.dim_movie AS m
    ON fm.movie_id = m.imdb_id
WHERE m.movie_runtime IS NOT NULL
  AND fm.revenue IS NOT NULL
GROUP BY runtime_bucket
ORDER BY avg_revenue ASC;
