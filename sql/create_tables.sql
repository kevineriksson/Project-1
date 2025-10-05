-- Create the Dim_Movie table
CREATE TABLE Dim_Movie (
    movie_ID INT PRIMARY KEY,
    imdb_ID VARCHAR(15),
    movie_title VARCHAR(255),
    movie_runtime INT,
    movie_popularity FLOAT
);

-- Create the Dim_Production table
CREATE TABLE Dim_Production (
    prod_ID INT PRIMARY KEY,
    prod_name VARCHAR(150),
    prod_role VARCHAR(50)
);

-- Create the Dim_Release_Date table
CREATE TABLE Dim_Release_Date (
    release_ID INT PRIMARY KEY,
    date_full DATE,
    year SMALLINT,
    month TINYINT,
    day TINYINT
);

-- Create the Dim_Genre table
CREATE TABLE Dim_Genre (
    genre_ID INT PRIMARY KEY,
    genre_name VARCHAR(100)
);

-- Create the Fact_Movie table
CREATE TABLE Fact_Movie (
    fact_ID INT PRIMARY KEY,
    movie_ID INT,
    release_ID INT,
    vote_count INT,
    vote_avg DECIMAL(2, 3),
    revenue BIGINT,
    budget BIGINT,
    FOREIGN KEY (movie_ID) REFERENCES Dim_Movie(movie_ID),
    FOREIGN KEY (release_ID) REFERENCES Dim_Release_Date(release_ID)
);

-- Create the Movie_Genre table (Junction table between movies and genres)
CREATE TABLE Movie_Genre (
    movie_ID INT,
    genre_ID INT,
    PRIMARY KEY (movie_ID, genre_ID),
    FOREIGN KEY (movie_ID) REFERENCES Dim_Movie(movie_ID),
    FOREIGN KEY (genre_ID) REFERENCES Dim_Genre(genre_ID)
);

-- Create the Prod_Movie table (Junction table between movies and productions)
CREATE TABLE Prod_Movie (
    movie_ID INT,
    prod_ID INT,
    PRIMARY KEY (movie_ID, prod_ID),
    FOREIGN KEY (movie_ID) REFERENCES Dim_Movie(movie_ID),
    FOREIGN KEY (prod_ID) REFERENCES Dim_Production(prod_ID)
);