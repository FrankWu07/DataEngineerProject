# Drop Tables
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# Create Tables

# 1.create songplays table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time BIGINT NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT NOT NULL,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

#2.create users table
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT NOT NULL PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR,
        level VARCHAR NOT NULL
    );
""")

#3.create songs table
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR NOT NULL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        artist_id VARCHAR(255) NOT NULL,
        year INT,
        duration FLOAT);
""")

#4.create artists table
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(255) NOT NULL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT);
""")

#5.create time table
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT NOT NULL PRIMARY KEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    );
""")

# Insert Values
#1. songplays table insert values
songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id,
                           artist_id, session_id, location, user_agent)
    VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s);
""")

#2. users table insert values
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE
    SET level = EXCLUDED.level;
""")

#3. songs table insert values
song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

#4. artists table insert values
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

#5. time table insert values
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT 
        s.song_id, 
        a.artist_id
    FROM songs s
    JOIN artists a 
    ON s.artist_id = a.artist_id
    WHERE s.title = %s 
    AND a.name = %s 
    AND s.duration = %s;
""")

# QUERY LISTS
# 1. create table queries
create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]

# 2. drop table queries
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

