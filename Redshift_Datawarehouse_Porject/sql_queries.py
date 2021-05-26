import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH= config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
REGION = config.get('S3','REGION')

# DROP TABLES
staging_events_table_drop = "DROP TABle IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
    event_id        INT IDENTITY(0, 1),
    artist          VARCHAR,
    auth            VARCHAR, 
    firstName       VARCHAR,
    gender          VARCHAR,   
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR, 
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INTEGER SORTKEY DISTKEY,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INTEGER SORTKEY DISTKEY,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year FLOAT 
);
""")

songplays_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time      TIMESTAMP   NOT NULL DISTKEY SORTKEY,
    user_id         INTEGER     NOT NULL,
    level           VARCHAR     NOT NULL,
    song_id         VARCHAR     NOT NULL,
    artist_id       VARCHAR     NOT NULL,
    session_id      INTEGER     NOT NULL,
    location        VARCHAR     NOT NULL,
    user_agent      VARCHAR     NOT NULL
);
""")

users_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id         INTEGER NOT NULL PRIMARY KEY SORTKEY,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          VARCHAR,
    level           VARCHAR
);
""")

songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id     VARCHAR PRIMARY KEY SORTKEY,
    title       VARCHAR NOT NULL,
    artist_id   VARCHAR  NOT NULL DISTKEY,
    year        INTEGER NOT NULL,
    duration    FLOAT NOT NULL
);
""")

artists_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id          VARCHAR  NOT NULL PRIMARY KEY DISTKEY,
    name               VARCHAR NOT NULL,
    location           VARCHAR,
    latitude           FLOAT,
    longitude          FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time      TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
    hour          INTEGER NOT NULL,
    day           INTEGER NOT NULL,
    week          INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    year          INTEGER NOT NULL,
    weekday       INTEGER NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TIMEFORMAT as 'epochmillisecs'
    COMPUPDATE OFF region {}
    FORMAT AS JSON {}
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, REGION, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TIMEFORMAT as 'epochmillisecs'
    COMPUPDATE OFF region {}
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT se.ts, 
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
FROM staging_events AS se
JOIN staging_songs AS ss
ON se.song = ss.title 
AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.userId, 
                se.firstName, 
                se.lastName, 
                se.gender, 
                se.level
FROM staging_events AS se
WHERE se.userId IS NOT NULL
AND se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT ss.song_id, 
                ss.title, 
                ss.artist_id, 
                ss.year, 
                ss.duration
FROM staging_songs AS ss
WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT ss.artist_id, 
                ss.artist_name, 
                ss.artist_location,
                ss.artist_latitude,
                ss.artist_longitude
FROM staging_songs AS ss
WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT se.ts,
                EXTRACT(hour from se.ts),
                EXTRACT(day from se.ts),
                EXTRACT(week from se.ts),
                EXTRACT(month from se.ts),
                EXTRACT(year from se.ts),
                EXTRACT(weekday from se.ts)
FROM staging_events AS se
WHERE se.page = 'NextSong';
""")





# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplays_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]