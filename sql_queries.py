import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')


# DWH config data been loaded into variables

S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"


# CREATE TABLES

staging_events_table_create= (
    """
    CREATE TABLE staging_events_table 
    (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts VARCHAR,
        userAgent VARCHAR,
        userId INTEGER
    );
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE staging_songs_table 
    (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DECIMAL,
        year INTEGER
    );
    """
)

# fact table 
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY SORTKEY ,
    start_time TIMESTAMP NOT NULL DISTKEY ,
    user_id INT NOT NULL, 
    level VARCHAR NOT NULL,
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INT NOT NULL, 
    location VARCHAR, 
    user_agent VARCHAR NOT NULL)
""")

# dimension table 
user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table(
    user_id INT PRIMARY KEY SORTKEY, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender CHAR(1) NOT NULL, 
    level VARCHAR NOT NULL)
    diststyle all; 
""")

# dimension table 
song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(
    song_id VARCHAR PRIMARY KEY SORTKEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INT NOT NULL, 
    duration FLOAT NOT NULL)
    diststyle all;
""")

# dimension table 
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(
    artist_id VARCHAR PRIMARY KEY SORTKEY, 
    name VARCHAR NOT NULL, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT)
    diststyle all;
""")

# dimension table 
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
    start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL)
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table
    FROM {}
    iam_role {}
    FORMAT AS json {}
    """).format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs_table 
    FROM {}
    iam_role {}
    FORMAT AS json 'auto'
    """).format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(events.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
    events.userId AS user_id,
    events.level AS level,
    songs.song_id AS song_id,
    songs.artist_id AS artist_id,
    events.sessionId AS session_id,
    events.location AS location,
    events.userAgent AS user_agent
    FROM staging_events_table AS events
    JOIN staging_songs_table AS songs ON (events.song = songs.title AND events.artist = songs.artist_name)
    AND events.page  =  'NextSong'
    """
)

user_table_insert = (
    """
    INSERT INTO user_table (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
    FROM staging_events_table
    WHERE page = 'NextSong' 
    AND user_id NOT IN (SELECT DISTINCT user_id FROM user_table)
    """
)

song_table_insert = (
    """
    INSERT INTO song_table (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs_table
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM song_table)
    """
)

artist_table_insert = (
    """
    INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) as artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
    FROM staging_songs_table
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artist_table)
    """
)

time_table_insert = (
    """
    INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT to_timestamp(to_char(events.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dayofweek FROM start_time) as weekday
    FROM staging_events_table events;
    """
)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
