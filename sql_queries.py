import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_tables"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_log_events(
        artist                  VARCHAR,
        auth                    VARCHAR,
        firstName               VARCHAR,
        gender                  VARCHAR,
        itemInSession           INTEGER,
        lastName                VARCHAR,
        length                  FLOAT,
        level                   VARCHAR,
        location                VARCHAR,
        method                  VARCHAR,
        page                    VARCHAR,
        registration            VARCHAR,
        sessionId               INTEGER,
        song                    VARCHAR,
        status                  INTEGER,
        ts                      TIMESTAMP,
        userAgent               VARCHAR,
        userId                  INTEGER  
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs_tables(
        artist_id               VARCHAR,
        artist_latitude         FLOAT,
        artist_location         VARCHAR,
        artist_longitude        FLOAT,
        artist_name             VARCHAR,
        duration                FLOAT,
        num_songs               INTEGER,
        song_id                 VARCHAR,
        title                   VARCHAR,
        year                    INTEGER 
    )

""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
        start_time          TIMESTAMP       NOT NULL SORTKEY DISTKEY,
        user_id             INTEGER         NOT NULL,
        level               VARCHAR,
        song_id             VARCHAR         NOT NULL,
        artist_id           VARCHAR         NOT NULL,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id             INTEGER         NOT NULL SORTKEY PRIMARY KEY,
        first_name          VARCHAR         NOT NULL,
        last_name           VARCHAR         NOT NULL,
        gender              VARCHAR         NOT NULL,
        level               VARCHAR         NOT NULL
    )
""")

song_table_create = ("""
     CREATE TABLE songs(
        song_id             VARCHAR         NOT NULL SORTKEY PRIMARY KEY,
        title               VARCHAR         NOT NULL,
        artist_id           VARCHAR         NOT NULL,
        year                INTEGER         NOT NULL,
        duration            FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id           VARCHAR         NOT NULL SORTKEY PRIMARY KEY,
        name                VARCHAR         NOT NULL,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
    )    
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time          TIMESTAMP       NOT NULL DISTKEY SORTKEY PRIMARY KEY,
        hour                INTEGER         NOT NULL,
        day                 INTEGER         NOT NULL,
        week                INTEGER         NOT NULL,
        month               INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_log_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], 
            role_arn=config['IAM_ROLE']['ARN'], 
            log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs_tables from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], 
            role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(sl.ts)  AS start_time, 
            sl.userId        AS user_id, 
            sl.level         AS level, 
            s.song_id        AS song_id, 
            s.artist_id      AS artist_id, 
            sl.sessionId     AS session_id, 
            sl.location      AS location, 
            sl.userAgent     AS user_agent
    FROM staging_log_events sl
    JOIN staging_songs_tables  s   ON (sl.song = s.title AND sl.artist = s.artist_name)
    Where sl.page  =  'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_log_events
    WHERE user_id IS NOT NULL
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
    AND page  =  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs_tables
    WHERE song_id IS NOT NULL
    AND song_id NOT IN (SELECT DISTINCT song_id FROM songs);    
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs_tables
    WHERE artist_id IS NOT NULL
    AND artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
