"""
This script is used to define you SQL statements

author: Philippe Jean Mith
date: Sept 9th 2023
"""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
  artist VARCHAR,
  auth VARCHAR(20),
  firstName VARCHAR (20),
  gender VARCHAR(6),
  itemInSession INTEGER,
  lastName VARCHAR(20),
  length REAL,
  level VARCHAR(5),
  location VARCHAR(50),
  method VARCHAR(8),
  page TEXT,
  registration DOUBLE PRECISION,
  sessionId INTEGER,
  song TEXT, 
  status INTEGER,
  ts TIMESTAMP,
  userAgent VARCHAR(256),
  userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
  song_id VARCHAR(256),
  title VARCHAR(256),
  duration FLOAT,
  year INTEGER,
  num_songs INTEGER,
  artist_id VARCHAR(256),
  artist_name VARCHAR(256),
  artist_longitude FLOAT,
  artist_latitude FLOAT,
  artist_location VARCHAR(256)
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays(
	songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  	start_time TIMESTAMP ,
  	user_id INTEGER ,
  	level VARCHAR(5),
  	song_id VARCHAR(256) ,
  	artist_id VARCHAR(256) ,
  	session_id INTEGER,
  	location VARCHAR(256),
  	user_agent VARCHAR(256)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users(
	user_id VARCHAR(18) PRIMARY KEY,
  	first_name VARCHAR(20),
  	last_name VARCHAR(20),
    gender VARCHAR(4),
    level VARCHAR(4)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs(
	song_id VARCHAR(20) NOT NULL PRIMARY KEY,
  	title VARCHAR(256) NOT NULL,
    artist_id VARCHAR(20) NOT NULL,
  	year INTEGER NOT NULL,
    duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists(
	artist_id VARCHAR(20) NOT NULL PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    location VARCHAR(256),
    latitude VARCHAR(64),
    longitude VARCHAR(64)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time(
	start_time TIMESTAMP NOT NULL PRIMARY KEY ,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
  	month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
from {}
IAM_ROLE {}
region 'us-west-2'
format as json {}
timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT as json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplays(
    start_time ,
    user_id ,
    level ,
    song_id ,
    artist_id ,
    session_id ,
    location ,
    user_agent 
)
SELECT
   DISTINCT se.ts,se.userId , se.level,ss.song_id,ss.artist_id,se.sessionId ,se.userAgent ,ss.artist_location
FROM
    staging_events se  
LEFT JOIN
    staging_songs ss ON ( ss.artist_name = se.artist AND ss.title = se.song )
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_users(
    user_id,
    first_name,
    last_name,
    gender,
    level
)SELECT
    DISTINCT userId,firstname,lastname,gender,level
FROM
    staging_events
WHERE
    userId IS NOT NULL 
    AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO dim_songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)SELECT
    DISTINCT song_id,title,artist_id,year,duration
FROM
    staging_songs
""")

artist_table_insert = ("""
INSERT INTO dim_artists(
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT
     DISTINCT artist_id,artist_name as name,artist_location as location,artist_latitude as latitude,artist_longitude as longitude
FROM
    staging_songs
""")

time_table_insert = ("""
INSERT INTO dim_time(
    start_time ,
    hour ,
    day ,
    week ,
    month ,
    year ,
    weekday 
)SELECT
    DISTINCT ts as start_time, EXTRACT (hr 
    from
        ts
    ) as hour, EXTRACT (d 
    from
        ts
    ) as day, EXTRACT (w 
    from
        ts
    ) as week, EXTRACT (mon 
    from
        ts
    ) as month, EXTRACT (y 
    from
        ts
    ) as year, EXTRACT (dow 
    from
        ts
    ) as weekday
FROM
    staging_events
WHERE
    page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
