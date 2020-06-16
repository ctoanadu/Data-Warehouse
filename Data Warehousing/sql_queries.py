import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE= config['IAM_ROLE']['ARN']
LOG_DATA= config['S3']['LOG_DATA']
LOG_JSONPATH=config['S3']['LOG_JSONPATH']
SONG_DATA=config['S3']['SONG_DATA']


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession int,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId int,
song varchar,
status int,
ts timestamp,
userAgent varchar,
userId int);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
artist_id varchar,
artist_latitude varchar,
artist_location varchar,
artist_longitude varchar,
artist_name varchar,
duration float,
num_song int,
song_id varchar,
title varchar,
year int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
songplay_id  int IDENTITY(0,1) PRIMARY KEY,
start_time timestamp NOT NULL REFERENCES time(start_time),
user_id int NOT NULL REFERENCES users(user_id),
level varchar,
song_id varchar REFERENCES songs(song_id),
artist_id varchar REFERENCES artists(artist_id),
session_id int,
location varchar,
user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
user_id int primary key,
first_name varchar NOT NULL,
last_name varchar NOT NULL,
gender varchar,
level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
song_id varchar primary key,
title varchar NOT NULL,
artist_id NOT NULL varchar REFERENCES artists(artist_id),
year int,
duration float NOT NULL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id varchar primary key,
name varchar NOT NULL,
location varchar,
latitude float,
longitude float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time timestamp primary key,
hour int,
day int,
week int,
month int,
year int,
weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
from {}
iam_role{}
json {};
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs
from {}
iam role{};
""").format(SONG_DATA,IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT to_timestamp (stanging_events.ts/1000) AS start_time,
staging_events.userId,
staging_events.level,
staging_songs.song_id,
staging_songs.artist_id,
staging_events.sessionId,
staging_events.location,
staging_events.userAgent
FROM staging_events
JOIN staging_songs
ON staging_events.artist=staging_songs.artist_name
WHERE page='NextSong';

""")

user_table_insert = ("""INSERT INTO users (user_id,first_name, last_name,gender,level)
SELECT userId,firstName,lastName,gender,level
FROM staging_events
WHERE page='NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT song_id,title,artist_id,year,duration
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artist (artist_id,name,location,latitude,longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time(start_time,hour,day,week,month,year)
SELECT start_time, EXTRACT (hour FROM start_time),
EXTRACT (day FROM start_time),
EXTRACT (week FROM start_time),
EXTRACT (month FROM start_time),
EXTRACT (year FROM start_time)
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create ]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
