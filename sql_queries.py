# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS song_play " 
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS "song_play" (
    "songplay_id" SERIAL PRIMARY KEY, 
    "start_time" TIMESTAMP NOT NULL, 
    "userId" SMALLINT NOT NULL, 
    "level" VARCHAR, 
    "song_id" VARCHAR, 
    "artist_id" VARCHAR, 
    "sessionId" SMALLINT,
    "location" VARCHAR,
    "userAgent" VARCHAR
)

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS "users" (
    "userId" SMALLINT PRIMARY KEY,
    "first_name" VARCHAR, 
    "last_name" VARCHAR, 
    "gender" CHAR(1), 
    "level" VARCHAR

)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS "songs" (
    "song_id" VARCHAR PRIMARY KEY,
    "title" VARCHAR,
    "artist_id" VARCHAR,
    "year" SMALLINT,
    "duration" NUMERIC
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS "artists" (
    "artist_id" VARCHAR PRIMARY KEY,
    "artist_name" VARCHAR,
    "artist_location" VARCHAR,
    "artist_latitude" REAL,
    "artist_longitude" REAL
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS "time" (
    "start_time" TIMESTAMP PRIMARY KEY,
    "hour" SMALLINT, 
    "day" SMALLINT, 
    "week" SMALLINT, 
    "month" SMALLINT, 
    "year" SMALLINT, 
    "weekday" SMALLINT
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO "song_play" VALUES (DEFAULT,%s,%s,%s,%s,%s,%s,%s,%s) 
""")

user_table_insert = ("""
INSERT INTO "users" VALUES (%s,%s,%s,%s,%s) ON CONFLICT("userId") DO UPDATE SET level=EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO "songs" VALUES (%s,%s,%s,%s,%s) ON CONFLICT ("song_id") DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO "artists" VALUES (%s,%s,%s,%s,%s) ON CONFLICT ("artist_id") DO NOTHING
""")


time_table_insert = ("""

INSERT INTO "time" VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ("start_time") DO NOTHING

""")

# FIND SONGS

song_select = ("""
SELECT s.song_id,s.artist_id
FROM songs s
JOIN artists a
ON s.artist_id = a.artist_id
WHERE s.title=%s AND a.artist_name=%s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

