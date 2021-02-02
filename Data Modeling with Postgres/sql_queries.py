# DROP TABLES

songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id int, 
        start_time timestamp, 
        user_id int, 
        level varchar(150), 
        song_id int, 
        artist_id int, 
        session_id int, 
        location varchar(150), 
        user_agent varchar(150)
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id int, 
        first_name varchar(50), 
        last_name varchar(50), 
        gender char(1), 
        level varchar(50)
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id varchar(50), 
        title varchar(100), 
        artist_id varchar(50), 
        year int, 
        duration real
    )
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id varchar(150), 
        name varchar(150), 
        location varchar(150), 
        latitude real, 
        longitude real
    )
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time timestamp, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday varchar(25)
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays(
        songplay_id, 
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
        )
    VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
        )
    VALUES (%s, %s, %s, %s, %s)

""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    VALUES (%s, %s, %s, %s, %s)
""")


time_table_insert = ("""
    INSERT INTO time(
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, 
        songs.artist_id
    FROM songs
    INNER JOIN artists on songs.artist_id = artists.artist_id
    WHERE songs.title = %s AND
        artists.name = %s AND
        songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]