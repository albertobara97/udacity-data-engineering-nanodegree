import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
    create table staging_events (
        artist text,
        auth text not null,
        firstName text,
        gender char (1),
        itemInSession int not null,
        lastName text,
        length numeric,
        level text not null,
        location text,
        method text not null,
        page text not null,
        registration numeric,
        sessionId int not null,
        song text,
        status int not null,
        ts numeric not null,
        userAgent text,
        userId int
    )
""")

staging_songs_table_create = ("""
    create table staging_songs (
        num_songs int not null,
        artist_id text not null,
        artist_latitude text,
        artist_longitude text,
        artist_location text,
        artist_name text not null,
        song_id text not null,
        title text not null,
        duration numeric not null,
        year int not null
    )
""")

songplay_table_create = ("""
    create table songplays (
        songplay_id int identity(0, 1) primary key,
        start_time timestamp not null,
        user_id int not null,
        level text not null,
        song_id text,
        artist_id text,
        session_id int not null,
        location text,
        user_agent text not null
    )
""")

user_table_create = ("""
    create table users (
        user_id int primary key,
        first_name text not null,
        last_name text not null,
        gender char (1) not null,
        level text not null
    )
""")

song_table_create = ("""
    create table songs (
        song_id text primary key,
        title text not null,
        artist_id text not null,
        year int not null,
        duration numeric not null
    )
""")

artist_table_create = ("""
    create table artists (
        artist_id text primary key,
        name text not null,
        location text,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    create table time(
        start_time timestamp primary key,
        hour int not null,
        day int not null,
        week int not null,
        month int not null,
        year int not null,
        weekday int not null
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {} iam_role '{}'
    format as json {}
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role '{}'
    json 'auto'
""").format(config['S3']['SONG_DATA'], 
            config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays(
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id,
        session_id, 
        location, 
        user_agent
    )
    select
        timestamp 'epoch' + se.ts / 1000 * interval '1 second' as start_time,
        se.userId as user_id,
        se.level,
        sg.song_id,
        sg.artist_id,
        se.sessionId as session_id,
        se.location,
        se.userAgent as user_agent
    from staging_events se
    left join staging_songs sg on se.song = sg.title and 
        se.artist = sg.artist_name
    where se.page = 'NextSong'
""")

user_table_insert = ("""
    insert into users
    select se.userId, 
        se.firstName, 
        se.lastName, 
        se.gender, 
        se.level
    from staging_events se
    join (
        select max(ts) as ts, 
            userId
        from staging_events
        where page = 'NextSong'
        group by userId
    ) sev on se.userId = sev.userId and 
        se.ts = sev.ts
""")

song_table_insert = ("""
    insert into songs
    select
        song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs
""")

artist_table_insert = ("""
    insert into artists
    select distinct
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from staging_songs
""")

time_table_insert = ("""
    insert into time
    select
        t.start_time,
        extract(hour from t.start_time) as hour,
        extract(day from t.start_time) as day,
        extract(week from t.start_time) as week,
        extract(month from t.start_time) as month,
        extract(year from t.start_time) as year,
        extract(weekday from t.start_time) as weekday
    from (
        select distinct
            timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
        from staging_events
        where page = 'NextSong'
    ) t
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
