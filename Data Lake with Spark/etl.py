import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from datetime import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

with open("output.txt", "a") as f:
    f.write("\n\n#\t#\t#\t\t" + str(datetime.now()) + "\t\t#\t#\t#\n\n")

config = config['KEYS']

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def write_output_data(output, var_name):  
    with open("output.txt", "a") as f:   
        f.write(var_name + ": " + str(output) + "\n")

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    
    write_output_data(spark, "spark")
    write_output_data(input_data, "input_data")
    write_output_data(output_data, "output_data")
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    write_output_data(song_data, "song_data (path)")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM songs_table_DF
        ORDER BY song_id
    """)
    
    songs_table_path = output_data + 'songss/songs_table.parquet'
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id")\
        .parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id AS artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM artists_table_DF
        ORDER BY artist_id desc
    """)
    
    artists_table_path = output_data + 'artists/artists_table.parquet'
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(artists_table_path)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    df.createOrReplaceTempView("users_table_view")
    users_table = spark.sql("""
        SELECT userId,
            firstName,
            lastName,
            gender,
            level
        FROM users_table_view
        ORDER BY lastName
    """)
    
    # write users table to parquet files
    users_table_path = output_data + 'users/users_table.parquet'
    users_table.write.mode("overwrite").parquet(users_table_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn("timestamp", datetime.fromtimestamp("ts" / 1000.0))
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = df.withColumn("datetime", datetime \
                                        .fromtimestamp("ts" / 1000.0) \
                                        .strftime('%Y-%m-%d %H:%M:%S'))
    
    # extract columns to create time table
    df.createOrReplaceTempView("time_table_view")
    time_table = spark.sql("""
        SELECT DISTINCT datetime AS start_time,
            hour(timestamp) AS hour,
            day(timestamp)  AS day,
            weekofyear(timestamp) AS week,
            month(timestamp) AS month,
            year(timestamp) AS year,
            dayofweek(timestamp) AS weekday
        FROM time_table_view
        ORDER BY start_time
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + 'time/time_table.parquet'
    time_table.write.mode("overwrite").parquet(time_table_path)

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    df.createOrReplaceTempView("songplays_table_view")
    songplays_table = spark.sql("""
        SELECT songplay_id,
            timestamp,
            userId,
            level,
            song_id,
            artist_id,
            sessionId,
            location,
            userAgent
        FROM songplays_table_view
        ORDER BY (userId, sessionId)
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + 'songplays/songplays_table.parquet'
    songplays_table.write.mode("overwrite").parquet(songplays_table_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-project-udacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
