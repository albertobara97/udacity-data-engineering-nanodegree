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
    """
    Reads and process the song data files into the songs and artists tables
    
    Parameters:
        - spark: Connection object to spark session
        - input_data: path to Udacity S3 Bucket
        - output_data: path to our S3 Bucket
    """
    
    write_output_data(spark, "spark")
    write_output_data(input_data, "input_data")
    write_output_data(output_data, "output_data")
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    write_output_data(song_data, "song_data (path)")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration') \
                    .dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    songs_table_path = output_data + 'songs/songs_table.parquet'
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id")\
        .parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = df.select('artist_id', 
                              'artist_name', 
                              'artist_location',
                              'artist_latitude', 
                              'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude') \
                      .dropDuplicates()
    
    artists_table.createOrReplaceTempView('artists')
    
    artists_table_path = output_data + 'artists/artists_table.parquet'
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(artists_table_path)


def process_log_data(spark, input_data, output_data):
    """
    Reads and process the log file into the time, users and songplays tables
    
    Parameters:
        - spark: Connection object to spark session
        - input_data: path to Udacity S3 Bucket
        - output_data: path to our S3 Bucket
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 
                            'firstName', 
                            'lastName',
                            'gender', 
                            'level')
                  .dropDuplicates()
    
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table_path = output_data + 'users/users_table.parquet'
    users_table.write.mode("overwrite").parquet(users_table_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    df.createOrReplaceTempView("time_table_view")
    time_table = df.select('datetime') \
                    .withColumn('start_time', df.datetime) \
                    .withColumn('hour', hour('datetime')) \
                    .withColumn('day', dayofmonth('datetime')) \
                    .withColumn('week', weekofyear('datetime')) \
                    .withColumn('month', month('datetime')) \
                    .withColumn('year', year('datetime')) \
                    .withColumn('weekday', dayofweek('datetime')) \
                    .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + 'time/time_table.parquet'
    
    time_table.write.mode("overwrite").partitionBy('year', 'month').parquet(time_table_path)

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    df = df.alias("staging_events")
    song_df = song_df.alias("staging_songs")
    
    df = df.join(song_df, col('staging_events.artist') == col('staging_songs.artist_name'), 'inner')
    
    
    songplays_table = df.select(
        col('staging_events.datetime').alias('start_time'),
        col('staging_events.userId').alias('user_id'),
        col('staging_events.level').alias('level'),
        col('staging_songs.song_id').alias('song_id'),
        col('staging_songs.artist_id').alias('artist_id'),
        col('staging_events.sessionId').alias('session_id'),
        col('staging_events.location').alias('location'), 
        col('staging_events.userAgent').alias('user_agent'),
        year('staging_events.datetime').alias('year'),
        month('staging_events.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + 'songplays/songplays_table.parquet'
    
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(songplays_table_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-project-udacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
