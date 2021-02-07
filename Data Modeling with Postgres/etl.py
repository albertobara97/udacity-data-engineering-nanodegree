import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):
    '''
    Process song file and populates song and artist table with its data.
    
        Parameters:
            cur (object): Postgresql cursor
            filepath (string): path of file to process
    '''
    
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Process log file and populates time, user and songplay tables.
    
        Parameters:
            cur (object): Postgresql cursor
            filepath (string): path of file to process 
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week_number', 'month', 'year', 'weekday')
    time_df = pd.DataFrame({column_labels[i]: time_data[i] for i in range(len(column_labels))})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_data = (df['userId'], df['firstName'], df['lastName'], df['gender'], df['level'])
    user_headers = ('user_id', 'first_name', 'last_name', 'gender', 'level')
    user_df = pd.DataFrame({user_headers[i]: user_data[i] for i in range(len(user_headers))}) 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row['ts'], unit='ms'), row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Get all the files to process and iterates over them calling the corresponding function for the processing
        
        Parameters:
            cur (object): Postgresql cursor
            conn (object): Postgresql connection object
            filepath (string): path of directory to scan
            func (function): function defined in this file
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    Connects to sparkify database and passes the connection and cursor objects to the corresponding functions to perform the ETL 
    '''
    
    #Connects to sparkify db
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    
    #Creates a crusor for the established connection
    cur = conn.cursor()
    
    #Call process_data functions
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    #Closes the connection with the db
    conn.close()


if __name__ == "__main__":
    main()