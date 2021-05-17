# import required modules
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# define function get all data files
def get_files(filepath):
    """
    Description: This function is responsible for extract all source data
    absolute file path and save it to a list for the purpose of loading 
    source data into dataframe in datafram_file_data function.

    Args:
        filepath: path of source data
    
    Returns:
        list of all souce files absolute file path
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    print(f"There are total {len(all_files)} files in {filepath}")
    return all_files  

# define function to process file data to dataframe
def dataframe_file_data(file_path):
    """
    Description: This function is responsible for transfer souce data 
    like json format to pandas datafram.

    Args:
        file_path: path of source data
    
    Returns:
        dataframe of source data
    """
    # generate source data dataframe
    dfs = []
    data_file_path = get_files(file_path)
    for file in data_file_path:
        df = pd.read_json(file, lines=True)
        dfs.append(df)
    data_df = pd.concat(dfs, ignore_index=True)
    print(f"The {file_path} file data has been converted to data frame.")
    return data_df

# define function to process song data to songs table and artists table
def process_song_data(conn, cur, file_path):
    """
    Description: This function is responsible for process song data 
    and load into song table and artist table

    Args:
        conn: psycopg2 connection
        cur: psycopg2 cursor
        file_path: path of source data
    
    Returns:
        None
    """
    song_data = []
    artist_data = []
    song_df = dataframe_file_data(file_path)
    for i, row in song_df.iterrows():
        song_data.append(list(row[['song_id','title','artist_id','year','duration']].values))
        artist_data.append(list(row[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values))
    
    # Insert Record into Song Table
    for data in song_data:
        cur.execute(song_table_insert, data)
        conn.commit()
    print("Song table has been inserted completed.")
    
    for data in artist_data:
        cur.execute(artist_table_insert,data)
        conn.commit()
    print("Artist table has been inserted completed.")
    

# define function to process log data to time, users and songplays table
def process_log_data(conn, cur, file_path):
    """
    Description: This function is responsible for process log data
    and load into time, users and songplays table.

    Args:
        conn: psycopg2 connection
        cur: psycopg2 cursor
        file_path: path of source data
    
    Returns:
        None 
    """
    log_df = dataframe_file_data(file_path)
    # filter log_df
    log_df = log_df[log_df['page']=='NextSong']
    dt = pd.to_datetime(log_df['ts'], unit='ms')
    
    # Extract the timestamp, hour, day, week of year, month, year, and weekday
    timestamp = log_df['ts'].values
    hour = dt.dt.hour.values
    day = dt.dt.day.values
    week_of_year = dt.dt.isocalendar().week.values
    month = dt.dt.month.values
    year = dt.dt.year.values
    weekday = dt.dt.weekday.values

    # generate time data list
    time_data = [timestamp,hour,day,week_of_year,month,year,weekday]
    # generate column list
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    time_dic = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dic)

    # Insert Records into Time Table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()
    print("Time table has been inserted completed.")

    # Extract Data for Users Table
    user_df = log_df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))
        conn.commit()
    print("Users table has been inserted completed.")
    # Extract Data and Songplays Table
    for index, row in log_df.iterrows():
    # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        result = cur.fetchone()

        if result:
            songid, artistid = result
        else:
            songid, artistid = None, None
        # Insert Records into Songplays Table
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
    
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()
    print("Songplays table has been inserted completed.")

def main():
    """
    Descriptions: This function is scripts main method. It is responsible
    for create connection and cursor to postgresql database, process song
    data and log data, and then close cursor and connection.

    Args:
        None
    
    Returns:
        None
    """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: can not connect to database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: can not create a cursor")
        print(e)
    
    song_file_path = 'data/song_data'
    log_file_path = 'data/log_data'

    process_song_data(conn, cur, file_path=song_file_path)
    process_log_data(conn, cur, file_path=log_file_path)

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()







                            

