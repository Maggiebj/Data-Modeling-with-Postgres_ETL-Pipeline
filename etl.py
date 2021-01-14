import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def get_files(filepath):
    """
    The function get all files' paths and names by walking through the dataset of files.
    
    Parameters:
    filepath(str): The path of dataset of files. 
    
    Returns:
    list: Files' paths and names.
    """
    all_files=[]
    for root,dirs,files in os.walk(filepath):
        for f in files:
            file = glob.glob(os.path.join(root,f))
            all_files.extend(file)
    return all_files

def process_song_file(cur,conn, datafiles):
    """
    The function extracts and transforms data from song magadata json files and loads them into tables of 'songs' and 'artists'.
    
    Parameters:
    cur(cursor): The cursor of the database connection.
    conn(connection): The connection of the database.
    datafiles(list): The list of songs magadata json files' paths and names.
    
    Returns:
    None
    """
    # open song file
    df = pd.DataFrame()
    for f in datafiles:
        df = df.append(pd.read_json(f,lines=True))
    print (df.shape[0])
    
    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]].copy()
    #print (song_data)
    for i ,row in song_data.iterrows():
        #print (i,row)
        
        cur.execute(song_table_insert, row)
        conn.commit()
       
    
    cur.execute("SELECT * FROM songs LIMIT 10")
    conn.commit()
    row = cur.fetchone()
    print ("song table sample print")
    print (row)
    
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].copy()
    for i ,row in artist_data.iterrows():
    
        cur.execute(artist_table_insert, row)
        conn.commit()
        
    cur.execute("SELECT * FROM artists LIMIT 10")
    conn.commit()
    row = cur.fetchone()
    print ("artists table sample print")
    print (row)
      
    

def process_log_file(cur, conn,datafiles):
    """
    The function extracts and transforms data from users activities log json files and loads them into tables of "users","time","song_file".
    
    Parameters:
    cur: The cursor of the database connection.
    conn: The connection of the database.
    datafiles(list): The list of users activities json files' paths and names.
    
    Returns:
    None 
    """
    # open log file
    df = pd.DataFrame()
    for f in datafiles:
        df = df.append(pd.read_json(f,lines=True))
    
    # filter by NextSong action
    df = df[df.page=='NextSong'].copy() 

    # convert timestamp column to datetime
    time_df = pd.DataFrame()
    time_df['start_time'] = df.ts.apply(lambda x: pd.Timestamp(x,unit='ms'))
    time_df['hour'] = time_df['start_time'].dt.hour
    time_df['day'] = time_df['start_time'].dt.day
    time_df['week'] = time_df['start_time'].dt.week
    time_df['month'] = time_df['start_time'].dt.month
    time_df['year'] = time_df['start_time'].dt.year
    time_df['weekday'] = df.ts.apply(lambda x:pd.Timestamp(x,unit='ms').dayofweek)
    
   

    for i, row in time_df.iterrows():
    
        cur.execute(time_table_insert, row)
        conn.commit()
        
   
    cur.execute("SELECT * FROM time LIMIT 10")
    conn.commit()
    row = cur.fetchone()
    print ("time table sample print")
    print (row)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        
        cur.execute(user_table_insert, row)
        conn.commit()
       
               
    cur.execute("SELECT * FROM users LIMIT 10")
    conn.commit()
    print ("user table sample print")
    row = cur.fetchone()
    print (row)
        
        
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
        songplay_data = (pd.Timestamp(row.ts,unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
       
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()
        
    
    cur.execute("SELECT * FROM song_play LIMIT 10")
    conn.commit()
    print ("song_play table sample print")
    row = cur.fetchmany(5)
    for r in row:
        print (r)

def process_data(cur, conn, filepath, func):
    """
    The function gets json data files' paths and names and invokes other functions to do ETL.
    
    Parameters:
    cur: The cursor of the database connection.
    conn: The connection of the database.
    filepath: The filepath of dataset.
    func: The function for ETL .
    
    Returns:
    None
    """
    # get all files matching extension from directory
    #all_files = []
    #for root, dirs, files in os.walk(filepath):
    #    files = glob.glob(os.path.join(root,'*.json'))
    #    for f in files :
     #       all_files.append(os.path.abspath(f))
    all_files = get_files(filepath)
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    func(cur,conn,all_files)
    
    
    

def main():
    conn = psycopg2.connect("host= dbname= user= password=")
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
   
    conn.close()


if __name__ == "__main__":
    main()
