import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd 
import psycopg2
import uuid



with open("/Users/julianlavie/Desktop/client_secret_spotify.txt",'r') as f:
    client_secret = f.read()
client_id = 'a3574c05cdc7499b8c272b2861bb048c'
client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


#Redshift connection
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user="julianlavie16_coderhouse"
with open("/Users/julianlavie/Desktop/password_redshift.txt",'r') as f:
    database_password= f.read()

try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=database_password,
        port='5439'
    )
    print("Connected successfully!")
    
except Exception as e:
    print("It has not been able to make the connection")
    print(e)


artists = ['79R7PUc6T6j09G8mJzNml2', '4EmjPNMuvvKSEAyx7ibGrs', '3HrbmsYpKjWH1lzhad7alj']


def transform_data(artist_data, tracks_data):
    data = []
    for track in tracks_data['tracks']:
        track_id = track['id']
        track_name = track['name']
        track_popularity = track['popularity']
        track_duration = track['duration_ms']
        artist_name = artist_data['name']
        artist_followers = artist_data['followers']['total']
        artist_popularity = artist_data['popularity']
        album_name = track['album']['name']
        album_release_date = track['album']['release_date']
        album_total_tracks = track['album']['total_tracks']
        has_collaboration = len(track['artists']) > 1
        
        row = {
            'track_id': track_id,
            'track_name': track_name,
            'track_popularity': track_popularity,
            'track_duration': track_duration,
            'artist_name': artist_name,
            'artist_followers': artist_followers,
            'artist_popularity': artist_popularity,
            'album_name': album_name,
            'album_release_date': album_release_date,
            'album_total_tracks': album_total_tracks,
            'has_collaboration': has_collaboration
        }
        
        data.append(row)
        
        
    
    return data

def get_and_transform_artists_data(artists):
    response_list = []
    for artist in artists:
        artist_response_data = sp.artist(artist)
        top_tracks_response_data = sp.artist_top_tracks(artist)
        result = transform_data(artist_response_data, top_tracks_response_data)
        response_list.append(result)
        response_flat_list = [track for artist_track_list in response_list for track in artist_track_list]

    
    return response_flat_list




def insert_data_pivot(data_to_load, conn):
    data = pd.DataFrame(data_to_load)
    rows_count_to_load = len(data)
    
    with conn.cursor() as cursor:
        cursor.execute("""
                              CREATE TABLE IF NOT EXISTS julianlavie16_coderhouse.spotify_data_pivot
                              (
                                  id VARCHAR(50) primary key
                                  ,track_id VARCHAR(50)
                                  ,track_name VARCHAR(300)   
                                  ,track_popularity INTEGER
                                  ,track_duration INTEGER   
                                  ,artist_name VARCHAR(200)   
                                  ,artist_followers INTEGER  
                                  ,artist_popularity INTEGER 
                                  ,album_name VARCHAR(250)
                                  ,album_release_date VARCHAR(100)   
                                  ,album_total_tracks INTEGER
                                  ,has_collaboration BOOLEAN
                                  )
                              """)
                              
        insert_query = """
        INSERT INTO julianlavie16_coderhouse.spotify_data_pivot (
        id, track_id, track_name, track_popularity, track_duration, artist_name,
        artist_followers, artist_popularity, album_name, album_release_date,
        album_total_tracks, has_collaboration
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for index, row in data.iterrows():
            cursor.execute(insert_query, (
            str(uuid.uuid4()),
            row['track_id'], row['track_name'], row['track_popularity'], row['track_duration'], row['artist_name'],
            row['artist_followers'], row['artist_popularity'], row['album_name'], row['album_release_date'],
            row['album_total_tracks'], row['has_collaboration']
            ))
            

        cursor.execute("""
                              SELECT count(*) FROM julianlavie16_coderhouse.spotify_data_pivot
                              """)
        rows_loaded_pivot = cursor.fetchone()[0]
    
    if rows_loaded_pivot == rows_count_to_load:
        print("Pivot import has been done successfully")
        conn.commit()
        return True
    else:
        raise Exception('Data length does not match loaded rows')
        return False
    


def insert_data(data, pivot_inserted, conn):  
    
    if pivot_inserted == False:
        raise Exception('Pivot table has not been executed')
        return False
    
    rows_count_to_load = len(data)  
    
    with conn.cursor() as cursor:    
        cursor.execute("""
                              DROP TABLE IF EXISTS julianlavie16_coderhouse.spotify_data
                              """)
    
    
        cursor.execute("""
                              CREATE TABLE IF NOT EXISTS julianlavie16_coderhouse.spotify_data
                              (
                                  id VARCHAR(50) primary key
                                  ,track_id VARCHAR(50)
                                  ,track_name VARCHAR(300)   
                                  ,track_popularity INTEGER
                                  ,track_duration INTEGER   
                                  ,artist_name VARCHAR(200)   
                                  ,artist_followers INTEGER  
                                  ,artist_popularity INTEGER 
                                  ,album_name VARCHAR(250)
                                  ,album_release_date VARCHAR(100)
                                  ,album_total_tracks INTEGER
                                  ,has_collaboration BOOLEAN
                                  )
                              """)
    
    
        cursor.execute("""
                              INSERT INTO julianlavie16_coderhouse.spotify_data 
                              SELECT * FROM julianlavie16_coderhouse.spotify_data_pivot
                              """)
                              
        cursor.execute("""
                              SELECT count(*) FROM julianlavie16_coderhouse.spotify_data
                              """)
        rows_loaded = cursor.fetchone()[0]
        
        if rows_count_to_load == rows_loaded:
            cursor.execute("""
                                  DROP TABLE IF EXISTS julianlavie16_coderhouse.spotify_data_pivot
                                  """)
            print("Data imported successfully!")
            conn.commit()
        else:
            raise Exception('Data length does not match loaded rows')
    
    conn.close()
    
    return data

                          
    
        
data = get_and_transform_artists_data(artists)
pivot_insert = insert_data_pivot(data, conn)
insert_data = insert_data(data, pivot_insert, conn)
    