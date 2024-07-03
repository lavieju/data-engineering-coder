import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import uuid
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv() 

client_secret = os.environ.get('spotify_client_secret')    
client_id = os.environ.get('spotify_client_id')
client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

url = os.environ.get('redshift_url')
database = os.environ.get('redshift_database')
user = os.environ.get('redshift_user')
database_password = os.environ.get('redshift_password')


def transform_data(artist_data, tracks_data, album_data):
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
        album_release_date_precision = track['album']['release_date_precision']
        total_artist_album = len(album_data['items'])
        is_single = track['album']['album_type'] == 'single'
        
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
            'has_collaboration': has_collaboration,
            'album_release_date_precision': album_release_date_precision,
            'total_artist_album': total_artist_album,
            'is_single': is_single

        }
        
        data.append(row)
        
        
    
    return data

def get_and_transform_artists_data(artists):
    response_list = []
    for artist in artists:
        artist_response_data = sp.artist(artist)
        top_tracks_response_data = sp.artist_top_tracks(artist)
        albums_data = sp.artist_albums(artist)
        result = transform_data(artist_response_data, top_tracks_response_data, albums_data)
        response_list.append(result)
        response_flat_list = [item for sublist in response_list for item in sublist]

    
    return response_flat_list


def insert_data(data_to_load):
    data = pd.DataFrame(data_to_load)
    rows_count_to_load = len(data)

    try:
        conn = psycopg2.connect(
        host = url,
        dbname = database,
        user = user,
        password = database_password,
        port='5439'
        )
        print("Connected successfully!")
        #return conn
        
    except Exception as e:
        print("It has not been able to make the connection")
        print(e)
    
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
                                  ,album_release_date_precision VARCHAR(100)
                                  ,total_artist_album INTEGER
                                  ,is_single BOOLEAN

                                  )
                              """)
                              
        insert_query = """
                            INSERT INTO julianlavie16_coderhouse.spotify_data (
                            id, track_id, track_name, track_popularity, track_duration, artist_name,
                            artist_followers, artist_popularity, album_name, album_release_date,
                            album_total_tracks, has_collaboration, album_release_date_precision,
                            total_artist_album, is_single
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
        
        for index, row in data.iterrows():
            cursor.execute(insert_query, (
            str(uuid.uuid4()),
            row['track_id'], row['track_name'], row['track_popularity'], row['track_duration'], row['artist_name'],
            row['artist_followers'], row['artist_popularity'], row['album_name'], row['album_release_date'],
            row['album_total_tracks'], row['has_collaboration'], row['album_release_date_precision'], row['total_artist_album'],
            row['is_single']
            ))
            
        cursor.execute("""
                              SELECT count(*) FROM julianlavie16_coderhouse.spotify_data
                              """)
        rows_loaded = cursor.fetchone()[0]
    
    if rows_loaded == rows_count_to_load:
        print("Data import has been done successfully")
        conn.commit()
    else:
        raise Exception('Data length does not match loaded rows')