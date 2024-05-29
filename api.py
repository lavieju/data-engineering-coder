import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from database import conn

with open("/Users/julianlavie/Desktop/client_secret_spotify.txt",'r') as f:
    client_secret = f.read()
#client_id = 'a3574c05cdc7499b8c272b2861bb048c'
client_id = spotify_client_id
client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


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
        response_flat_list = [item for sublist in response_list for item in sublist]

    
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
        #sumar rollback
        raise Exception('Data length does not match loaded rows')
        return False
    


def insert_data(data, pivot_inserted, conn):  
    
#    if pivot_inserted == False:
#        raise Exception('Pivot table has not been executed')
#        return False
    
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