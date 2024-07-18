import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd 
from api import get_and_transform_artists_data, insert_data
from utils import send_email


artists = ['79R7PUc6T6j09G8mJzNml2', '4EmjPNMuvvKSEAyx7ibGrs', '3HrbmsYpKjWH1lzhad7alj']

#EXTRACT AND TRANSFORM
#Get and transform spotify data for selected artists
data = get_and_transform_artists_data(artists)

#Insert data into production table and validate length is correct
insert_data = insert_data(data)

#Send email with the results
send_email = send_email(insert_data)
