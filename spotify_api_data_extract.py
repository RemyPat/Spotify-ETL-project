import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime 

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    #add your keys here generated from the app created on https://developer.spotify.com/
    #Save the creditionals in Environmental variables to abstract the sensitive creditionals
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp=spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    #link of top 50 globally played songs
    playlist_link="https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
    playlist_ID=playlist_link.split("/")[-1]
    
    data=sp.playlist_tracks(playlist_ID)
    
    #boto3 is AWS SDK to communicate with AWS resources
    bucket_name = 'spotify-etl-project-remy' 
    file_name = 'spotify_raw_' + str(datetime.now()) + '.json' 
    file_content = json.dumps(data) 
    
    s3 = boto3.client('s3') 
    s3.put_object(Body=file_content, Bucket=bucket_name, Key='raw_data/to_process/' + file_name)
    return { 
        'statusCode': 200, 
        'body': f'Data uploaded successfully in {file_name}.' 
         }
    

