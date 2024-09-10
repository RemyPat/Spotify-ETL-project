import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    album_list=[]
    for row in data["items"]:
        album_id=row['track']['album']['id']
        album_name=row['track']['album']['name']
        album_release_date=row['track']['album']['release_date']
        album_total_tracks=row['track']['album']['total_tracks']
        album_url=row['track']['album']['external_urls']['spotify']
        album_element={'album_id':album_id,'album_name':album_name,'album_release_date':album_release_date,
                            'album_total_tracks':album_total_tracks,'album_url':album_url}
        album_list.append(album_element)
    return album_list
    
def artist(data):
    artist_list=[]
    for row in data["items"]:
        for key,value in row.items():
            if key=='track':
                for artist in value['artists']:
                    artist_id=artist['id']
                    artist_name=artist['name']
                    artist_url=artist['external_urls']['spotify']
                    artist_dict={'artist_id':artist_id,'artist_name':artist_name,'artist_url':artist_url}
                    artist_list.append(artist_dict)
    return artist_list  
        

def song (data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)
    return song_list

def lambda_handler(event, context):

    s3=boto3.client('s3')
    Bucket='spotify-etl-project-remy'
    Key='raw_data/to_process/'
    
    files=s3.list_objects_v2(Bucket=Bucket,Prefix=Key)['Contents']
    
    
    spotify_data=[] #list of json_data
    spotify_keys=[] #list of json file names
    for file in files:
        file_name=file['Key']
        if file_name.split('.')[-1]=='json':
            # Retrieve the JSON file from S3
            response=s3.get_object(Bucket=Bucket,Key=file_name)
            
            # Read the content of the file
            file_content=response['Body']
            
            # Parse the JSON content
            json_object=json.loads(file_content.read())
            spotify_data.append(json_object)
            spotify_keys.append(file_name)
            
    #loop to call the album,artist and song functions on each json file content file      
    for data in spotify_data:
        album_list=album(data)
        artist_list=artist(data)
        song_list=song(data)
        
        album_df=pd.DataFrame.from_dict(album_list)
        album_df=album_df.drop_duplicates(subset=['album_id'])
        
        artist_df=pd.DataFrame.from_dict(artist_list)
        artist_df=artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df=pd.DataFrame.from_dict(song_list)
        
        # album_df['album_release_date']=pd.to_datetime(album_df['album_release_date'])
        song_df['song_added']=pd.to_datetime(song_df['song_added'])
        
        song_key='transformed_data/songs_data/song_transformed_'+ str(datetime.now())+'.csv'
        
        song_buffer=StringIO() #create an In-Memory File-Like Object 
        song_df.to_csv(song_buffer, index=False) #Write the DataFrame to the In-Memory Buffer so that it can be extracted as a string
        song_content = song_buffer.getvalue() #Extract the CSV Content as a String
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content) # Upload the CSV Data to S3
        
        
        album_key='transformed_data/album_data/album_tranformed_'+str(datetime.now())+'.csv'
        
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
    
        artist_key='transformed_data/artist_data/artist_tranformed_'+str(datetime.now())+'.csv'
        
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer,index=False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artist_key,Body=artist_content)
    
        
    #a loop to move the processed JSON files to 'raw_data/processed/'folder and delete them from 'raw_data/to_process/'   
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket': Bucket,'Key': key}
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])   
        s3_resource.Object(Bucket, key).delete()   
    

