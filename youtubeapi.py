# YouTube Data Harvesting Application

# Libraries
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


# Set up MongoDB
mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['youtube_data']

# Set up PostgreSQL connection
apidb = psycopg2.connect(host="localhost", user="postgres", password="Rashmi.6", database="youtube_data", port="5432")
cursor=apidb.cursor()

# Function to retrieve YouTube data using Google API
def Api_connection():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = 'AIzaSyB3ZchJVvxeIpPHIz1IVRg06HLnzdTo75U'
    
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = Api_connection()

# Retrieve channel details
def get_channel(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                  Channel_Id =i["id"],
                  Channel_Des=i['snippet']['description'],
                  Subscriber_Count=i['statistics']['subscriberCount'],
                  Video_Count=i['statistics']['videoCount'],
                  Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],
                  Channel_Views=i['statistics']['viewCount'])
        return data
    
# Retrieve video-ids details
def get_videoids(channel_id):#3
    video_ids=[]
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    token=None
    while True:
        response1=youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        token=response1.get('nextPageToken')
        if token is None:
            break
    return video_ids

# Retrieve Video details
def get_video(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id =item['snippet']['channelId'],
                    Video_Id=item["id"],
                    video_name=item["snippet"]["title"],
                    Tags=item['snippet'].get("tags"),
                    thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                    video_description=item["snippet"]["description"],
                    published_date=item["snippet"]["publishedAt"],
                    duration=item["contentDetails"]["duration"],
                    view_count=item["statistics"].get("viewCount"),
                    comment_count=item["statistics"].get("commentCount"),
                    like_count=item["statistics"].get("likeCount"),
                    favorite_count=item["statistics"]["favoriteCount"],
                    Defination=item["contentDetails"]["definition"],
                    caption_status=item["contentDetails"]["caption"])
            video_data.append(data)
    return video_data

# Retrieve comment details
def get_comment(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
            
            for item in response['items']:
                data=dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                          Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                          comment_author_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          comment_text = item['snippet']['topLevelComment']['snippet']['textOriginal'],
                          comment_published_date = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# Retrieve playlist details 
def get_playlist(channel_id):
    token = None
    All_data = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=token
        )

        response = request.execute() 

        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id =item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        Published_at=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)

        token = response.get('nextPageToken')
        if token is None:
            break

    return All_data

# Migrate data to MongoDB
def channel_details(channel_id):
    ch_details=get_channel(channel_id)
    pl_details=get_playlist(channel_id)
    vi_ids=get_videoids(channel_id)
    vi_details=get_video(vi_ids)
    com_details=get_comment(vi_ids)
    
    youtubedb=mongo_db['channel_details']
    youtubedb.insert_one({"Channel_information":ch_details,"Playlist_information":pl_details,
                           "Video_information":vi_details,"Comment_information":com_details})
    
    return "upload completed successfully"

# Create PostgreSQL tables for YouTube data
def channels_table():
    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    apidb.commit()

    try:
        create_query='''CREATE TABLE IF NOT EXISTS channels(Channel_Name VARCHAR(255),
                                                    Channel_Id VARCHAR(255) primary key,
                                                    Channel_Des TEXT,
                                                    Subscriber_Count BIGINT,
                                                    Video_Count INTEGER,
                                                    Playlist_Id VARCHAR(255),
                                                    Channel_Views BIGINT)'''
        cursor.execute(create_query)
        apidb.commit()
    except:
        print("channels tables already created")

    ch_list=[]
    mongo_db=mongo_client['youtube_data']
    youtubedb=mongo_db['channel_details']
    for ch_data in youtubedb.find({},{"_id":0,"Channel_information":1}):
        ch_list.append(ch_data["Channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(
            Channel_Name, 
            Channel_Id,
            Channel_Des,
            Subscriber_Count,
            Video_Count,
            Playlist_Id,
            Channel_Views)
            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Channel_Des'],
                row['Subscriber_Count'],
                row['Video_Count'],
                row['Playlist_Id'],
                row['Channel_Views'])
        try:
            cursor.execute(insert_query,values)
            apidb.commit()
        except:
            print("Channel values are already inserted")

#playlist details table
def playlist_table():       
    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    apidb.commit()
    
    create_query='''CREATE TABLE IF NOT EXISTS playlists(Playlist_Id VARCHAR(255)primary key,
                                                        Title VARCHAR(255),
                                                        Channel_Id VARCHAR(255),
                                                        Channel_Name VARCHAR(100),
                                                        Published_at timestamp,
                                                        Video_Count INT)'''
    cursor.execute(create_query)
    apidb.commit()

    pl_list=[]
    mongo_db=mongo_client['youtube_data']
    youtubedb=mongo_db['channel_details']
    for pl_data in youtubedb.find({},{"_id":0,"Playlist_information":1}):
        for i in range(len(pl_data["Playlist_information"])):
            pl_list.append(pl_data["Playlist_information"][i])
    df1=pd.DataFrame(pl_list)
    
    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id, 
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            Published_at,
                                            Video_Count)
                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Published_at'],
                row['Video_Count'])
        
        cursor.execute(insert_query,values)
        apidb.commit()
            
#videos details table
def videos_table():    
    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    apidb.commit()
    
    create_query='''CREATE TABLE IF NOT EXISTS videos(Channel_Name VARCHAR(255),
                                                    Channel_Id VARCHAR(255),
                                                    Video_Id VARCHAR(50) primary key,
                                                    video_name VARCHAR(255),
                                                    Tags text,
                                                    thumbnail VARCHAR(255),
                                                    video_description text,
                                                    published_date timestamp,
                                                    duration interval,
                                                    view_count BIGINT,
                                                    comment_count INT,
                                                    like_count BIGINT,
                                                    favorite_count INT,
                                                    Defination VARCHAR(100),
                                                    caption_status VARCHAR(100))'''
    cursor.execute(create_query)
    apidb.commit()
    
    vi_list=[]
    mongo_db=mongo_client['youtube_data']
    youtubedb=mongo_db['channel_details']
    for vi_data in youtubedb.find({},{"_id":0,"Video_information":1}):
        for i in range(len(vi_data["Video_information"])):
            vi_list.append(vi_data["Video_information"][i])
    df2=pd.DataFrame(vi_list)
    
    for index,row in df2.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    video_name,
                                                    Tags,
                                                    thumbnail,
                                                    video_description,
                                                    published_date,
                                                    duration,
                                                    view_count,
                                                    comment_count,
                                                    like_count,
                                                    favorite_count,
                                                    Defination,
                                                    caption_status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['video_name'],
                    row['Tags'],
                    row['thumbnail'],
                    row['video_description'],
                    row['published_date'],
                    row['duration'],
                    row['view_count'],
                    row['comment_count'],
                    row['like_count'],
                    row['favorite_count'],
                    row['Defination'],
                    row['caption_status'])
            cursor.execute(insert_query,values)
            apidb.commit()

#comments details table
def comments_table():
    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    apidb.commit()
    create_query='''CREATE TABLE IF NOT EXISTS comments(Comment_Id VARCHAR(255) primary key,
                                                        Video_Id VARCHAR(255),
                                                        comment_author_name VARCHAR(50),
                                                        comment_text text,
                                                        comment_published_date timestamp)'''
    cursor.execute(create_query)
    apidb.commit()

    com_list=[]
    mongo_db=mongo_client['youtube_data']
    youtubedb=mongo_db['channel_details']
    for com_data in youtubedb.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(com_data["Comment_information"])):
            com_list.append(com_data["Comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                comment_author_name,
                                                comment_text,
                                                comment_published_date
                                                )
                                                
                                                values(%s,%s,%s,%s,%s)'''
            values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['comment_author_name'],
                    row['comment_text'],
                    row['comment_published_date'])
            cursor.execute(insert_query,values)
            apidb.commit()
# Final function to create all tables
def finaltable():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    return "Successfully created"

# Streamlit UI
st.title("YouTube Data Harvesting")
st.write("Unveil the essence of your favorite channel with just a Channel ID - where data meets fascination!")

# Display existing channel IDs
st.sidebar.header("Some Channel IDs")
existing_channel_df = pd.DataFrame({
    'Channel Name': ['Captain Nick', 'Mostly Sane', 'Doja Cat', 'Taylor Swift', 'OK Tested', 'Girliyapa', 'FilterCopy', 'ScoopWhoop', 'Only Desi', 'Yogi baba'],
    'Channel ID': ['UCy6El3LYdeAzdHbYs4S_1kg', 'UCvCyIiKSCA1fHKSCOKJyjXA', 'UCzpl23pGTHVYqvKsgY0A-_w', 'UCqECaJ8Gagnn7YCbPEzWH6g', 'UC7lmZqhJeTzeQQkqNvfmjqw', 'UCdxbhKxr8pyWTx1ExCSmJRw', 'UC7IMq6LHbptAnSucW1pClA', 'UCx2HcmpB-UZGkMXOCJ4QIVA', 'UCCC4-ZHzMHUKNyDENY7Pk6Q', 'UCadPKrZKWAuyA6klaA3zp2w']
})
st.sidebar.table(existing_channel_df)

# Retrieve YouTube Data
st.subheader("Retrieve YouTube Data")
channel_id = st.text_input("Enter YouTube Channel ID:")

if st.button("Store data"):
    ch_ids=[]
    mongo_db=mongo_client['youtube_data']
    youtubedb=mongo_db['channel_details']
    for ch_data in youtubedb.find({},{"_id":0, "Channel_information":1}):
        ch_ids.append(ch_data["Channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.success("Channel data already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

# Migrate data to SQL
if st.button("Migrate Data to SQL"):
    migration_result = finaltable()
    st.success(migration_result)
   
# Show final table
def show_channels():
    ch_list=[]
    mongo_db=mongo_client['youtube_data']
    youtubedb=mongo_db["channel_details"]
    for ch_data in youtubedb.find({},{"_id":0,"Channel_information":1}):
        ch_list.append(ch_data["Channel_information"])
    df=st.dataframe(ch_list)
    return df

if st.button("Show Final Table"):
    st.subheader("Retrieved Data")
    show_channels()

# Streamlit UI - SQL Queries
st.header("Data Analysis")

# Checkbox for SQL queries
query_checkboxes = {
    "1)What are the names of all the videos and their corresponding channels?": "SELECT video_name, Channel_Name FROM videos",
    "2)Which channels have the most number of videos, and how many videos do they have?": "SELECT Channel_Name, COUNT(*) as Video_Count FROM videos GROUP BY Channel_Name ORDER BY Video_Count DESC LIMIT 3",
    "3)What are the top 10 most viewed videos and their respective channels?": "SELECT video_name, Channel_Name, view_count FROM videos ORDER BY view_count DESC LIMIT 10",
    "4)How many comments were made on each video, and what are their corresponding video names?": "SELECT video_name, comment_count FROM videos ORDER BY comment_count DESC",
    "5)Which videos have the highest number of likes, and what are their corresponding channel names?": "SELECT video_name, Channel_Name, COALESCE(like_count, 0) as like_count FROM videos ORDER BY like_count DESC LIMIT 1",
    "6)What is the total number of likes for each video, and what are their corresponding video names?": "SELECT video_name, Channel_Name, like_count FROM videos",
    "7)What is the total number of views for each channel, and what are their corresponding channel names?": "SELECT Channel_Name, SUM(view_count) as Total_Views FROM videos GROUP BY Channel_Name",
    "8)What are the names of all the channels that have published videos in the year 2022?": "SELECT DISTINCT Channel_Name FROM videos WHERE EXTRACT(YEAR FROM published_date) = 2022",
    "9)What is the average duration of all videos in each channel, and what are their corresponding channel names?": "SELECT Channel_Name, AVG(EXTRACT(EPOCH FROM duration)) as Avg_Duration FROM videos GROUP BY Channel_Name",
    "10)Which videos have the highest number of comments, and what are their corresponding channel names?": "SELECT video_name, Channel_Name, comment_count FROM videos WHERE comment_count IS NOT NULL ORDER BY comment_count DESC LIMIT 10"
}

# Display checkbox for each query
selected_queries = [st.checkbox(question) for question in query_checkboxes.keys()]

# Run queries when the button is clicked
if st.button("Run Selected Queries"):
    st.subheader("Results of Selected Queries")

    for question, query, selected in zip(query_checkboxes.keys(), query_checkboxes.values(), selected_queries):
        if selected:
            st.write(f"Result of Query - {question}")
            query_result = pd.read_sql_query(query, con=apidb)
            st.table(query_result)
            st.write("\n")
