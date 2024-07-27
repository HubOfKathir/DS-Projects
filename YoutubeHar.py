import googleapiclient.discovery
import streamlit as st
import mysql.connector
import pandas as pd
import re
from sqlalchemy import create_engine
from PIL import Image
from streamlit_option_menu import option_menu
from googleapiclient.errors import HttpError

api_service_name = "youtube"
api_version = "v3"
api_key = 'AIzaSyBshQ4wxr3u6fc0lfvN1o6KRYzvK10vZjk'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#mycurser and engine created to intreact with MYSQL Database
mydb = mysql.connector.connect(host="localhost",user="root",password="")
mycursor = mydb.cursor(buffered=True)
engine = create_engine("mysql+mysqlconnector://root:@localhost/youtubedata")

#to create and use the database in MYSQL database 
mycursor.execute('create database if not exists youtubedata')
mycursor.execute('use youtubedata')

icon = Image.open("youtubeicon.png")
st.set_page_config(page_title=None, page_icon=icon, 
                   layout="wide", initial_sidebar_state="expanded",
                menu_items={"About":"This is Test app created fo my knowledge",
                            })
st.title("YouTube Data Harvesting and Warehousing")
st.subheader('', divider='rainbow')
#sl.subheader(":red[wait] :sunglasses:")

#setting up streamlit sidebar menu with optins
with st.sidebar:
    selected =option_menu("Main Menu",
                        ["Home","Data collection and upload","MYSQL Database","Analysis using SQL"],
                        icons=["house","cloud-upload","database", "filetype-sql", "bar-chart-line"],
                        menu_icon="menu-up",
                        orientation="vertical")
    # Setting up the option "Home" in streamlit page

if selected == "Home":

    st.title("Let's take a look down below")
    st.subheader(':blue[Domain I used:] Youtube','19px')
    st.subheader(':blue[Overview for the domain :]')
    st.markdown('This particular :red[YouTube] Data Harvesting Project is designed to extract, analyze, and visualize data from YouTube channels and videos')
    st.subheader(':blue[Skill Take Away :]')
    st.write(''' Python scripting.
                \nData Collection.
                \nAPI(:red[Youtube]) integration.
                \nData Management using SQL and Streamlit.''')
    

# Function to Retrieve channel information from Youtube    
def channel_information(channel_id):
        request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
        response = request.execute()

        for i in response['items']:
            channel_data= dict(
                channel_name=i['snippet']['title'],
                Channel_id=i["id"],
                channel_Description=i['snippet']['description'],
                channel_Thumbnail=i['snippet']['thumbnails']['default']['url'],
                channel_playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
                channel_subscribers=i['statistics']['subscriberCount'],
                channel_video_count=i['statistics']['videoCount'],
                channel_views=i['statistics']['viewCount'],
                channel_publishedat=i['snippet']['publishedAt'])
        return (channel_data)
    
    # Function to Retrieve playlist information of channel from Youtube
def playlist_information(channel_id):
    playlist_info=[]
    nextPageToken=None
    try:
        while True:
            request = youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=nextPageToken
                    )
            response = request.execute()
        
            for i in response['items']:
                data=dict(
                    playlist_id=i['id'],
                    playlist_name=i['snippet']['title'],
                    publishedat=i['snippet']['publishedAt'],
                    channel_ID=i['snippet']['channelId'],
                    channel_name=i['snippet']['channelTitle'],
                    videoscount=i['contentDetails']['itemCount'])
                playlist_info.append(data)
                nextPageToken=response.get('nextPageToken')
            if nextPageToken is None:
                break
    except HttpError as e:
        error_message = f"Error retrieving playlists: {e}"   # Handles YouTube API errors
        st.error(error_message)
    return (playlist_info)

#Function to Retrieve video ids of a channel from Youtube
def get_video_ids(channel_id):
    response= youtube.channels().list( part="contentDetails",
                                        id=channel_id).execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token=None
    
    videos_ids=[]
    
    while True:
        response1=youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        
        for i in range (len(response1['items'])):
            videos_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return (videos_ids)

#Function to Retrieve video information of all video IDS from Youtube
def video_information(video_ids):
    video_info=[]
    for video_id in video_ids:
        response= youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_id).execute()
        
        for i in response['items']:
                data=dict(
                        channel_id=i['snippet']['channelId'],
                        video_id=i['id'],
                        video_name=i['snippet']['title'],
                        video_Description=i['snippet']['description'],
                        Thumbnail=i['snippet']['thumbnails']['default']['url'],
                        Tags=i['snippet'].get('tags'),
                        publishedAt=i['snippet']['publishedAt'],
                        Duration=convert_duration(i['contentDetails']['duration']),
                        View_Count=i['statistics']['viewCount'],
                        Like_Count=i['statistics'].get('likeCount'),
                        Favorite_Count=i['statistics'].get('favoriteCount'),
                        Comment_Count=i['statistics']['commentCount'],
                        Caption_Status=i['contentDetails']['caption'] 
                        )
                video_info.append(data)
    return(video_info)

#Function to Retrieve comments information of all video IDS from Youtube
def comments_information(video_ids):
    comments_info=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100)
            response = request.execute()

            for i in response.get('items',[]):
                data=dict(
                            video_id=i['snippet']['videoId'],
                            comment_id=i['snippet']['topLevelComment']['id'],
                            comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_publishedat=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                comments_info.append(data)
    except HttpError as e:
        if e.resp.status == 403 and e.error_details[0]["reason"] == 'commentsDisabled':
                st.error("comments diabled for some videos")
    return (comments_info)

#Function to convert Duration from ISO 8601 format to HH:MM:SS format(parsesa duration in format used by Youtube API)
def convert_duration(duration): 
        regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
        match = re.match(regex, duration)
        if not match:
                return '00:00:00'
        hours, minutes, seconds = match.groups()
        hours = int(hours[:-1]) if hours else 0
        minutes = int(minutes[:-1]) if minutes else 0
        seconds = int(seconds[:-1]) if seconds else 0
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))

##setting up the option "Data collection and upload" in streamlit page
if selected == "Data collection and upload":
    st.subheader(':blue[Data collection and upload]')
    st.markdown('''
                - Provide channel ID in the input field.
                - Clicking the 'View Details' button will display an overview of youtube channel.
                - Clicking 'Upload to MYSQL database' will store the extracted channel information,
                Playlists,Videos,Comments in MYSQL Database''')
    
    channel_ID = st.text_input("**Enter the channel ID in the below box :**")

    if st.button("View details"): # Shows the channel information from Youtube
        with st.spinner('Extraction in progress...'):
            try:
                extracted_details = channel_information(channel_id=channel_ID)
                st.write('**:blue[Channel Thumbnail]** :')
                st.image(extracted_details.get('channel_Thumbnail'))
                st.write('**:blue[Channel Name]** :', extracted_details['channel_name'])
                st.write('**:blue[Description]** :', extracted_details['channel_Description'])
                st.write('**:blue[Total_Videos]** :', extracted_details['channel_video_count'])
                st.write('**:blue[Subscriber Count]** :', extracted_details['channel_subscribers'])
                st.write('**:blue[Total Views]** :', extracted_details['channel_views'])
            except:
                st.error("Please fill the channelID in textfield")

    if st.button("Upload to my sql database"):
        with st.spinner('Upload in progress...'):
            try:  
               #to create a channel table in sql database
               mycursor.execute('''create table if not exists channel(channel_name VARCHAR(100),
                                channel_id VARCHAR(100) PRIMARY KEY, channel_description VARCHAR(1000),channel_Thumbnail VARCHAR(100),
                                channel_playlist_id VARCHAR(50),channel_subscribers BIGINT,channel_video_count BIGINT,channel_views BIGINT,
                                channel_publishedat DATETIME)''')
               
               #to create a playlist table in sql database
               mycursor.execute('''create table if not exists playlist(playlist_id VARCHAR(50) PRIMARY KEY,playlist_name VARCHAR(100),
                                publishedat DATETIME,channel_id VARCHAR(50),channel_name VARCHAR(100),videoscount BIGINT)''')
               
               #to create videos table in sql database
               mycursor.execute('''create table if not exists videos(channel_id VARCHAR(100),video_id VARCHAR(50) PRIMARY KEY,
                                 video_name VARCHAR(100),video_Description VARCHAR(500),Thumbnail VARCHAR(100),Tags VARCHAR(250),
                                publishedAt DATETIME,Duration VARCHAR(10),View_count BIGINT,Like_Count BIGINT,Favorite_Count BIGINT,
                                Comment_Count BIGINT,Caption_Status VARCHAR(10),FOREIGN KEY (channel_id) REFERENCES channel(channel_id))''')
               
               #to create comments table in sql database
               mycursor.execute('''create table if not exists comments(video_id VARCHAR(50),comment_id VARCHAR(50),comment_text TEXT,
                                comment_author VARCHAR(50),comment_publishedat DATETIME,FOREIGN KEY (video_id) REFERENCES Videos(video_id))''')
               
               
               #Transform corresponding data's into pandas dataframe
               df_channel=pd.DataFrame(channel_information(channel_id=channel_ID),index=[0])
               df_playlist=pd.DataFrame(playlist_information(channel_id=channel_ID))
               df_videos=pd.DataFrame(video_information(video_ids= get_video_ids(channel_id=channel_ID)))
               df_comments=pd.DataFrame(comments_information(video_ids=get_video_ids(channel_id=channel_ID)))

               #Load DF into SQL database
               df_channel.to_sql('channel',engine,if_exists='append',index = False)
               df_playlist.to_sql('playlist',engine,if_exists='append',index=False)
               df_videos['Tags'] = df_videos['Tags'].apply(lambda x: ','.join(x) if isinstance(x,list)else '')
               df_videos.to_sql('videos',engine,if_exists='append',index=False)
               df_comments.to_sql('comments',engine,if_exists='append',index=False)
               mydb.commit()
               st.success('Datas are successfully uploaded')
            except:
                st.error("Don't ask for the channel that already exists!")
# Function to retrieve channel name from SQL DB
def fetch_channel_names():
  mycursor.execute("SELECT channel_name FROM channel")
  channel_names = [row[0] for row in mycursor.fetchall()]
  return channel_names

#Functiion to fetch all database from SQL :-
def load_channel_data(channel_name):
# Fetch channel data
    mycursor.execute('SELECT * From channel WHERE channel_name = %s',(channel_name,))
    out = mycursor.fetchall()
    channel_df = pd.DataFrame(out,columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    channel_df.index +=1

# Fetch playlists data
    mycursor.execute("SELECT * FROM playlist WHERE channel_id = %s", (channel_df['channel_id'].iloc[0],))
    out = mycursor.fetchall()
    playlists_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    playlists_df.index +=1

# Fetch videos data
    mycursor.execute("SELECT * FROM videos WHERE channel_id = %s", (channel_df['channel_id'].iloc[0],))
    out= mycursor.fetchall()
    videos_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    videos_df.index +=1

# Fetch comments data
    mycursor.execute("SELECT * FROM comments WHERE video_id IN (SELECT video_id FROM videos WHERE channel_id = %s)",
                     (channel_df['channel_id'].iloc[0],))
    out = mycursor.fetchall()
    comments_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    comments_df.index +=1

    return channel_df, playlists_df, videos_df, comments_df

# Setting up the option "MYSQL Database" in streamlit page 
if selected == "MYSQL Database":
    st.subheader(':red[MYSQL Database]')
    st.markdown('''__You can view the channel details along with all the details in :blue[table] format ''')

    try:
        channel_names = fetch_channel_names()
        selected_channel = st.selectbox(':red[Select Channel]', channel_names)
    
        if selected_channel:
                channel_info,playlist_info,videos_info,comments_info = load_channel_data(selected_channel)
        
        st.subheader(':green[Channel Table]')
        st.write(channel_info)
        st.subheader(':green[Playlists Table]')
        st.write(playlist_info)
        st.subheader(':green[Videos Table]')
        st.write(videos_info)
        st.subheader(':green[Comments Table]')
        st.write(comments_info)
    except:
        st.error('Database is empty ')

#SQL Query Output need to displayed as table in Streamlit Application:

#1.What are the names of all the videos and their corresponding channels?
def Sql_Q1():
    mycursor.execute('''SELECT channel.channel_name,videos.video_name FROM videos
                    JOIN channel ON channel.channel_id = videos.channel_id
                    ORDER BY channel_name''')
    out = mycursor.fetchall()
    Q1 = pd.DataFrame(out, columns = ['Channel Name','Videos_name']).reset_index(drop = True)
    Q1.index +=1
    st.dataframe(Q1)

#2.Which channels have the most number of videos, and how many videos do they have?
def Sql_Q2():
    mycursor.execute('''SELECT DISTINCT channel_name, COUNT(videos.video_id) as Total_videos
                     FROM channel
                     JOIN videos ON channel.channel_id = videos.channel_id
                     GROUP BY channel_name
                     ORDER BY Total_videos DESC''')
    out=mycursor.fetchall()
    Q2= pd.DataFrame(out, columns=['Channel Name','Total Videos']).reset_index(drop=True)
    Q2.index +=1
    st.dataframe(Q2) 

#3.What are the top 10 most viewed videos and their respective channels?
def Sql_Q3():
    mycursor.execute('''SELECT channel.Channel_name,videos.Video_name, videos.View_count as Total_views
                     FROM videos
                     JOIN channel ON channel.Channel_id = videos.Channel_id
                     ORDER BY videos.View_count DESC
                     LIMIT 10;''')
    
    out=mycursor.fetchall()
    Q3= pd.DataFrame(out, columns=['Channel Name','Total Views','Videos Name']).reset_index(drop=True)
    Q3.index +=1
    st.dataframe(Q3) 

#4.How many comments were made on each video, and what are their corresponding video names?
def Sql_Q4():
    mycursor.execute('''SELECT videos.video_name,videos.comment_count as Total_comments
                     FROM videos
                     ORDER BY videos.comment_count DESC ''')
    out = mycursor.fetchall()
    Q4 = pd.DataFrame(out,columns = ['Videos Name','Likes']).reset_index(drop = True)
    Q4.index +=1
    st.dataframe(Q4)

#Which videos have the highest number of likes, and what are their corresponding channel names?
def Sql_Q5():
    mycursor.execute('''SELECT channel.Channel_name,videos.video_name,videos.Like_Count as Likes_number
                     FROM videos
                     JOIN channel ON videos.channel_id = channel.channel_id
                     WHERE Like_count = (SELECT MAX(videos.Like_count) FROM videos v WHERE videos.channel_id=v.channel_id
                     GROUP BY channel_id)
                     ORDER BY Likes_number DESC''')
    out = mycursor.fetchall()
    Q5= pd.DataFrame(out, columns=['Channel Name','Videos Name','Likes']).reset_index(drop=True)
    Q5.index +=1
    st.dataframe(Q5)

#What is the total number of likes and dislikes for each video, and what are their corresponding video names?
def Sql_Q6():
     mycursor.execute('''SELECT videos.video_name,videos.Like_count as likes
                      FROM videos
                      ORDER BY videos.Like_count DESC''')

     out = mycursor.fetchall()
     Q6= pd.DataFrame(out, columns=['Videos Name','Likes']).reset_index(drop=True)
     Q6.index +=1
     st.dataframe(Q6)
    
#What is the total number of views for each channel, and what are their corresponding channel names?
def Sql_Q7():
    mycursor.execute('''SELECT channel.channel_name, channel.channel_views as total_views
                     FROM channel
                     ORDER BBY channel.channel_views DESC''')
    out = mycursor.execute()
    Q7 = pd.DataFrame(out, columns = ['Channel Name','Total views']).reset_index(drop=True)
    Q7.index +=1
    st.dataframe(Q7)

#What are the names of all the channels that have published videos in the year 2022?
def Sql_Q8():
    mycursor.execute('''SELECT DISTINCT channel.channel_name
                    FROM channel
                    JOIN videos ON  videos.channel_id=channel.channel_id
                    WHERE YEAR(videos.PublishedAt) = 2022 ''')
    out=mycursor.fetchall()
    Q8= pd.DataFrame(out, columns=['Channel Name']).reset_index(drop=True)
    Q8.index +=1
    st.dataframe(Q8)

#What is the average duration of all videos in each channel, and what are their corresponding channel names?
def Sql_Q9():
    mycursor.execute('''SELECT channel.channel_name,
                     TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(videos.Duration)))), "%H:%i:%s") AS Duration
                     FROM videos
                     JOIN channel ON videos.channel_id = channel.channel_id
                     GROUP BY channel_name''')
    out=mycursor.fetchall()
    Q9= pd.DataFrame(out, columns=['Chanel Name','Duration']).reset_index(drop=True)
    Q9.index +=1
    st.dataframe(Q9)

#Which videos have the highest number of comments, and what are their corresponding channel names?
def Sql_Q10():
    mycursor.execute('''SELECT channel.channel_name,videos.video_name,videos.comment_count as Total_Comments
                    FROM videos
                    JOIN channel ON channel.channel_id=videos.channel_id
                    ORDER BY videos.comment_count DESC''')
    out=mycursor.fetchall()
    Q10= pd.DataFrame(out, columns=['Channel Name','Videos Name','Comments']).reset_index(drop=True)
    Q10.index +=1
    st.dataframe(Q10)


#setting "Analysis using Sql" option:
if selected == 'Analysis using SQL':
    st.subheader(':blue[Analysis using SQL]')
    st.markdown('''You can analyze the collection of YouTube channel data stored in a MySQL database.
                Based on selecting the listed questions below, the output will be displayed in a table format''')
    Questions = ['Select your Question',
        '1.What are the names of all the videos and their corresponding channels?',
        '2.Which channels have the most number of videos, and how many videos do they have?',
        '3.What are the top 10 most viewed videos and their respective channels?',
        '4.How many comments were made on each video, and what are their corresponding video names?',
        '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7.What is the total number of views for each channel, and what are their corresponding channel names?',
        '8.What are the names of all the channels that have published videos in the year 2022?',
        '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10.Which videos have the highest number of comments, and what are their corresponding channel names?' ]
    Selected_Question = st.selectbox(' ',options=Questions)
    if Selected_Question =='1.What are the names of all the videos and their corresponding channels?':
        Sql_Q1()
    if Selected_Question =='2.Which channels have the most number of videos, and how many videos do they have?':
        Sql_Q2()
    if Selected_Question =='3.What are the top 10 most viewed videos and their respective channels?': 
        Sql_Q3()
    if Selected_Question =='4.How many comments were made on each video, and what are their corresponding video names?':
        Sql_Q4() 
    if Selected_Question =='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        Sql_Q5() 
    if Selected_Question =='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('**:red[Note]:- Dislike property was made private as of December 13, 2021.**')
        Sql_Q6()   
    if Selected_Question =='7.What is the total number of views for each channel, and what are their corresponding channel names?':
        Sql_Q7()
    if Selected_Question =='8.What are the names of all the channels that have published videos in the year 2022?':
        Sql_Q8()
    if Selected_Question =='9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        Sql_Q9()
    if Selected_Question =='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        Sql_Q10()