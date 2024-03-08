import googleapiclient.discovery 
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


api_service_name = "youtube"
api_version = "v3"

#  Assigning my API_KEY as a Variable

api_key = 'AIzaSyCD-5rbRHNATyxZ8uINyXAkPyTFVePpz5E'


# To Get Channel Details

# To get channel Id/name as a input we using below function
def get_channel_info(Channel_ID):

    youtube = googleapiclient.discovery.build(
              api_service_name, api_version, developerKey = api_key) # here we calling the variable we assigned above
    request = youtube.channels().list(
              part="snippet,contentDetails,statistics",
              id=Channel_ID
        ) # Here we selecting the part of the list we getting the reqiured data
    response = request.execute()

    # Using For loop function to get required info as a dictionary format which will help to copy the file easily to Mongo db.

    for i in response['items']:
        channel_info = dict(Channel_Name = i['snippet']['title'], 
                            Channel_ID = i['id'], 
                            Channel_Descrption = i['snippet']['description'],
                            Playlist_ID = i['contentDetails']['relatedPlaylists']['uploads'],
                            Total_ViewCount = i['statistics']['viewCount'], 
                            Total_Subscriber = i['statistics']['subscriberCount'],
                            Total_Videos_ASON = i['statistics']['videoCount'])
        #dict() -> new empty dictionary dict(mapping) -> new dictionary initialized from a mapping object's(key, value) pairs

    return channel_info


# To Get Video ID's

def get_video_ids(Channel_ID):

    youtube = googleapiclient.discovery.build(
              api_service_name, api_version, developerKey = api_key) # here we calling the variable we assigned above
    response = youtube.channels().list(id=Channel_ID,
                                        part="contentDetails").execute()

    Playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'] #Here we first getting the Playlist Id and then from that we taking video id's

    Next_Page = None
    
    video_ids=[] #here we creating an empty dictionary to get our video id's from below steps
    
    while True:

        response1 = youtube.playlistItems().list(
                                                part="snippet,contentDetails",
                                                playlistId = Playlist_ID,
                                                maxResults = 50, #here We are deciding how many results to print per page.
                                                pageToken = Next_Page #this will help to get all page details in the result 
                                                ).execute() 
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            Next_Page = response1.get('nextPageToken') #get function is to get the data if it's there or else it will print NULL
        
        if Next_Page is None:
            break #it will break the loop and return to the function

    return video_ids   


# To Get Video_Details with Video ID's

def get_video_details(video_Ids):
        
        Video_Details = [] # empty dict to get the video detail from below

        for video_id in video_Ids:
        
                youtube = googleapiclient.discovery.build(
                        api_service_name, api_version, developerKey = api_key)

                request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id = video_id
                )
                response = request.execute()

                for i in response["items"]:
                
                        info = dict(Channel_Name = i['snippet']['channelTitle'],
                                    Video_Id = i['id'], 
                                    Video_Name = i['snippet']['title'],
                                    Video_descrption = i['snippet'].get('description'),
                                    Published_date = i['snippet']['publishedAt'], 
                                    View_count = i['statistics'].get('viewCount'),
                                    Favorite_count = i['statistics']['favoriteCount'],
                                    Comment_count = i['statistics'].get('commentCount'),
                                    likes = i['statistics'].get('likeCount'),
                                    Duration_of_video = i['contentDetails']['duration'],
                                    ThumbNail = i['snippet']['thumbnails']['default']['url'],
                                    Caption_State = i['contentDetails']['caption']
                                    )
                        #dict() -> new empty dictionary dict(mapping) -> new dictionary initialized from a mapping object's(key, value) pairs

                Video_Details.append(info) # append is to add the above received values to the empty dict we created first
                
        return Video_Details


# To get Comment details

def get_comment_details(video_Ids): 

    Comment = []
    try: # try will run code for errors
        for video_id in video_Ids: 
    #here we passing the video ids list we get from above function as one by one and getting comment details for each specific video ids. 
                
            youtube = googleapiclient.discovery.build(
                api_service_name, api_version, developerKey = api_key)

            request = youtube.commentThreads().list(
                        part = "snippet,replies",
                        videoId = video_id,
                        maxResults = 100
                    )
            response = request.execute()

            for i in response["items"]:
                data = dict(Channel_Id = i['snippet']['topLevelComment']['snippet']['channelId'],
                            Comment_id = i['snippet']['topLevelComment']['id'],
                            Video_id = i['snippet']['topLevelComment']['snippet']['videoId'],
                            TopLevel_Comment = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_PublishedAt =i['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment.append(data)
    except: # except block will let you to handle the error
            pass
            
    return Comment


#To get Playlist Details

def get_Playlist_details(Channel_ID):
        
        Playlist = []
        
        youtube = googleapiclient.discovery.build(
                api_service_name, api_version, developerKey = api_key)

        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=Channel_ID,
                maxResults=100
        )
        response = request.execute()

        for i in response["items"]:
                        data = dict(Playlist_Id = i['id'],
                                Channel_id = i['snippet']['channelId'],
                                Channel_Name = i['snippet']['channelTitle'],
                                Playlist_Name = i['snippet']['title'],
                                Playlist_created = i['snippet']['publishedAt'],
                                TotalNo_Videos_Playlist = i['contentDetails']['itemCount'])
                        Playlist.append(data)
        return Playlist


# Here we transfering our data to MongoDB (Data Lake)

myclient = pymongo.MongoClient("mongodb+srv://MGRajananthini:BNandy0330@mgrajananthini.dpgpuem.mongodb.net/?retryWrites=true&w=majority&appName=MGRajananthini")

db = myclient["YouTube_Data_Harvesting"] #creating database in MongoDB


def channel_details(Channel_ID): # with this function we storing all data collected from youtube to MOngodb datalake.

  ch_details =  get_channel_info(Channel_ID)
  vd_ids =  get_video_ids(Channel_ID)
  vd_details = get_video_details(vd_ids)
  pl_details =  get_Playlist_details(Channel_ID)
  co_details =  get_comment_details(vd_ids)

  mycoll = db["channel_details"] #here we inserting each channel information as a collection of data
  mycoll.insert_one({"Channel_Info" : ch_details , 
            "Video_Info" : vd_details,
            "Playlist_info" : pl_details,
            "Comment_info" : co_details})
  
  return "Upload successfully"


def Table_channels(): # creating tables for the collected data in SQL and getting that from MongoDB

    Data_Base = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "BNandy30",
                            database = "YouTube_DataHarvesting",
                            port = "5432",) # it is the connection to run SQL in backend

    cursor = Data_Base.cursor() #Cursor is a Temporary Memory or Temporary Work Station.

    drop_query = '''drop table if exists channels''' # it will help to get all channels details without error
    cursor.execute(drop_query)
    Data_Base.commit()

    Query = '''create table if not exists channels(Channel_Name varchar(100),  
                                                    Channel_ID varchar(80) primary key,
                                                    Channel_Descrption text,
                                                    Playlist_ID varchar(80),
                                                    Total_ViewCount bigint,
                                                    Total_Subscriber bigint,
                                                    Total_Videos_ASON int)''' #creating channel table in SQL IF NOt exists will not allow to create duplicate table
    
    cursor.execute(Query)
    Data_Base.commit()
  
    # Getting Data from Mongo DB

    channel_list = []
    db = myclient["YouTube_Data_Harvesting"]
    mycoll = db["channel_details"]
    for ch_data in mycoll.find({},{"_id" : 0 , "Channel_Info" : 1}):
        channel_list.append(ch_data["Channel_Info"])
    df = pd.DataFrame(channel_list) # here we getting data from mongoDB to SQL as dataframe using pandas

    for index,row in df.iterrows(): #iterrows() iterate the rows of a DataFrame and access the index and the row data.
        insert_Query = '''insert into channels(Channel_Name,
                                            Channel_ID,
                                            Channel_Descrption,
                                            Playlist_ID,
                                            Total_ViewCount,
                                            Total_Subscriber,
                                            Total_Videos_ASON) 
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)''' 
        # here we inserting the data we collected from MongoDB as Data frame to the table we created above.
        values = (row['Channel_Name'],
                 row['Channel_ID'],
                 row['Channel_Descrption'],
                 row['Playlist_ID'],
                 row['Total_ViewCount'],
                 row['Total_Subscriber'],
                 row['Total_Videos_ASON'])
      
        cursor.execute(insert_Query,values)
        Data_Base.commit()


def Table_playlist():

    Data_Base = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "BNandy30",
                            database = "YouTube_DataHarvesting",
                            port = "5432",)

    cursor = Data_Base.cursor()

    drop_query = '''drop table if exists playlist'''
    cursor.execute(drop_query)
    Data_Base.commit()

    Query = '''create table if not exists playlist(Playlist_Id varchar(80) primary key,
                                                    Channel_Name varchar(100),
                                                    Channel_Id varchar(80),
                                                    Playlist_Name text,
                                                    Playlist_created timestamp,
                                                    TotalNo_Videos_Playlist int)'''
    cursor.execute(Query)
    Data_Base.commit()
   

    # Getting Data from Mongo DB

    playlist_list = []
    db = myclient["YouTube_Data_Harvesting"]
    mycoll = db["channel_details"]
    for pl_data in mycoll.find({},{"_id" : 0 , "Playlist_info" :1}):
        for i in range(len(pl_data["Playlist_info"])): # range(len) will help to get all the detail in the playlist.
           playlist_list.append(pl_data["Playlist_info"][i])
        df1 = pd.DataFrame(playlist_list)

    for index,row in df1.iterrows():
        insert_Query = '''insert into playlist(Playlist_Id,
                                                Channel_id,
                                                Channel_Name,
                                                Playlist_Name,
                                                Playlist_created,
                                                TotalNo_Videos_Playlist)
                                            
                                                 values(%s,%s,%s,%s,%s,%s)'''
        values = (row['Playlist_Id'],
                row['Channel_id'],
                row['Channel_Name'],
                row['Playlist_Name'],
                row['Playlist_created'],
                row['TotalNo_Videos_Playlist'])
        
        cursor.execute(insert_Query,values)
        Data_Base.commit()


def Table_Comment():

    Data_Base = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "BNandy30",
                            database = "YouTube_DataHarvesting",
                            port = "5432",)

    cursor = Data_Base.cursor()

    drop_query = '''drop table if exists Comment'''
    cursor.execute(drop_query)
    Data_Base.commit()

    Query = '''create table if not exists Comment(Channel_Id varchar(80),
                                                   Comment_id varchar(80) primary key,
                                                   Video_id varchar(50),
                                                   TopLevel_Comment text,
                                                   Comment_author varchar(80),
                                                   Comment_PublishedAt timestamp)'''
    cursor.execute(Query)
    Data_Base.commit()

    # Getting Data from Mongo DB

    comment_list = []
    db = myclient["YouTube_Data_Harvesting"]
    mycoll = db["channel_details"]
    for co_data in mycoll.find({},{"_id" : 0 , "Comment_info" :1}):
        for i in range(len(co_data["Comment_info"])):   
           comment_list.append(co_data["Comment_info"][i])
        df2 = pd.DataFrame(comment_list)

    for index,row in df2.iterrows():
        insert_query = '''insert into comment(Channel_Id,
                                               Video_id,
                                               Comment_id,
                                               TopLevel_Comment,
                                               Comment_author,
                                               Comment_PublishedAt)
                                            
                                               values(%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Id'],
                  row['Video_id'],
                  row['Comment_id'],
                  row['TopLevel_Comment'],
                  row['Comment_author'],
                  row['Comment_PublishedAt'])
        
        cursor.execute(insert_query,values)
        Data_Base.commit()


def Table_Videos():

    Data_Base = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "BNandy30",
                            database = "YouTube_DataHarvesting",
                            port = "5432",)

    cursor = Data_Base.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    Data_Base.commit()

    Query = '''create table if not exists videos(Channel_Name varchar(100),
                                                Video_Id varchar(50) primary key,
                                                Video_Name varchar(100),
                                                Video_descrption text,
                                                Published_date timestamp,
                                                View_count bigint,
                                                Favorite_count int,
                                                Comment_count int,
                                                Likes bigint,
                                                Duration_of_video interval,
                                                ThumbNail varchar(200),
                                                Caption_State varchar(10))'''
    cursor.execute(Query)
    Data_Base.commit()

    # Getting Data from Mongo DB

    video_list = []
    db = myclient["YouTube_Data_Harvesting"]
    mycoll = db["channel_details"]
    for vd_data in mycoll.find({},{"_id" : 0 , "Video_Info" :1}):
        for i in range(len(vd_data["Video_Info"])):   
           video_list.append(vd_data["Video_Info"][i])

        df3 = pd.DataFrame(video_list)

    for index,row in df3.iterrows():
        insert_query = '''insert into videos(Channel_Name,
                                             Video_Id,
                                             Video_Name,
                                             Video_descrption,
                                             Published_date,
                                             View_count,
                                             Favorite_count,
                                             Comment_count,
                                             likes,
                                             Duration_of_video,
                                             ThumbNail,
                                             Caption_State)
                                            
                                             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                  row['Video_Id'],
                  row['Video_Name'],
                  row['Video_descrption'],
                  row['Published_date'],
                  row['View_count'],
                  row['Favorite_count'],
                  row['Comment_count'],
                  row['likes'],
                  row['Duration_of_video'],
                  row['ThumbNail'],
                  row['Caption_State'])
        
        cursor.execute(insert_query,values)
        Data_Base.commit()


def Tables():

    Table_channels()
    Table_playlist()
    Table_Comment()
    Table_Videos()
    
    return "All Four Tables Created Successfully in SQL"


def display_channels_tables():
    
    channel_list = []
    db = myclient["YouTube_Data_Harvesting"]
    mycoll = db["channel_details"]
    for ch_data in mycoll.find({},{"_id" : 0 , "Channel_Info" : 1}):
        channel_list.append(ch_data["Channel_Info"])

    st.dataframe(channel_list)

    return 

def display_playlist_tables():  
     
    playlist_list = []
    db = myclient["YouTube_Data_Harvesting"]
    mycoll = db["channel_details"]
    for pl_data in mycoll.find({},{"_id" : 0 , "Playlist_info" :1}):
        for Playlist_info in pl_data["Playlist_info"]:   
    #here we iterating playlist_info with pl_data['Playlist_info']. 
    #so, each time the item in the playlist info will get every data in the playlist info by repeating the process 
            playlist_list.append(Playlist_info)

    st.dataframe(playlist_list)

    return 

def display_comment_tables():

    comment_list = []
    db = myclient["YouTube_Data_Harvesting"]
    mycoll = db["channel_details"]
    for co_data in mycoll.find({},{"_id" : 0 , "Comment_info" :1}):
        for Comment_info in co_data["Comment_info"]:  
           comment_list.append(Comment_info)

    st.dataframe(comment_list)
    
    return


def display_videos_tables():

    video_list = []
    db = myclient["YouTube_Data_Harvesting"]
    mycoll = db["channel_details"]
    for vd_data in mycoll.find({},{"_id" : 0 , "Video_Info" :1}):
        for Video_Info in vd_data["Video_Info"]:   
           video_list.append(Video_Info)

    st.dataframe(video_list)

    return 

# To create web application using streamlit 

with st.sidebar:
     st.title(':gray[YOUTUBE DATA HARVESTING AND WAREHOUSING] :admission_tickets:')
     st.header('Skill Take Away', divider = 'gray')
     st.subheader(':ballot_box_with_check: API Integration')
     st.subheader(':ballot_box_with_check: Python Scripting')
     st.subheader(':ballot_box_with_check: Data Collection')
     st.subheader(':ballot_box_with_check: MongoDB')
     st.subheader(':ballot_box_with_check: Data Management Using MongoDB , Pandas and SQL')

channel_id = st.text_input('Enter the Channel ID') 
#here to get channel id as input and storing it in MongoDB,Below function is to check whether the channel id already exists or not
#if exists it won't allow data to insert in Mongo Db or else it will Upload Successfully.

if st.button('Collect and Store data'):
     channel_ids = []
     db = myclient["YouTube_Data_Harvesting"]
     mycoll = db["channel_details"]
     for ch_data in mycoll.find({},{"_id" : 0, "Channel_Info": 1}):
       channel_ids.append(ch_data["Channel_Info"]["Channel_ID"])
     
     if channel_id in channel_ids:
         st.success("Given channel ID already exists")
     
     else:
         insert = channel_details(channel_id) 
         st.success(insert)

if st.button('Migrate Data to SQL'): #here we migrating the data that we stored in MongoDB to SQL
    tables = Tables()
    st.success(tables)

display_tables = st.radio("SELECT THE TABLE TO VIEW",("CHANNELS","PLAYLIST","COMMENTS","VIDEOS"))
#This will display the tables we created in SQL with all details

if display_tables == "CHANNELS": 
    display_channels_tables()

elif display_tables == "PLAYLIST":
    display_playlist_tables()

elif display_tables == "COMMENTS":
    display_comment_tables()

elif display_tables == "VIDEOS":
    display_videos_tables()


# Connecting to SQl to answer the given Questions

Data_Base = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = "BNandy30",
                        database = "YouTube_DataHarvesting",
                        port = "5432",)
cursor = Data_Base.cursor()

Question = st.selectbox("Select your Question",("1.What are the names of all the videos and their corresponding channels?",
                                                "2.Which channels have the most number of videos, and how many videos dothey have?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

# here we writing an SQL Query to answer above all Question.
# In this we are writing an SQL Query and then converting it to dataframe and using st.write we displaying it in streamlit.
if Question == "1.What are the names of all the videos and their corresponding channels?" :
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Channel_Name, Video_Name, Video_Id FROM Videos'''
        cursor.execute(Answer)
        Q1 = cursor.fetchall()
        data_frame = pd.DataFrame(Q1,columns=['Channel_Name', 'Video_Name', 'Video_Id'])
        st.write(data_frame)
        st.success("Here are all the videos with their corresponding channels")

elif Question == "2.Which channels have the most number of videos, and how many videos dothey have?" :
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Channel_Name, Total_Videos_ASON FROM Channels ORDER BY Total_Videos_ASON DESC'''
        cursor.execute(Answer)
        Q2 = cursor.fetchall()
        data_frame = pd.DataFrame(Q2,columns=['Channel_Name', 'Total_Videos_ASON'])
        st.write(data_frame)
        st.success("The channels with the most number of videos are")

elif Question == "3.What are the top 10 most viewed videos and their respective channels?" :
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Videos.Channel_Name, Videos.Video_Name, Videos.View_count FROM Videos INNER JOIN 
        Channels on Videos.Channel_Name = Channels.Channel_Name ORDER BY View_count DESC LIMIT 10'''
        cursor.execute(Answer)
        Q3 = cursor.fetchall()
        data_frame = pd.DataFrame(Q3,columns=['Channel_Name', 'Video_Name', 'View_count'])
        st.write(data_frame)
        st.success("The 10 top most viewed videos from Each channel")

elif Question == "4.How many comments were made on each video, and what are their corresponding video names?":
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Channel_Name, Video_Name, Comment_count FROM Videos WHERE Comment_count is NOT NULL'''
        cursor.execute(Answer)
        Q4 = cursor.fetchall()
        data_frame = pd.DataFrame(Q4,columns=['Channel_Name', 'Video_Name', 'Comment_count'])
        st.write(data_frame)
        st.success("Total number of comment made under each Videos with respective channel Name")

elif Question == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Videos.Channel_Name, Videos.Video_Name, Videos.likes FROM Videos INNER JOIN 
        Channels on Videos.Channel_Name = Channels.Channel_Name Where likes is NOT NULL ORDER BY likes DESC'''
        cursor.execute(Answer)
        Q5 = cursor.fetchall()
        data_frame = pd.DataFrame(Q5,columns=['Channel_Name', 'Video_Name', 'likes'])
        st.write(data_frame)
        st.success("The videos with the highest number of likes, along with their respective channel names, are as above")

elif Question == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Channel_Name, Video_Name, likes FROM Videos'''
        cursor.execute(Answer)
        Q6 = cursor.fetchall()
        data_frame = pd.DataFrame(Q6,columns=['Channel_Name', 'Video_Name', 'likes'])
        st.write(data_frame)
        st.success("The total number of likes for each video, along with their respective channel names, is provided. However, the dislike count has been made private, as the 'statistics.dislikeCount' property was made private as of December 13, 2021.")

elif Question == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Channel_Name, Total_ViewCount FROM Channels'''
        cursor.execute(Answer)
        Q7 = cursor.fetchall()
        data_frame = pd.DataFrame(Q7,columns=['Channel_Name', 'Total_Viewcount'])
        st.write(data_frame)
        st.success("The Total View count of each channels.")

elif Question == "8.What are the names of all the channels that have published videos in the year 2022?":
    if st.button("GET SOLUTION"):
        Answer = '''SELECT DISTINCT Channel_Name, Video_Name, Published_date FROM Videos WHERE extract (year from Published_date) = 2022 '''
        cursor.execute(Answer)
        Q8 = cursor.fetchall()
        data_frame = pd.DataFrame(Q8,columns=['Channel_Name', 'Video_Name','Published_date'])
        st.write(data_frame)
        st.success("The Videos Published at the Year of 2022 with respective channel name")

elif Question == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Channel_Name, Video_Name, AVG(Duration_of_video) as Averageduration FROM Videos GROUP BY Channel_Name, Video_Name;'''
        cursor.execute(Answer)
        Q9 = cursor.fetchall()
        data_frame = pd.DataFrame(Q9,columns=['Channel_Name', 'Video_Name','Averageduration'])
        st.write(data_frame)
        st.success("The average duration of all videos in each channel")

elif Question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    if st.button("GET SOLUTION"):
        Answer = '''SELECT Channel_Name, Video_Name, Comment_count FROM Videos Where Comment_count is NOT NULL 
        order by Comment_count DESC'''
        cursor.execute(Answer)
        Q10 = cursor.fetchall()
        data_frame = pd.DataFrame(Q10,columns=['Channel_Name', 'Video_Name','Comment_count'])
        st.write(data_frame)
        st.success("The highest comment made under each channel")
