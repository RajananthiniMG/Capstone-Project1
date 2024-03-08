# Capstone-Project1
YouTube Data Harvesting and Warehousing

Project Description:

This project involves collecting data from YouTube using its API, storing it in a MongoDB database, and creating a table for each data type in a SQL database. The data will then be connected to Pandas for analysis. Finally, we will fetch the stored details from MongoDB and transfer them to SQL for further querying and analysis. This allows for efficient data management, retrieval, and analysis, facilitating insightful answers to questions derived from the collected YouTube data.

Requirements:

Python 3.x
MongoDB
PostgreSQL
Google Cloud Platform Account with YouTube Data API enabled

Step1: Installation:

we are installed and Imported some pacages that reqiured for this project as the first step of the Project.

1. pip install streamlit
2. pip install pymongo
3. pip install psycopg2
4. pip install pandas

Srep2: Declaration of API Key:

After installation we are declaring the API key that we are going to use in this project to get data from Youtube. The API key as been enabled from Google Cloud Platform Account

api_key = 'AIzaSyCD-5rbRHNATyxZ8uINyXAkPyTFVePpz5E'

Step3: To Collect Data from YouTube:

In this Step we writing the code to get the specifice detail from YouTube and creating each as a Function to call when we required in next Process,

def get_channel_info(Channel_ID):
def get_video_ids(Channel_ID):
def get_video_details(video_ids):
def get_Playlist_details(Channel_ID):
def get_comment_details(video_ids):

Step4: To Store the above collected data into MongoDB

In this Process we are using cloud MongoDB servet to create an database as YouTube_Data_Harvestin and inserting all the details we collected from youtube by calling the above defined function to the data lake of the the MongoDB as Channel_details.

Step5: To create an Table for each data we collected as Channels Table, Playlist Table, Comment Table and Video Table in SQL
