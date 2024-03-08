# Capstone-Project1
YouTube Data Harvesting and Warehousing

Project Description:

This project involves collecting data from YouTube using its API, storing it in a MongoDB database, and creating a table for each data type in a SQL database. The data will then be connected to Pandas for analysis. Finally, we will fetch the stored details from MongoDB and transfer them to SQL for further querying and analysis. This allows for efficient data management, retrieval, and analysis, facilitating insightful answers to questions derived from the collected YouTube data.

Requirements:

Python 3.x
MongoDB
PostgreSQL
Google Cloud Platform Account with YouTube Data API enabled

Step-by-Step Process

Step 1: Installation
Install required packages using pip.
1. pip install streamlit
2. pip install pymongo
3. pip install psycopg2
4. pip install pandas

Step 2: Declaration of API Key
Declare the API key obtained from Google Cloud Platform for accessing the YouTube Data API.

Step 3: Collecting Data from YouTube
Write functions to retrieve specific details from YouTube using its API.

Step 4: Storing Data in MongoDB
Store the collected data in a MongoDB database for efficient data storage.

Step 5: Creating SQL Tables
Create tables in a PostgreSQL database to organize and structure the collected data.

Step 6: Display the Tables
Displaying the Tables we created in SQL using st.dataframe().

Step 7: Streamlit Code
In this process, we create an Interactive Web Interface providing a user-friendly experience using Streamlit. It allows users to:

1. Input Channel ID: Users can input a YouTube channel ID in the provided text input field.
2. Collect and Store Data: Upon clicking the "Collect and Store data" button, the application checks if the provided channel ID already exists in the MongoDB database. If not, it collects data from YouTube and 3. 3. stores it in MongoDB. If the channel ID already exists, it displays a message indicating that the channel ID already exists.
4. Migrate Data to SQL: Users can migrate the collected data from MongoDB to a PostgreSQL database by clicking the "Migrate Data to SQL" button.
5. View Data: Users can select from different tables (channels, playlists, comments, or videos) to view the stored data in the SQL database.
6. Answer Questions: Users can select predefined questions from a dropdown menu. Upon selecting a question and clicking the "GET SOLUTION" button, the application executes the corresponding SQL query and displays the results.
