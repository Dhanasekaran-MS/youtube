# YouTube Data Harvesting and Warehousing

## Table of Contents:
1. About The Project
2. Getting Started
   - Prerequisites
   - Installation
4. Usage
5. Steps
6. Run
------------------

## 1. About The Project :

   This project is focused on harvesting data from YouTube channels using the YouTube API, processing the data, and warehousing it. The harvested data is initially stored in a MongoDB database as documents inside collections and is then transfered into SQL records for in-depth data analysis. The project's core functionality relies on the Extract, Transform, Load (ETL) process.

## 2. Getting Started :

> Before starting we need to install certain **python** libraries.
-  ### PREREQUISITES :
   + googleapiclient
   + pymongo
   + pymysql
   + streamlit
   + pandas - are the Python libraries used in this project
   * Get Youtube API key from [google developer console](https://developers.google.com/youtube/v3/getting-started)
- ### INSTALLATION :
  Run these command separately on your python environment to install.
  
        pip install google-api-python-client
        pip install pymongo
        pip install pymysql
        pip install streamlit
        pip install pandas
   
## 3. USAGE :
   - In this Project, Wrote a python program to get Youtube channel data using googleapiclient
   - Extracted the channel data by giving channel Id 
   - Ordered the required data from extracted data using python
   - Store ordered data to MongoDB database (DataLake) from python by using **pymongo** library
   - Creating Tables on MySQL database from python by using **pymysql** library
   - Transfer the Data from MongoDB to MySQL (DATA MIGRATION)
   - We can now show tables that were available in MySQL database
   - Ability to search and retrieve data from the SQL database using different search options (10 questions)
   - Finally created a Streamlit Web application interface used to Display our project

## 4. STEPS :
   1. Import all the libraries that are used in this project.
   2. Created a function which returns api client request and storing in into a variable "youtube".
   3. Cretaed 5 functions which returns channel,video,playlist and comments data from Youtube API.
   4. Established mongo client connection.
   5. Created a function 'data_to_mongo()' with argument "channel_id" which calls the above 5 functions and also stores the collected data into Mongo DB database.
   6. Created a function 'mongo_to_sql()' which upon calling will migrate the data from Mongo DB to MySQL also creates a database and table if it does not exists.
   7. Finally created a interactive web application using Streamlit and added buttons to it to make it work.
   8. Wrote SQL queries to fetch data from MySQL database by using read_sql_query() function from pandas to display it in streamlit

   > - TO AVOID DUPLICATE REPETATION OF CHANNEL WE USED 'PRIMARY KEY' WHILE CREATING TABLES
   > - ALSO USED "IGNORE" IN SQL QUERY TO AVOID GETTING ERROR

## 6. RUN :
   > - To Run this project we need to go to terminal and change its path to file_loacted_directory
   > - Then run streamlit code to execute,

           streamlit run file_name.py
   - replace file_name with created python file name
   - To run this project use,

           streamlit run Youtube.py 
# Skills Covered ✅ ⬇️

    Python (Scripting)
    Data Collection
    MongoDB
    SQL
    API Integration
    Data Management using MongoDB and MySQL
    IDE: PyCharm Community Version, VS Code, Jupyter

