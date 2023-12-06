# YouTube Data Harvesting and Warehousing

## Table of Contents:
1. About The Project
2. Getting Started
   - Prerequisites
   - Installation
4. Usage
5. Steps
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
   1. We created 
# Skills Covered ✅ ⬇️

    Python (Scripting)
    Data Collection
    MongoDB
    SQL
    API Integration
    Data Management using MongoDB and MySQL
    IDE: PyCharm Community Version, VS Code, Jupyter

