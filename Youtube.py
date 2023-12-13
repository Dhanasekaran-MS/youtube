from googleapiclient.discovery import build
import pymongo
import pymysql
import streamlit as st
import pandas as pd


# Get an API client
def get_client():
    api_service_name = "youtube"
    api_version = "v3"
    API_KEY = "AIzaSyAnU2BfmUhJYhVi9K6_BfdGnfTWIbeyhok"

    youtube = build(api_service_name, api_version, developerKey=API_KEY)
    return youtube


youtube = get_client()


# getting channel details from api using channel_id using "channels().list"
def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    # filtering data and storing them in variables
    item = response["items"][0]
    Details = dict(Channel_Name=item["snippet"]['title'],
                   Channel_Id=item["id"],
                   Description=item["snippet"]["description"],
                   Subscribers=item["statistics"]["subscriberCount"],
                   Total_Views=item["statistics"]["viewCount"],
                   Videos_count=item["statistics"]["videoCount"],
                   Playlist_id=item["contentDetails"]["relatedPlaylists"]["uploads"])
    return Details


# fetching 'video ids' from channel using 'playlist id' from a channel_id using "PlaylistItems().list()"
def get_video_ids(channel_id):
    response = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id).execute()
    Playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None
    video_ids = []

    while True:
        response1 = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=Playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get("nextPageToken")
        if next_page_token is None:
            break
    return video_ids


# Getting and Collecting Video Details using video_ids from get_video_id() using "videos().list"
def get_video_details(video_ids):
    video_data = []
    for v_id in video_ids:
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=v_id
        ).execute()
        for item in response["items"]:
            data = dict(Channel_Name=item["snippet"]["channelTitle"],
                        Channel_Id=item["snippet"]["channelId"],
                        Title=item["snippet"]["title"],
                        Video_Id=item["id"],
                        Description=item["snippet"].get("description"),
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item["snippet"]['thumbnails']['default']['url'],
                        Published_Date=item["snippet"]['publishedAt'],
                        Duration=item["contentDetails"]['duration'],
                        Views=item['statistics']['viewCount'],
                        Likes=item['statistics']['likeCount'],
                        Favorite=item['statistics']['favoriteCount'],
                        Comment_Count=item['statistics'].get('commentCount'),
                        Definition=item["contentDetails"]['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


# Getting and Collecting comment information using video_ids from get_comment_details using "commentThreads().list()"
def get_comment_details(video_ids):
    next_page_token = None
    comment_data = []

    # error-"exception" may arise if the comment section is off to handle this exception we use exceptional handling:
    for video_id in video_ids:
        while True:
            try:
                response = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100,  # Default 20
                    pageToken=next_page_token  # There may be more than 100 cmts for a video
                ).execute()
                for item in response["items"]:
                    comment = item["snippet"]['topLevelComment']
                    data = dict(Comment_Id=comment['id'],
                                Video_Id=comment['snippet']['videoId'],
                                Comment_Text=comment['snippet']['textDisplay'],
                                Comment_Author=comment['snippet']['authorDisplayName'],
                                Comment_Published_date=comment['snippet']['publishedAt']
                                )
                    comment_data.append(data)
                next_page_token = response.get("nextPageToken")
                if next_page_token is None:
                    break
            except:
                break  # To break the while loop if try block catches error "video might has disabled comments"
    return comment_data


# getting 'Playlists_details' using 'channel_id' from get_playlists_details using 'playlists().list()'
def get_playlists_details(channel_id):
    next_page_token = None
    playlist_details = []

    while True:
        response = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response["items"]:
            data = dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        Video_count=item['contentDetails']['itemCount'],
                        Playlist_Published_At=item['snippet']['publishedAt']
                        )
            playlist_details.append(data)
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
    return playlist_details


# MONGO DB CONNECTION
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['Youtube']
collection = db['Data']


# creating a function data_to_mongo to upload all data into mongodb as collection "channel_details"
def data_to_mongo(channel_id):
    channel_data = get_channel_details(channel_id)
    video_ids = get_video_ids(channel_id)
    playlist_data = get_playlists_details(channel_id)
    video_data = get_video_details(video_ids)
    comment_data = get_comment_details(video_ids)

    collection.insert_one({"Channel_Info": channel_data, "Playlist_Info": playlist_data,
                           "Video_Info": video_data, "Comments_Info": comment_data})
    return f"{channel_data['Channel_Name']}'s YouTube data has Uploaded To MongoDB Successfully"


def formatted_duration(dur):
    hours = 0
    minutes = 0
    seconds = 0

    duration = dur.replace("PT", "")
    if 'H' in duration:
        hours = int(duration.split('H')[0])
        duration = duration.split('H')[1]
    if 'M' in duration:
        minutes = int(duration.split('M')[0])
        duration = duration.split('M')[1]
    if 'S' in duration:
        seconds = int(duration.split('S')[0])
    formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return formatted


def mongo_to_sql(selected_channels):
    mydb = pymysql.Connection(host="127.0.0.1", user="root", passwd="Dhana@123")
    cur = mydb.cursor()
    cur.execute("create database if not exists Youtube")
    # Establishing the connection to MySQL database --> 'youtube_data' and creating cursor
    mydb = pymysql.Connection(host="127.0.0.1", user="root", passwd="Dhana@123", database="Youtube")
    cur = mydb.cursor()

    # Getting mongo DB client and collection
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["Youtube"]
    collection = db["Data"]
    # creating channels table query :
    create_ch = '''create table if not exists Channels(Channel_Name varchar(90),
                                                          Channel_Id varchar(80) primary key,
                                                          Description text,
                                                          Subscribers bigint,
                                                          Total_Views bigint,
                                                          Videos_count int,
                                                          Playlist_id varchar(50))'''
    cur.execute(create_ch)
    mydb.commit()
    # creating playlists table query :
    create_pl = '''create table if not exists Playlists(Playlist_Id varchar(50) primary key,
                                                           Title varchar(120),
                                                           Channel_Id varchar(80),
                                                           Channel_Name varchar(40),
                                                           Video_count int,
                                                           Playlist_Published TIMESTAMP)'''
    cur.execute(create_pl)
    mydb.commit()
    # creating videos table query :
    create_vdo = '''create table if not exists Videos(Channel_Name varchar(40),
                                                  Channel_Id varchar(80),
                                                  Title text,
                                                  Video_Id varchar(100) primary key,
                                                  Description text,
                                                  Tags text,
                                                  Thumbnail text,
                                                  Published_Date TIMESTAMP,
                                                  Duration time,
                                                  Views bigint,
                                                  Likes bigint,
                                                  Favorite int,
                                                  Comment_Count bigint,
                                                  Definition varchar(20),
                                                  Caption_Status varchar(20)
                                                  ) '''
    cur.execute(create_vdo)
    mydb.commit()
    # creating comments table query :
    create_cmt = '''create table if not exists Comments(Comment_Id varchar(100) primary key,
                                                      Video_Id varchar(100),
                                                      Comment_Text text,
                                                      Comment_Author varchar(60),
                                                      Comment_date timestamp)'''
    cur.execute(create_cmt)
    mydb.commit()

    # Insert channels_data into channels table:    
    channel_columns = ["Channel_Name", "Channel_Id", "Description", "Subscribers", "Total_Views", "Videos_count",
                       "Playlist_id"]
    channel_col_count = ["%s"] * len(channel_columns)
    insert_ch = f'''insert ignore into Channels({",".join(channel_columns)})
                values({",".join(channel_col_count)})'''
    for channels in collection.find({}, {"_id": 0, "Channel_Info": 1}):
        for channel in channels.values():
            if channels['Channel_Info']["Channel_Name"] in selected_channels:
                cur.execute(insert_ch, tuple(channel.values()))
                mydb.commit()
    # Insert playlist_data into playlists table:
    playlist_columns = ["Playlist_Id", "Title", "Channel_Id", "Channel_Name", "Video_count", "Playlist_Published"]
    playlist_col_count = ["%s"] * len(playlist_columns)
    insert_pl = f'''insert ignore into Playlists({','.join(playlist_columns)})
                    values({','.join(playlist_col_count)})'''
    for playlists in collection.find({}, {"_id": 0, "Playlist_Info": 1}):
        for list_item in playlists.values():
            for playlist in list_item:
                if playlist['Channel_Name'] in selected_channels:
                    cur.execute(insert_pl, tuple(playlist.values()))
                    mydb.commit()
    # inserting videos_data into videos table:
    video_columns = ["Channel_Name", "Channel_Id", "Title", "Video_Id", "Description", "Tags", "Thumbnail",
                     "Published_Date", "Duration",
                     "Views", "Likes", "Favorite", "Comment_Count", "Definition", "Caption_Status"]
    video_col_count = ["%s"] * len(video_columns)
    insert_vdo = f'''insert ignore into Videos({','.join(video_columns)})
                     values({','.join(video_col_count)})'''
    vid_cmt=[]
    for videos in collection.find({}, {"_id": 0, "Video_Info": 1}):
        for video in videos.values():
            for lists in video:
                if lists["Channel_Name"] in selected_channels:
                    try:
                        lists["Tags"] = ','.join(lists["Tags"])
                    except TypeError:
                        pass
                    # using formatted_duration() to format the time to store it in MySQL
                    lists["Duration"] = formatted_duration(lists["Duration"])
                    values = tuple(lists.get(col, None) for col in video_columns)
                    cur.execute(insert_vdo, values)
                    mydb.commit()
                    vid_cmt.append(lists['Video_Id'])
    # insert comments_data into comments table:
    comment_columns = ['Comment_Id', 'Video_Id', 'Comment_Text', 'Comment_Author', 'Comment_date']
    comment_col_count = ['%s'] * len(comment_columns)
    insert_cmt = f'''insert ignore into Comments({','.join(comment_columns)})
                   values({','.join(comment_col_count)})'''
    for comments in collection.find({}, {"_id": 0, "Comments_Info": 1}):
        for lists in comments.values():
            for comment in lists:
                if comment['Video_Id'] in vid_cmt:
                    cur.execute(insert_cmt, tuple(comment.values()))
                    mydb.commit()
    mydb.close()
    return "The Data From MongoDB has now Migrated to MySQL"


with st.sidebar:
    st.title('**You:red[Tube]** :blue[Data Harvesting and Warehousing in Streamlit]')
    st.header(body=":orange[**SKILLS TAKE AWAY :**]", divider="rainbow")
    st.caption('  - :violet[**Python** Scripting]')
    st.caption('  - :violet[Data **Collection and Management**]')
    st.caption('  - :violet[**Mongo DB**]')
    st.caption('  - :violet[**MySQL**]')
    st.caption('  - :violet[**API** Integration]')
    st.caption('  - :violet[Data **Migration**]')
    st.caption('  - :violet[**Streamlit**]')
try:
    mydb = pymysql.Connection(host="127.0.0.1", user="root", passwd="Dhana@123", database="Youtube")
    cur = mydb.cursor()
except:
    st.warning(":no_entry_sign: The Database has not Created, Migrate Data to MySQL")

channels = []
ch_name = []
for channel in collection.find({}, {"_id": 0, "Channel_Info": 1}):
    channels.append(channel["Channel_Info"]["Channel_Id"])
    ch_name.append(channel["Channel_Info"]["Channel_Name"])

channel_id = st.text_input("Enter a **You:red[Tube]** Channel Id :")

if st.button("Extract to :violet[**MongoDB**]"):
    if channel_id in channels:
        st.warning("The Channel Data Already Exists", icon="тЪая╕П")
    else:
        insert = data_to_mongo(channel_id)
        st.success(insert)

select = st.multiselect(':red[**Select Channel Names to Migrate**]',
                        ch_name,
                        placeholder='Select Channels')

if st.button("Transfer to :blue[**MySQL**]"):
    migrate = mongo_to_sql(select)
    st.success(migrate)

on = st.toggle(':green[**View Table**]')
if on:
    show = st.radio("SELECT A TABLE TO VIEW", ["CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"],
                    captions=["To view Channels Table", "To view Playlists Table", "To view Videos Table",
                              "To view Comments Table"],
                    index=None)

    if show == "CHANNELS":
        ch = pd.read_sql_query("select * from channels;", mydb)
        st.write(ch)
    elif show == "PLAYLISTS":
        pl = pd.read_sql_query("select * from playlists;", mydb)
        st.write(pl)
    elif show == "VIDEOS":
        vd = pd.read_sql_query("select * from videos;", mydb)
        st.write(vd)
    elif show == "COMMENTS":
        ct = pd.read_sql_query("select * from comments;", mydb)
        st.write(ct)

question = st.selectbox(':orange[**Select a *Question* to Display Table**]',
                        ("1. The name of all the videos and their channel name",
                         "2. The Channel which  has most number of Videos",
                         "3. Top 10 Most Viewed Videos and their  Channel Name",
                         "4. How many Comments were made on each Video ?",
                         "5. Video that has the Highest Number of Likes and it's Channel Name",
                         "6. The Total Number of Likes for each Video",
                         "7. The Total Number of Views for each Channel",
                         "8. The Channels that has published videos in the year 2022",
                         "9. The Average Duration of all Videos in each channel",
                         "10. The Video which has Highest No of Comments and it's Channel Name"
                         ),
                        index=None,
                        placeholder='Select a Question !'
                        )
if question == "1. The name of all the videos and their channel name":
    query = 'select title as VIDEO,channel_name as CHANNEL from videos;'
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "2. The Channel which  has most number of Videos":
    query = "select channel_name as CHANNEL,count(video_id)as VIDEOS_COUNT from videos group by channel_name order by count(video_id) desc limit 1;"
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "3. Top 10 Most Viewed Videos and their  Channel Name":
    query = 'select title as VIDEO, channel_name as CHANNEL,views as VIDEO_VIEWS from videos order by views desc limit 10;'
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "4. How many Comments were made on each Video ?":
    #     query="select b.title as VIDEO,count(a.comment_text) COMENTS_COUNT from comments as a left join videos as b on a.video_id=b.video_id group by a.video_id;"
    query = "select comment_count as COMMENTS , title as VIDEO from videos;"
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "5. Video that has the Highest Number of Likes and it's Channel Name":
    query = "select title as VIDEO,channel_name as CHANNEL,likes as LIKES from (select *,dense_rank()over(order by likes desc) as r from videos)as ranked_table where r=1;"
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "6. The Total Number of Likes for each Video":
    query = "select likes as NO_OF_LIKES,title as VIDEO from videos;"
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "7. The Total Number of Views for each Channel":
    query = "select sum(views) as TOTAL_VIEWS,channel_name as CHANNEL from videos group by channel_name;"
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "8. The Channels that has published videos in the year 2022":
    query = "select distinct channel_name as CHANNEL from videos where year(published_date)=2022;"
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "9. The Average Duration of all Videos in each channel":
    query = "select channel_name as CHANNEL,sec_to_time(avg(time_to_sec(duration))) as AVERAGE_DURATION from videos group by channel_name;"
    result = pd.read_sql_query(query, mydb)
    st.write(result)
elif question == "10. The Video which has Highest No of Comments and it's Channel Name":
    # query='''select title as VIDEO,count(comment_text)as comments_count,channel_name as CHANNEL from comments as a 
    #         left join videos as b on a.video_id=b.video_id group by a.video_id order by comments_count desc limit 1;'''
    query = "select comment_count as COUNT,title as VIDEO,channel_name as CHANNEL from videos where comment_count=(select max(comment_count) from videos);"
    result = pd.read_sql_query(query, mydb)
    st.write(result)
