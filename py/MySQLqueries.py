from datetime import datetime
from kafka import KafkaConsumer
import json
import mysql.connector as mc
from time import sleep
import configuration as c
# MySQL
host = 'localhost'
mysql_port = 3306
mysql_database_name = 'youtube'
mysql_table_name = 'comments'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'
# connector to mysql
mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,
    database=mysql_database_name)

#kafka
topicName = c.topic_comment_aggbyVideo
brokers = c.bootstrap_servers

# connector to mysql
mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,
    database=mysql_database_name)

# create a cursor
mysql_cursor = mysql_conn.cursor()

# Create table if needed

create_tbl_comments_enriched = f"""create table if not exists {mysql_database_name}.tbl_comments_enriched"""+\
    """(textDisplay varchar(1000),
    authorDisplayName varchar(1000),
    video_id varchar(50),
    publishedAt varchar(100),
    likeCount integer,
    sentiment float,
    isPositive integer,
    isNegative integer,
    event_time varchar(100),
    extractedWords varchar(1000),
    UpdatedDate datetime,
    CreatedDate datetime,
    PRIMARY KEY (authorDisplayName,video_id,publishedAt)
     );"""
insert_statement_comments_enriched =f"""\
    INSERT INTO tbl_comments_enriched"""+\
    """(textDisplay,authorDisplayName,video_id, publishedAt, likeCount,"""+\
    """sentiment, isPositive, isNegative, event_time,"""+\
    """extractedWords,CreatedDate,UpdatedDate)
    VALUES ('{}' ,'{}', '{}', '{}',{},{},{},{},'{}','{}', Now(),Now())"""
update_part_comments_enriched =f"""\
    ON DUPLICATE KEY UPDATE """+\
    """likeCount= {},extractedWords='{}',"""+\
    """isPositive= {},isNegative={},sentiment='{}', UpdatedDate=Now()"""

create_tbl_transcripts = f"""create table if not exists {mysql_database_name}.tbl_transcripts"""+\
    """(text varchar(10000),
    start varchar(50),
    duration varchar(50),
    video_id varchar(50),
    UpdatedDate datetime,
    CreatedDate datetime,
    PRIMARY KEY (start,video_id)###NEED TO ADD VIDEO ID

     );"""
insert_statement_transcripts =f"""\    
    INSERT INTO tbl_aggbyAuthor"""+\
    """(text,video_id,start,duration, UpdatedDate, CreatedDate)
    VALUES ('{}', '{}', '{}','{}', Now(),Now())"""
update_part_transcripts =f"""\
    ON DUPLICATE KEY UPDATE """+\
    """text= '{}',UpdatedDate=Now()"""

create_tbl_aggbyAuthor = f"""create table if not exists {mysql_database_name}.tbl_aggbyAuthor"""+\
    """(
    authorDisplayName varchar(100),
    video_id varchar(50),
    start varchar(50),
    end varchar(50),
    num_comments int,
    avg_sentiment float,
    UpdatedDate datetime,
    CreatedDate datetime,
    PRIMARY KEY (authorDisplayName,video_id,start,end));"""

insert_statement_aggbyAuthor =f"""\
    INSERT INTO tbl_aggbyAuthor"""+\
    """(authorDisplayName, video_id, start, end, num_comments, avg_sentiment, UpdatedDate,CreatedDate)
    VALUES ('{}', '{}', '{}','{}',{},{}, Now(),Now())"""
update_part_aggbyAuthor =f"""\
    ON DUPLICATE KEY UPDATE """+\
    """num_comments= {},avg_sentiment= {}, UpdatedDate=Now()"""

create_tbl_comments_aggbyVideo_schema = f"""create table if not exists {mysql_database_name}.tbl_aggbyVideo"""+\
    """(video_id varchar(100),
    start varchar(50),
    end varchar(50),
    sum_Positive int,
    sum_Negative int,
    total int,
    UpdatedDate datetime,
    CreatedDate datetime,
    PRIMARY KEY (video_id,start));"""

insert_statement_tbl_aggbyVideo =f"""\
    INSERT INTO tbl_aggbyVideo"""+\
    """(video_id, start, end, sum_Positive,"""+\
    """sum_Negative, total, CreatedDate, UpdatedDate)
    VALUES ('{}', '{}', '{}',{},{},{}, Now(),Now())"""
update_part_tbl_aggbyVideo =f"""\
    ON DUPLICATE KEY UPDATE """+\
    """sum_Positive= {},sum_Negative= {},total= {},UpdatedDate=Now()"""


create_tbl_comments_aggbyWatchlist_schema = f"""create table if not exists {mysql_database_name}.tbl_aggbyWatchlist"""+\
    """(
    extractedWords varchar(100),
    video_id varchar(100),
    start varchar(50),
    end varchar(50),
    num_comments int,
    avg_sentiment float,
    UpdatedDate datetime,
    CreatedDate datetime,
    PRIMARY KEY (video_id,start)
    );"""
insert_statement_tbl_aggbyWatchlist =f"""\
    INSERT INTO tbl_aggbyWatchlist"""+\
    """(extractedWords,video_id, start, end, num_comments,avg_sentiment,"""+\
    """CreatedDate, UpdatedDate)
    VALUES ('{}','{}', '{}', '{}',{},{}, Now(),Now())"""
update_part_tbl_aggbyWatchlist =f"""\
    ON DUPLICATE KEY UPDATE """+\
    """num_comments= {},avg_sentiment= {},UpdatedDate=Now()"""



create_tbl_comments_tbl_trending_videos = f"""create table if not exists {mysql_database_name}.tbl_trending_videos"""+\
    """(
    id varchar(100),
    snippet_publishedAt varchar(100),
    snippet_channelId varchar(100),
    snippet_title varchar(100),
    snippet_description varchar(4000),
    snippet_channelTitle varchar(100),
    snippet_categoryId int,
    statistics_likeCount int,
    statistics_viewCount int,   
    statistics_favoriteCount int,
    statistics_commentCount int,
    UpdatedDate datetime,
    CreatedDate datetime,
    PRIMARY KEY (id)
    );"""
insert_statement_tbl_trending_videos =f"""\
    INSERT INTO tbl_trending_videos"""+\
    """(id, snippet_publishedAt, snippet_channelId, snippet_title,"""+\
    """snippet_description, snippet_categoryId,statistics_likeCount,"""+\
    """statistics_viewCount, statistics_favoriteCount,statistics_commentCount,"""+\
    """CreatedDate, UpdatedDate)
    VALUES ('{}', '{}','{}','{}','{}',{},{},{},{},{}, Now(),Now())"""
update_part_tbl_trending_videos =f"""\
    ON DUPLICATE KEY UPDATE """+\
    """statistics_commentCount={},statistics_likeCount= {},statistics_viewCount= {},statistics_favoriteCount= {},UpdatedDate=Now()"""

create_tbl_wordsCloud = f"""create table if not exists {mysql_database_name}.tbl_wordsCloud"""+\
    """(words varchar(10000),
    frequency int,
    video_id varchar(50),
    UpdatedDate datetime,
    CreatedDate datetime,
    PRIMARY KEY (video_id,frequency)

     );"""
insert_statement_wordsCloud =f"""\    
    INSERT INTO tbl_wordsCloud"""+\
    """(words,frequency,video_id, UpdatedDate, CreatedDate)
    VALUES ('{}', {}, {}, Now(),Now())"""
update_statement_wordsCloud =f"""\
    ON DUPLICATE KEY UPDATE """+\
    """words= '{}',UpdatedDate=Now()"""
    
