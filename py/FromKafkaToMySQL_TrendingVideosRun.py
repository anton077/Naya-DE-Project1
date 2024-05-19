from datetime import datetime
from kafka import KafkaConsumer
import json
import mysql.connector as mc
from time import sleep
import configuration as c
import MySQLqueries as mysql
import nltk ,re
from nltk.tokenize import word_tokenize
#kafka
topicName = c.kafka_videos_topic
nltk.download('punkt')


# create a cursor

def mysql_insert_comments_enriched(mysql_cursor,id,snippet_publishedAt,snippet_channelId, snippet_title,\
                                    snippet_description, snippet_categoryId,statistics_likeCount,\
                                    statistics_viewCount, statistics_favoriteCount,statistics_commentCount):

    insertSQL=mysql.insert_statement_tbl_trending_videos\
        .format(id,snippet_publishedAt,snippet_channelId, snippet_title,\
                                    snippet_description, snippet_categoryId,statistics_likeCount,\
                                    statistics_viewCount, statistics_favoriteCount,statistics_commentCount)\
            +mysql.update_part_tbl_trending_videos.format(statistics_commentCount,statistics_likeCount,statistics_viewCount,statistics_favoriteCount)

    print(insertSQL)
    mysql_cursor.execute(insertSQL)
def is_valid_string(text):
    # Define a regular expression to match valid ASCII characters
    pattern = re.compile(r'^[\x00-\x7F]+$')
    return pattern.match(text) is not None
def mysql_comments_enriched():
    print (f"Monitoring Topic {topicName} to Mysql")
    createTable=mysql.create_tbl_comments_tbl_trending_videos
    print(createTable)
    mysql_cursor = mysql.mysql_conn.cursor()
    mysql_cursor.execute(createTable)



    for message in consumer:
        content = json.loads(message.value)
        print(content)
        id=content['id']
        snippet_publishedAt=content['snippet_publishedAt']
        snippet_channelId=content['snippet_channelId']
        
        input_string=str(word_tokenize(content['snippet_title'])[:100])
        input_string_cleaned=re.sub(r'[^\w\s]', '', input_string)
        if is_valid_string(input_string_cleaned):
            snippet_title=input_string_cleaned
        else:
            snippet_title='N/A'
        snippet_categoryId=content['snippet_categoryId']

        input_string=str(word_tokenize(content['snippet_description'])[:100])
        input_string_cleaned=re.sub(r'[^\w\s]', '', input_string)
        if is_valid_string(input_string_cleaned):
            snippet_description=input_string_cleaned
        else:
            snippet_description='N/A'

        snippet_categoryId=content['snippet_categoryId']
  
        try:
            statistics_commentCount=content['statistics_commentCount']
        except KeyError:
            statistics_commentCount=0
        try:
            statistics_viewCount=content['statistics_viewCount']
        except KeyError:
            statistics_viewCount=0
        try:
            statistics_likeCount=content['statistics_likeCount']
        except KeyError:
            statistics_likeCount=0       
        try:
            statistics_favoriteCount=content['statistics_favoriteCount']
        except KeyError:
            statistics_favoriteCount=0

        try:
            mysql_insert_comments_enriched(mysql_cursor,id,snippet_publishedAt,snippet_channelId, snippet_title,\
                                    snippet_description, snippet_categoryId,statistics_likeCount,\
                                    statistics_viewCount, statistics_favoriteCount,statistics_commentCount)
        except Exception as e:
            print(f"Insert enriched comment for video:{id} failed: {e}")
    mysql_cursor.close()

consumer = KafkaConsumer(
    topicName,
    bootstrap_servers=c.bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

mysql_comments_enriched()





# # During the work on MySQL database one may wsh to recreate the kafka_pipeline table.
# mysql_cursor.execute('drop table kafka_pipeline')

#https://stackoverflow.com/questions/44927687/not-clear-about-the-meaning-of-auto-offset-reset-and-enable-auto-commit-in-kafka


"""

value1="valsdsdaue1"
value2="valsdsdaue1"
table_name='tbl_aggSentimentByVideo'
columns=['video_id, sum_Positive, sum_Negative, UpdatedDate,CreatedDate']
values = [(value1,value2 , "value3","value2", "value3")]
updaetecolumns=['video_id, sum_Positive, sum_Negative, UpdatedDate,CreatedDate']
update_values=['video_id, sum_Positive, sum_Negative, UpdatedDate,CreatedDate']

insert_part = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(values[0]))})"
update_part = f"ON DUPLICATE KEY UPDATE {', '.join([f'{column} = VALUES({column})' for column in update_values])}"
sql_statement = f"{insert_part} {update_part}"
sql_statement
"""