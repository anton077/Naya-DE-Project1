from datetime import datetime
from kafka import KafkaConsumer
import json
import mysql.connector as mc
from time import sleep
import configuration as c
import MySQLqueries as mysql

#kafka
topicName = c.topic_comment_aggbyVideo


# create a cursor

def mysql_insert_aggSentimentbyVideo(mysql_cursor,video_id, start,end, sum_Positive,sum_Negative,total):

    insertTableSQL=mysql.insert_statement_tbl_aggbyVideo\
        .format(video_id, start,end, sum_Positive,sum_Negative,total)\
        +mysql.update_part_tbl_aggbyVideo.format(sum_Positive,sum_Negative,total)
    print(insertTableSQL)
    mysql_cursor.execute(insertTableSQL)

def mysql_aggSentimentbyVideo():
    print (f"Monitoring Topic {topicName} to Mysql")
    createTable=mysql.create_tbl_comments_aggbyVideo_schema
    print(createTable)
    mysql_cursor = mysql.mysql_conn.cursor()
    mysql_cursor.execute(createTable)
    for message in consumer:
        content = json.loads(message.value)
        print(content)
        video_id=content['video_id']
        start=content['start']
        end=content['end']
        sum_Positive=content['sum_Positive']
        sum_Negative=content['sum_Negative']
        total=content['total']
        print(video_id, start, sum_Positive,sum_Negative,total)
        try:
            mysql_insert_aggSentimentbyVideo(mysql_cursor,video_id, start,end, sum_Positive,sum_Negative,total)
        except Exception as e:
            print(f"Insert enriched comment for video:{video_id} failed: {e}")
    mysql_cursor.close()

consumer = KafkaConsumer(
    topicName,
    bootstrap_servers=c.bootstrap_servers,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

mysql_aggSentimentbyVideo()





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