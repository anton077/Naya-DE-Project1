from datetime import datetime
from kafka import KafkaConsumer
import json
import mysql.connector as mc
from time import sleep
import configuration as c
import MySQLqueries as mysql
import nltk ,re
from nltk.tokenize import word_tokenize

# Download the punkt tokenizer models if not already downloaded
nltk.download('punkt')
#kafka
topicName = c.topic_comments_enriched



# create a cursor

def mysql_insert_comments_enriched(mysql_cursor,textDisplay,video_id,authorDisplayName, publishedAt,\
                                        likeCount,sentiment, isPositive, isNegative, event_time,extractedWords):

    insertTableSQL=mysql.insert_statement_comments_enriched\
        .format(textDisplay,authorDisplayName,video_id, publishedAt,\
                                        likeCount,sentiment, isPositive, isNegative, event_time,extractedWords)\
        +mysql.update_part_comments_enriched.format(likeCount,extractedWords,isPositive,isNegative,sentiment)
    print(insertTableSQL)
    mysql_cursor.execute(insertTableSQL)

def mysql_comments_enriched():
    print (f"Monitoring Topic {topicName} to Mysql")
    createTable=mysql.create_tbl_comments_enriched
    print(createTable)
    mysql_cursor = mysql.mysql_conn.cursor()
    mysql_cursor.execute(createTable)
    for message in consumer:
        content = json.loads(message.value)
        print(content)  
        input_string=str(word_tokenize(content['textDisplay'])[:1000])
        input_string_cleaned=re.sub(r'[^\w\s]', '', input_string)
        textDisplay=input_string_cleaned
        authorDisplayName=content['authorDisplayName']
        publishedAt=content['publishedAt']
        likeCount=content['likeCount']
        sentiment=content['sentiment']
        isPositive=content['isPositive']
        isNegative=content['isNegative']
        event_time=content['event_time']
        extractedWords=content['extractedWords']
        video_id=content['video_id']

        try:
            mysql_insert_comments_enriched(mysql_cursor,textDisplay,video_id,authorDisplayName, publishedAt,\
                                        likeCount,sentiment, isPositive, isNegative, event_time,extractedWords)
        except Exception as e:
            print(f"Insert enriched comment for video:{video_id} failed: {e}")

    mysql_cursor.close()

consumer = KafkaConsumer(
    topicName,
    bootstrap_servers=c.bootstrap_servers,
    auto_offset_reset='latest',
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