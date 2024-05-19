from kafka import KafkaProducer
import os
from time import sleep
import json
from pyspark.sql import SparkSession
import configuration as c
import schemas as s
from pyspark.sql import functions as F
from pyspark.sql.functions import split, explode, lower
from pyspark.sql import types as T
from pyspark.sql.types import FloatType,StringType
from textblob import TextBlob
from pyspark.sql.window import Window



# Topics/Brokers



class SparkStructureStreamingClient:
    def __init__(self):
        self.sparkAppName=c.spark_application_name
        self.spark = SparkSession.builder\
            .master("local[*]")\
            .appName(self.sparkAppName)\
            .getOrCreate()
        self.hdfs_server=c.hdfs_server
        print("SparkSession's built ")
        self.env=os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'



    def setup_consumer_print_toConsole(self,df,outputMode='complete'):
        df.writeStream\
            .format("console")\
            .outputMode(outputMode)\
            .start()\
            .awaitTermination()
        
    def readstream_schema_from_topic(self,topicName,schema):
        df=self.spark \
            .readStream \
            .format('kafka') \
            .option("kafka.bootstrap.servers", c.bootstrap_servers) \
            .option("subscribe", topicName) \
            .load()\
            .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    ##.select(F.col('value').cast(T.StringType()))
        df.printSchema()   
        parsed_df = df.select(F.col("value").cast("string")).select(F.from_json(F.col("value"), schema).alias("value")).select("value.*")
        parsed_df.printSchema()
        return parsed_df
    
    def writestream_data_toKafkaTopic(self,dataframe,topic_name,outputMode='complete'):
        checkpointPath=os.path.join(c.home_path,'kafkacheckpoints/comments',topic_name)
        dataframe.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", c.bootstrap_servers) \
            .option("topic", topic_name) \
            .option("failOnDataLoss", "false")\
            .option('checkpointLocation', checkpointPath) \
            .outputMode(outputMode) \
            .start()\
            .awaitTermination()  

    def setup_sentiment_comment_analysis(self):
        columnBy='textDisplay'
        def get_sentiment(string1):
            return TextBlob(string1).sentiment.polarity
   
        commentsDF=self.readstream_schema_from_topic(topicName=c.topic_comments,schema=s.comments_schema)
        get_sentiment_udf = F.udf(get_sentiment, FloatType())
        df_withSentiment = commentsDF.withColumn('sentiment', get_sentiment_udf(F.col(columnBy)))

        df_withFlags = df_withSentiment.withColumn("isPositive", F.when(df_withSentiment.sentiment>c.sentiment_comment_filter_threshold,1).otherwise(0))\
                                        .withColumn("isNegative", F.when(df_withSentiment.sentiment<-1*c.sentiment_comment_filter_threshold,1).otherwise(0))\
                                        .withColumn('event_time' , F.current_timestamp())
        predefined_words = c.monitoring_list
        predefined_words_lower = set(word.lower() for word in predefined_words)

        def get_predefined_words(text):
            words = text.lower().split()
            extracted_words = ['#'+word for word in words if word in predefined_words_lower]
            if extracted_words:
                return "|".join(extracted_words)
            else:
                return "N/A"
        
        get_predefined_words_udf = F.udf(get_predefined_words, StringType())

        extracted_words_df = df_withFlags.withColumn('extractedWords',get_predefined_words_udf(F.col(columnBy)))
        extracted_words_df.printSchema()
        return extracted_words_df

    def write_parquet_fromTopic_toHDFS(self,topicName,schemaName):

        path=os.path.join(c.hdfs_server,'FinalProject/data/comments',topicName)
        df=self.spark.read \
        .format('kafka')\
        .option("kafka.bootstrap.servers", c.bootstrap_servers) \
        .option("subscribe", topicName) \
        .option('startingOffsets', 'earliest') \
        .load()\
        .select(F.col('value').cast(T.StringType()))
        parsed_df = df.select(F.col("value").cast("string")).select(F.from_json(F.col("value"), schemaName).alias("value")).select("value.*")
        parsed_df.printSchema()
        parsed_df.write.parquet(path, mode='append')
        print(f"Topic {topicName} written to {path}")

