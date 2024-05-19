import os
from StreamingManager import SparkStructureStreamingClient
import configuration as c
import schemas as s
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pyspark.sql.window import Window


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

spark_stream_client=SparkStructureStreamingClient()
##spark_stream_client.setup_producer_comment_analysis_byVideo()
df=spark_stream_client.setup_sentiment_comment_analysis()#from kafka_comments_topic
df.printSchema()
spark_stream_client.writestream_data_toKafkaTopic(dataframe=df,topic_name=c.topic_comments_enriched,outputMode='append')