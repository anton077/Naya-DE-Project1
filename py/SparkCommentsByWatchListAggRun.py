import os
from StreamingManager import SparkStructureStreamingClient
import configuration as c
from pyspark.sql import functions as F
from pyspark.sql import types as T
import schemas as s

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

spark_stream_client=SparkStructureStreamingClient()
dataFrame=spark_stream_client.readstream_schema_from_topic(topicName=c.topic_comments_enriched,schema=s.comments_enriched_schema)

df_filteredNA=dataFrame.filter(F.col('extractedWords')!=F.lit('N/A'))
df_filtered=df_filteredNA.filter(F.abs(F.col('sentiment')) >c.sentiment_comment_filter_threshold)

grouped_window = df_filtered\
    .groupBy(F.col("extractedWords"),F.col("video_id"),F.window(F.col('event_time'), '60 minutes').alias('date_window'))\
        .agg(
            F.count(F.lit(1)).alias("num_comments"),
            F.avg(F.col("sentiment")).alias("avg_sentiment")
            )

df_with_agg_columns = grouped_window.select(                                         
                                            F.col('extractedWords').cast(T.StringType()),\
                                            F.col('video_id').cast(T.StringType()),\
                                            F.col('date_window.start'),\
                                            F.col('date_window.end'),\
                                            F.col('num_comments').cast(T.IntegerType()),\
                                            F.col('avg_sentiment').cast(T.FloatType()),\
    
                                               ) 

spark_stream_client.writestream_data_toKafkaTopic(dataframe=df_with_agg_columns,topic_name=c.topic_comments_aggByWatchList,outputMode='complete')






