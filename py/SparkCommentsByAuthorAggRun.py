import os
from StreamingManager import SparkStructureStreamingClient
import configuration as c
import schemas as s
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pyspark.sql.window import Window


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

spark_stream_client=SparkStructureStreamingClient()
dataFrame=spark_stream_client.readstream_schema_from_topic(topicName=c.topic_comments_enriched,schema=s.comments_enriched_schema)
df_filteredNegative=dataFrame.filter(F.abs(F.col('sentiment'))>c.sentiment_comment_filter_threshold)
df_filteredNegative.printSchema()

grouped_window = df_filteredNegative\
    .groupBy(F.col("authorDisplayName"),F.col("video_id"),F.window(F.col('event_time'), '60 minutes').alias('date_window'))\
        .agg(
            F.count(F.lit(1)).alias("num_comments"),
            F.avg(F.col("sentiment")).alias("avg_sentiment")

            )

df_with_agg_columns = grouped_window.select(                                         
                                            F.col('authorDisplayName').cast(T.StringType()),\
                                            F.col('video_id').cast(T.StringType()),\
                                            F.col('date_window.start'),\
                                            F.col('date_window.end'),\
                                            F.col('num_comments').cast(T.IntegerType()),\
                                            F.col('avg_sentiment').cast(T.FloatType())

                                               ).filter(F.col('num_comments') >c.comments_under_same_video_threshold) 

df_filtered=df_with_agg_columns.filter(F.col('num_comments') >c.comments_under_same_video_threshold) 

df_with_agg_columns.printSchema()
spark_stream_client.writestream_data_toKafkaTopic(dataframe=df_with_agg_columns,topic_name=c.topic_comments_aggByAuthor,outputMode='complete')


