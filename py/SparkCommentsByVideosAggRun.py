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
dataFrame=spark_stream_client.readstream_schema_from_topic(topicName=c.topic_comments_enriched,schema=s.comments_enriched_schema)
    
grouped_window = dataFrame \
    .groupBy(F.col('video_id'), F.window(F.col('event_time'), '60 minutes').alias('date_window'))\
    .agg(
       F.sum(F.col('isNegative')).alias('sum_Negative'),\
       F.sum(F.col('isPositive')).alias('sum_Positive'),\
        F.sum(F.lit(1)).alias('total') 
       )
df_with_agg_columns = grouped_window.select(F.col('video_id'),\
                                            F.col('sum_Negative').cast(T.IntegerType()),\
                                            F.col('sum_Positive').cast(T.IntegerType()),\
                                            F.col('total').cast(T.IntegerType()),\
                                            F.col('date_window.start'),
                                            F.col('date_window.end'))
df_with_agg_columns.printSchema()
spark_stream_client.writestream_data_toKafkaTopic(dataframe=df_with_agg_columns,topic_name=c.topic_comment_aggbyVideo,outputMode='complete')


