import os
from StreamingManager import SparkStructureStreamingClient
import configuration as c
import schemas as s
from pyspark.sql import functions as F
from pyspark.sql.functions import desc,row_number,approx_count_distinct
from pyspark.sql.window import Window

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

topicTuple =((c.topic_comments_enriched,s.comments_enriched_schema),\
            (c.topic_comment_aggbyVideo,s.comments_aggbyVideo_schema),\
            (c.topic_comments_aggByAuthor,s.comments_aggbyAuthor_schema),\
            (c.topic_comments_aggByWatchList,s.comments_aggbyWatchlist_schema_agg),\
            (c.kafka_videos_topic,s.trending_videos),\
            (c.kafka_transcripts_topic,s.transcripts_schema)

)
spark_stream_client=SparkStructureStreamingClient()

for topic,schema in topicTuple:
    print (f"Setup topic{ topic}, schema {schema}")
    df=spark_stream_client.write_parquet_fromTopic_toHDFS(topicName=topic,schemaName=schema)






  




