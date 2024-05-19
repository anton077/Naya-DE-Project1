import os
from StreamingManager import SparkStructureStreamingClient
import configuration as c
import schemas as s
from pyspark.sql import functions as F
from pyspark.sql.functions import desc,row_number,approx_count_distinct
from pyspark.sql.window import Window


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
spark_stream_client=SparkStructureStreamingClient()
df=spark_stream_client.readstream_schema_from_topic(c.topic_comment_aggbyVideo,s.comments_aggbyVideo_schema)
spark_stream_client.setup_consumer_print_toConsole(df,outputMode='append')
