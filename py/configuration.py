videos_toProcess = 100
category_toProcess = 25
regionCode_toProcess = 'US'
maxComments_toProcess = 1000
api_key_path="ApiKey.txt"
topic_comments = 'kafka-comments'
topic_comments_enriched = 'kafka-comments-enriched'
topic_comment_aggbyVideo='kafka-agg-video-bySentiment-witheventTime'
topic_comments_aggByAuthor='kafka-agg-author-bySentiment'
topic_comments_aggByWatchList='kafka_comments_output_topic_aggbyWatchList_agg'
kafka_transcripts_topic = 'kafka-transcripts'
kafka_videos_topic = 'kafka-video'
videos_fileName='TrendingVideos'
transcript_video_prefix='Transcripts/CaptureTranscript_'
comments_video_prefix='Comments/CaptureComments_'
bootstrap_servers = 'cnt7-naya-cdh63:9092'
spark_application_name = 'FinalProject'
hdfs_server = 'hdfs://Cnt7-naya-cdh63:8020'
home_path = '/usr/final_project/Data'
monitoring_list=['Israel','eurovision','Biden','Eden Golan','government','Hamas','Russia','Ukraine','Iran','USA','America',]
comments_under_same_video_threshold=0
negative_comments_count_ofVideo_threshold=10
sentiment_comment_filter_threshold=0.2


