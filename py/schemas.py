from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType,FloatType,DateType,TimestampType
    
comments_schema = StructType([
            StructField("textDisplay", StringType(), True),
            StructField("authorDisplayName", StringType(), True),
            StructField("video_id", StringType(), True),
            StructField("publishedAt", DateType(), True),
            StructField("likeCount", IntegerType(), True)           

        ]) 

comments_enriched_schema = StructType([
            StructField("textDisplay", StringType(), True),
            StructField("authorDisplayName", StringType(), True),
            StructField("video_id", StringType(), True),
            StructField("publishedAt", DateType(), True),
            StructField("likeCount", IntegerType(), True),           
            StructField("sentiment", FloatType(), True),
            StructField("isPositive", IntegerType(), True),
            StructField("isNegative", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("extractedWords", StringType(), True)

        ])  
 

transcripts_schema = StructType([
            StructField("text", StringType(), True),
            StructField("start", FloatType(), True),
            StructField("duration", FloatType(), True),
            StructField("video_id", StringType(), True)
      

        ])   

comments_aggbyAuthor_schema = StructType([
            StructField("authorDisplayName", StringType(), True),
            StructField("video_id", StringType(), True),
            StructField("start", TimestampType(), True),
            StructField("end", TimestampType(), True),
            StructField("num_comments", IntegerType(), True),          
            StructField("avg_sentiment", FloatType(), True)

        ])   

comments_aggbyVideo_schema = StructType([
            StructField("video_id", StringType(), True),
            StructField("start", TimestampType(), True),
            StructField("end", TimestampType(), True),
            StructField("sum_Positive", IntegerType(), True),
            StructField("sum_Negative", IntegerType(), True),
            StructField("total", IntegerType(), True)

        ])  

comments_aggbyWatchlist_schema_agg = StructType([
            StructField("extractedWords", StringType(), True),
            StructField("video_id", StringType(), True),
            StructField("start", TimestampType(), True),
            StructField("end", TimestampType(), True),
            StructField("num_comments", IntegerType(), True), 
             StructField("avg_sentiment", FloatType(), True)
          

        ])    
comments_aggbyWatchlist_schema = StructType([
            StructField("textDisplay", StringType(), True),
            StructField("video_id", StringType(), True),
            StructField("start", TimestampType(), True),
            StructField("end", TimestampType(), True),
            StructField("authorDisplayName", StringType(), True),
            StructField("publishedAt", DateType(), True),
            StructField("likeCount", IntegerType(), True),
            StructField("sentiment", FloatType(), True),
            StructField("isPositive:", IntegerType(), True),
            StructField("isNegative", IntegerType(), True),
            StructField("extractedWords", StringType(), True)           

        ])  
trending_videos = StructType([
            StructField("id", StringType(), True),
            StructField("snippet_publishedAt", TimestampType(), True),
            StructField("snippet_channelId", StringType(), True),
            StructField("snippet_title", StringType(), True),
            StructField("snippet_description", StringType(), True),
            StructField("snippet_channelTitle", StringType(), True),
            StructField("snippet_categoryId", IntegerType(), True),
            StructField("statistics_viewCount", IntegerType(), True),
            StructField("statistics_likeCount", IntegerType(), True),
            StructField("statistics_favoriteCount", IntegerType(), True),
            StructField("statistics_commentCount", IntegerType(), True)           

        ]) 
wordcloud_videos= StructType([
            StructField("frequency", IntegerType(), True),
            StructField("words", StringType(), True)
        ]) 