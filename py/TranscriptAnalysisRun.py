from pyspark.sql import SparkSession
import configuration as c
from FileManager import HDFSClient,SparkClient
from pyspark.sql import functions as F
import schemas as s
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from matplotlib import pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
from sklearn.pipeline import make_pipeline
import nltk
import MySQLqueries as mysql
from sqlalchemy import create_engine

nltk.download('stopwords')
stopwords_list = stopwords.words('english')

from pathlib import Path
import pandas as pd
hdfs=HDFSClient()
#hdfs.get_directory_items('/FinalProject/data/comments/kafka-transcripts')
transcriptsDir='/FinalProject/data/comments/kafka-transcripts'
# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user=mysql.mysql_username,
                               pw=mysql.mysql_password,
                               db=mysql.mysql_database_name))

path=c.hdfs_server+transcriptsDir
spark = SparkSession.builder\
            .master("local")\
            .appName('FinalProject')\
            .getOrCreate()
df = spark.read.format("parquet").option("header", "true").load(path)

transcriptDF=df.toPandas()

video_id_lists = transcriptDF.groupby('video_id')['text'].apply(list).reset_index()
video_id_lists.set_index('video_id',inplace=True)

for video_id in video_id_lists.index:
    transcript_toProcess=video_id_lists.loc[video_id][:10000].text #limit 10000 rows only
    # CountVectorizer converts a collection of text documents to a matrix of token counts
    c_vec = CountVectorizer(stop_words=stopwords_list, ngram_range=(2,3))
    # Fit the CountVectorizer to the reviews data to get a matrix of ngrams
    ngrams = c_vec.fit_transform(transcript_toProcess)
    # Count the frequency of ngrams
    count_values = ngrams.toarray().sum(axis=0)
    # Get a list of ngrams
    vocab = c_vec.vocabulary_
    # Create a DataFrame to store the frequency and n-gram, sorted in descending order of frequency
    df_ngram = pd.DataFrame(sorted([(count_values[i],k) for k,i in vocab.items()], reverse=True)
        ).rename(columns={0: 'frequency', 1:'bigram/trigram'})
    # Display the top 10 n-gr ams by frequency
    sorted_df = df_ngram.sort_values(by='frequency',ascending=False)
    df_output=sorted_df.head(n=10).copy()
    df_output['video_id']=video_id
    df_output.to_sql('tbl_words_cloud', con = engine, if_exists = 'append', chunksize = 1000)



        