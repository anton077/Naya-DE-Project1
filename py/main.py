
import sys, argparse
from YouTubeApiExtraction import YouTubeApiExtractor
from FileManager import HDFSClient,SparkClient
import configuration as c
#from Runner import run_all
from time import sleep

print("Welcome")

def get_api_key(api_path):
    with open(api_path, 'r') as file:
        api_key = file.readline()
    return api_key


def get_data(videosNumber, regionCode, videoCategoryId, apiKey, maxComments):
    print(sys.argv[0])
    y = YouTubeApiExtractor(videosNumber, regionCode, videoCategoryId, apiKey, maxComments)
    y.get_print_Comments_byList() 
    y.get_print_Transcript_byList()
    

def init_hdfs_directory():
    hdfs=HDFSClient()
    hdfs.make_dir('/FinalProject') #create directory FinalProject
    print(hdfs.get_directory_items(''))

if __name__ == "__main__": 

    apiKey = get_api_key(c.api_key_path)
    videosNumber = c.videos_toProcess
    regionCode = c.regionCode_toProcess
    videoCategoryId = c.category_toProcess
    maxComments = c.maxComments_toProcess
    get_data(videosNumber, regionCode, videoCategoryId, apiKey, maxComments)
