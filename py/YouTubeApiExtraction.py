from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
import pandas as pd
import os
import configuration as c
from pathlib import Path
from datetime import datetime



class YouTubeApiExtractor:
    def __init__(self, videosNumber, regionCode, videoCategoryId, apiKey, maxComments) -> None:
        self.videos_toProcess = videosNumber
        self.regionCode = regionCode
        self.videoCategoryId = videoCategoryId
        self.maxComments = maxComments
        self.youtube = build('youtube', 'v3', developerKey=apiKey)
        self.initDirectories()
        self.createVideosDF()
        print(f'Proccessed {len(self.videos_id_list)} trending videos')
    
    def initDirectories(self):
        print(f'Missing path will be created')
        path=Path(os.path.join(c.home_path,"Comments"))
        path.mkdir(parents=True, exist_ok=True)
        print(path)
        path=Path(os.path.join(c.home_path,"Transcripts"))
        print(path)
        path.mkdir(parents=True, exist_ok=True)
        path=Path(os.path.join(c.home_path,"TrendingVideos"))
        print(path)
        path.mkdir(parents=True, exist_ok=True)
    def createVideosDF(self):
        request = self.youtube.videos().list(
            part="contentDetails,snippet,statistics,localizations",
            chart="mostPopular",
            regionCode=self.regionCode,
            videoCategoryId=self.videoCategoryId,
            maxResults=self.videos_toProcess,
            fields = 'nextPageToken,items'

        )
        cum_response=[]
        while request is not None:
                response = request.execute()
                cum_response+=response['items']
                request = self.youtube.videos().list_next(request,response)
        print("Unpack videos json")

        flattened_list = []
        for item in cum_response:
            flattened_item = {}
            for key, value in item.items():
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        if not isinstance(subvalue, dict):
                            flattened_item[f"{key}_{subkey}"] = subvalue
                else:
                    flattened_item[key] = value
            flattened_list.append(flattened_item)

        # Convert the flattened list of dictionaries to JSON
        current_date = datetime.now()
        # Format the date as YYYYMMDD
        date_integer = int(current_date.strftime('%Y%m%d'))
        fileName=os.path.join(c.home_path,"TrendingVideos",c.videos_fileName+str(date_integer)+".json")
        if os.path.exists(fileName):
            os.remove(fileName)
        dfTrendingVideos = pd.DataFrame(flattened_list)
        dfTrendingVideos.to_json(fileName, orient='records', indent=1)
        #with open(fileName, 'w') as output_file:
        self.videos_id_list = [item["id"] for item in flattened_list]


    # Saving transcripts of given video_id in file
    def get_print_transcript(self, video_id):
        filePath=os.path.join(c.home_path,c.transcript_video_prefix)
        filename = filePath+ video_id + '.json'
        if os.path.exists(filename):
            os.remove(filename)
        # Must be a single transcript.
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id, ['en', 'en-US'])
            print(f"Transcript of {video_id} extracted successfully")
            dftranscript = pd.DataFrame(transcript)
            dftranscript['video_id']=video_id
            dftranscript.to_json(filename, orient='records', indent=1)
            print(f"Printed transcript for video:{video_id}")
        except Exception as e:
            print(f"Transcript for video:{video_id} failed: {e}")
            return

    def get_print_Transcript_byList(self):
        i = 0
        for video_id in self.videos_id_list:
            self.get_print_transcript(video_id)
            i += 1
            print(f"Iteration of get_print_transcript:{i}")

    ##Comments
    def get_Comments_byVideo_Id(self, video_Id):
        request = self.youtube.commentThreads().list(
            part='snippet',
            videoId=video_Id,
            maxResults=self.maxComments
        )
        cum_response=[]
        while request is not None:
                response = request.execute()
                cum_response+=response['items']
                request = self.youtube.commentThreads().list_next(request,response)
        return (response)

    # Saving Comments of given video_id in files
    def get_print_Comments_byVideo_Id(self, video_id):
        print(f"Proccessing video id: {video_id} ")
        filePath=os.path.join(c.home_path,c.comments_video_prefix)
        filename = filePath+ video_id + '.json'
        if os.path.exists(filename):
            os.remove(filename)
        try:
            comments = self.get_Comments_byVideo_Id(video_id)
            print(f"Comments of {video_id} extracted successfully")
            CommentWithAuthorTuple =  [(item['snippet']['topLevelComment']['snippet']['textDisplay'] \
                                            ,item['snippet']['topLevelComment']['snippet']['authorDisplayName']\
                                            ,item['snippet']['topLevelComment']['snippet']['likeCount']\
                                            ,item['snippet']['topLevelComment']['snippet']['publishedAt']\
                                            ,video_id
                                           ) 
                                      for item in comments['items']]

            dfComments = pd.DataFrame(CommentWithAuthorTuple, columns=['textDisplay', 'authorDisplayName','likeCount','publishedAt','video_id'])
            dfComments.to_json(filename, orient='records', indent=1)
            print(f"Printed {len(comments['items'])} comments for video:{video_id}")
        except Exception as e:
            print(f"Comments for video:{video_id} failed: {e}")
        return

    def get_print_Comments_byList(self):
        i = 0
        for video_id in self.videos_id_list:
            self.get_print_Comments_byVideo_Id(video_id)
            i += 1
            print(f"Iteration of get_print_Comments:{i}")



