import pyarrow as pa
import os
from pyspark.sql import SparkSession
import configuration as c



class HDFSClient:
    def __init__(self):
        fs = pa.hdfs.connect(
            host='Cnt7-naya-cdh63',
            port=8020,
            user='hdfs',
            kerb_ticket=None,
            extra_conf=None)
        self.fs=fs
        self.hdfs_server=c.hdfs_server

    def get_directory_items(self,path='/'):
        print('Path: {}{}'.format(self.hdfs_server,path))
        ls=pa.hdfs.HadoopFileSystem.ls(self.fs, '{}{}'.format(self.hdfs_server,path), detail=False)
        return ls

    def make_dir(self,path=''):
        print('Creation of {}{}'.format(self.hdfs_server,path))
        if pa.hdfs.HadoopFileSystem.exists(self.fs, path):
            print("The directory {} already exists!".format(path))
        else:
            pa.hdfs.HadoopFileSystem.mkdir(self.fs, '{}{}'.format(self.hdfs_server,path), create_parents=True)
            print("The directory {} created!".format(path))

    def upload_files(self,source_dir,target_dir,extension = '.json'):
        files = [f for f in os.listdir(source_dir) if f.endswith(extension)]
        print(files)
        for filename in files:
            with open(filename, 'rb') as f:
                pa.hdfs.HadoopFileSystem.upload(self.fs, '{}{}/{}'.format(self.hdfs_server,target_dir,filename), f,buffer_size=None)
                print('Uploaded file {}'.format(filename))
    def remove_directory(self,directory):
        pa.hdfs.HadoopFileSystem.rm(self.fs,'{}{}'.format(self.hdfs_server,directory), recursive=True)

class SparkClient:
    def __init__(self):
        self.spark = SparkSession.builder\
            .master("local")\
            .appName('FinalProject')\
            .getOrCreate()
    def files_to_parquet(self,source_dir,target_dir,files_format = "json"):
        df = self.spark.read.load('{}{}'.format(self.hdfs_server,source_dir), format=files_format)
        df.write.parquet('{}{}'.format(self.hdfs_server,target_dir), mode='overwrite')
        self.spark.stop()

