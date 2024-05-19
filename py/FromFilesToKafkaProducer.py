import time
import json
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer
import configuration as c


# Folder to watch
##folder_to_watch = '/home/'
# Kafka Producer


class KafkaClient():
    def __init__(self,topic_name,filesDir='Comments'):
        
        # Kafka configuration
        self.bootstrap_servers = c.bootstrap_servers
        self.topic = topic_name
        self.folder_to_watch= os.path.join(c.home_path,filesDir)
        self.producer = KafkaProducer(bootstrap_servers=c.bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.start()
   
    def get_NewFileHandler(self,topic,producer):
        return self.NewFileHandler(topic,producer)     
    # Watchdog event handler
    class NewFileHandler(FileSystemEventHandler):
        def __init__(self,topic,producer):
            self.topic_name=topic
            self.producer=producer
            print (f'Topic name {self.topic_name} ')
        def on_created(self, event):
            if event.is_directory or not event.src_path.endswith('.json'):
                return
            print(f"Preparing new JSON file: {event.src_path} for topic {self.topic_name}")
            self.process_json(event.src_path)

        def process_json(self, filepath):
            try:
                print(f"Topic set: {self.topic_name}  Producer {self.producer}")

                with open(filepath, 'r') as file:
                    json_data = json.load(file)
                    if isinstance(json_data, list):
                        for entry in json_data:
                            self.producer.send(self.topic_name, value=entry)
                        print(f"Sent JSON List data from {filepath} to Kafka topic {self.topic_name}")
                    else:
                        self.producer.send(self.topic_name, value=json_data)
                        print(f"Sent JSON data from {filepath} to Kafka topic {self.topic_name}")
            except Exception as e:
                print(f"Error processing JSON file: {e}")

    ##self. event_handler=self.get_NewFileHandler(self.topic,self.producer)

    def start(self):
        event_handler=self.get_NewFileHandler(self.topic,self.producer)
        topic_name=self.topic
        folder_to_watch=self.folder_to_watch
        self.observer = Observer()
        self.observer.schedule(event_handler,folder_to_watch, recursive=False)
        self.observer.start()
        print(f"Watching folder '{self.folder_to_watch}'for new JSON files to send to topic {topic_name} ")
        self.listen()
    def listen(self):      
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
            self.observer.join()
