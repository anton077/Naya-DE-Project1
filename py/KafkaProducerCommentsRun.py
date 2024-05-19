from FromFilesToKafkaProducer import KafkaClient
import configuration  as c
kafka_client=KafkaClient(c.topic_comments)
