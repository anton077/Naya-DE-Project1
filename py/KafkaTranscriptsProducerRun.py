from FromFilesToKafkaProducer import KafkaClient
import configuration  as c
kafka_client=KafkaClient(c.kafka_transcripts_topic,filesDir='Transcripts')
