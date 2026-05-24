from airflow.sensors.base import BaseSensorOperator
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

class KafkaThresholdSensor(BaseSensorOperator):
    def __init__(self, topic, bootstrap_servers, threshold=1000, **kwargs):
        super().__init__(**kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.threshold = threshold

    def poke(self, context):
        admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})

        metadata = admin.list_topics(topic=self.topic)
        partitions = metadata.topics[self.topic].partitions

        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'airflow-sensor-check',
        })

        total_lag = 0
        for partition_id in partitions:
            tp = TopicPartition(self.topic, partition_id)

            _, high = consumer.get_watermark_offsets(tp)

            committed = consumer.committed([tp])[0].offset
            if committed < 0:
                committed = 0

            total_lag += high - committed

        consumer.close()

        self.log.info(f"Current lag: {total_lag}, threshold: {self.threshold}")
        return total_lag >= self.threshold