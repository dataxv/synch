import functools
import json
import logging

import kafka.errors
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic, NewPartitions

from synch.broker import Broker
from synch.common import JsonEncoder, object_hook
from synch.redis import RedisOffsetPos
from synch.settings import Settings

logger = logging.getLogger("synch.brokers.kafka")


class KafkaBroker(Broker):
    consumer: KafkaConsumer = None
    producer: KafkaProducer = None

    def __init__(self, alias):
        super().__init__(alias)
        self.servers = Settings.get("kafka").get("servers")
        self.topic = f'{Settings.get("kafka").get("topic_prefix")}.{alias}'
        self.databases = Settings.get_source_db(alias).get("databases")
        self.producer = KafkaProducer(
            bootstrap_servers=self.servers,
            value_serializer=lambda x: json.dumps(x, cls=JsonEncoder).encode(),
            key_serializer=lambda x: x.encode(),
        )
        self._init_topic()
        self.offset_handler = RedisOffsetPos(alias)

    def close(self):
        self.producer and self.producer.close()
        self.consumer and self.consumer.close()

    def send(self, schema: str, msg: dict):
        partition = self._get_kafka_partition(schema)
        logger.debug(f'Topic {self.topic} schema:{schema} partition is {partition}')
        self.producer.send(self.topic, key=schema, partition=partition, value=msg)

    @functools.lru_cache()
    def _get_kafka_partition(self, schema: str) -> int:
        for index, database in enumerate(self.databases):
            if database.get("database") == schema:
                return index

    def msgs(self, schema: str, last_msg_id, block: int = None):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.servers,
            value_deserializer=lambda x: json.loads(x, object_hook=object_hook),
            key_deserializer=lambda x: x.decode() if x else None,
            enable_auto_commit=False,
            group_id=schema,
            consumer_timeout_ms=block,
            auto_offset_reset="latest",
        )
        partition = self._get_kafka_partition(schema)
        topic_partition = TopicPartition(self.topic, partition)
        self.consumer.assign([topic_partition])
        if last_msg_id:
            self.consumer.seek(topic_partition, last_msg_id)
        while True:
            msgs = self.consumer.poll(block)
            if not msgs:
                yield None, msgs
            else:
                for msg in msgs.get(topic_partition):
                    self._partition, self._offset = partition, msg.offset
                    yield msg.offset, msg.value

    def commit(self, schema: str = None):
        self.consumer.commit()
        self.offset_handler.set_offset(schema, self._partition, self._offset)

    def _init_topic(self):
        client = KafkaAdminClient(bootstrap_servers=self.servers)
        try:
            client.create_topics(
                [NewTopic(self.topic, num_partitions=len(self.databases), replication_factor=1)]
            )
        except kafka.errors.TopicAlreadyExistsError:
            pass
        try:
            client.create_partitions({self.topic: NewPartitions(len(self.databases))})
        except kafka.errors.InvalidPartitionsError:
            pass
