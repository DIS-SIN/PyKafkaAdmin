import logging
import logging.config
import hashlib
import time
import traceback
import json
import confluent_kafka
from queue import PriorityQueue
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import KafkaError, KafkaException, TopicPartition, Consumer as KafkaConsumer 
from datetime import datetime

class Consumer:
    def __init__(self, broker, schema_registry, topic = None, logging_enabled = False, group_id = None, auto_commit = True):
        """
        Initialiser for Confluent Consumer using AvroConsumer. 
        Each consumer can only be subscribed to one topic 
        Parameters
        ----------
        broker: str
            The URL of the broker (example: 'localhost:9092')
        schema_registry: str
            The URL of the confluent Schema Registry endpoint (example: 'http://localhost:8081')
        topic: str
            The topic to subscribe too
        logger: Logger object, Optional
            The logger object which will be used to log messages if provided
        groupId: str, Optional
            An optional groupId which can be used to loadbalance consumers default is "asgard"
        """
        if group_id is None:
            new_hash = hashlib.sha1()
            new_hash.update(str(time.time()).encode("utf-8"))
            group_id = new_hash.hexdigest()

        self.__consumer = AvroConsumer(
            {
                "bootstrap.servers": broker,
                "group.id": group_id,
                "schema.registry.url": schema_registry,
                "enable.auto.commit": auto_commit
            }
        )
        self.__consumer_non_avro = KafkaConsumer(
            {
                "bootstrap.servers": broker,
                "group.id": group_id + "0",
                "enable.auto.commit": auto_commit
            }
        )
        self.auto_commit = auto_commit
        if not auto_commit:
            self.consumed_messages= PriorityQueue()
        if not topic is None:
            self.subscribe_to_topic(topic)
        else:
            self.topic = None
        if logging_enabled:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = None

    def consume(self, timeout=1):
        """
        Method to consume and return message if exists and can be deserialized
        Returns
        -------
        str
            The recieved message payload as a string
        None
            No message has been recieved or an error has occured
        """
        if not self.topic is None:
            msg = None
            non_avro = False
            try:
                msg = self.__consumer.poll(timeout)
            except SerializerError as e:
                try:
                    msg = self.__consumer_non_avro.poll(timeout)
                    non_avro = True
                except Exception as e:
                    self.__log_msg(
                    "Message deserialization has failed {}: {}".format(msg,e),
                    "See the following stack trace",
                    f"{traceback.format_exc()}",
                    delimeter="\n",
                    level="ERROR")
            except RuntimeError as e:
                self.__log_msg(
                    "The consumer has been closed and cannot recieve messages",
                    level = "ERROR"
                )
            except Exception as e:
                self.__log_msg(
                    "An unkown error has occured {}".format(e),
                    "See the following stack trace",
                    f"{traceback.format_exc()}",
                    delimeter="\n",
                    level= "ERROR"
                )

            if not msg is None:
                if msg.error():
                    self.__log_msg(
                        "AvroConsumer error: {}".format(msg.error()),
                        level="ERROR"
                    )
                else:
                    if not self.auto_commit:
                        self.consumed_messages.put_nowait(
                            msg
                        )
                    if non_avro:
                        data_to_be_returned = json.loads(msg.value().decode())
                    else:
                        data_to_be_returned = msg.value()
                    return data_to_be_returned
        else:
            raise ValueError(
                "Consumer is currently not subscribed to a topic"
            )
    def __enter__(self):
        return self.__consumer

    def __exit__(self, *args):
        self.close()
    
    def __log_msg(self, *messages, level="NOTSET", delimeter= " ", ):
        levels = {
            "CRITICAL": logging.CRITICAL,
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
            "NOTSET": logging.NOTSET
        }
        msg = delimeter.join(messages)
        if self.logger is not None:
            if level not in levels:
                raise ValueError(
                    f"level {level} is not valid must be one of {list(levels.keys())}"
                )
            self.logger.log(
                levels[level],
                msg
            )
        else:
            if level is not None:
                print(f"LOGGED MESSAGE: {msg}")
            else:
                print(f"{level}: {msg}")
    def commit(self, asynchronous = True):
        if not self.auto_commit and not self.consumed_messages.empty():
            msg = self.consumed_messages.get_nowait()
            self.__consumer.commit(
                msg, asynchronous = asynchronous
            )
    def list_topics(self, topic = None, timeout = 1):
        try:
            metadata = self.__consumer.list_topics(topic, timeout)
            topics = metadata.topics
            return list(topics.keys())
        except Exception as e:
            self.__log_msg(
                f"An unknown error has occured when trying to list topics {e}",
                "ERROR"
            )
            self.logger.debug(e)
    
    def check_if_topic_exists(self, topic, timeout = 1):
        topic_list = self.list_topics(timeout = timeout)
        if topic_list is not None:
            return topic in topic_list
            
    def subscribe_to_topic(self, topic):
        try:
            self.__consumer_non_avro.subscribe([topic], on_assign = self.__assign)
            self.__consumer.subscribe([topic], on_assign = self.__assign)
            self.topic = topic
            return True
        except Exception as e:
            self.__log_msg(
                "An unknown error {}".format(e),
                "occured while trying to subscribe to topic {}".format(topic),
                delimeter=" ",
                level="ERROR"
            )
            return False
    
    def __assign(self,consumer,partitions):
        for p in partitions:
            p.offset = consumer.get_watermark_offsets(p)[1] - 1
        self.__consumer.assign(partitions)
        self.__consumer_non_avro.assign(partitions)

        
    def close(self):
        """
        Close the consumer, Once called this object cannot be reused
        """
        self.__consumer.close()
