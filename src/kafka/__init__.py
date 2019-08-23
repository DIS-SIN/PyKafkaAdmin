from flask import g, current_app
from .consumer import Consumer
from .producer import Producer

def get_consumer(topic = None, auto_commit = False):
    if "consumer" not in g:
        g.consumer = Consumer(
            broker = current_app.config["BROKER_HOST"],
            schema_registry = current_app.config["SCHEMA_REGISTRY"],
            logging_enabled= current_app.config["LOGGING_ENABLED"],
            topic= topic,
            auto_commit=auto_commit
        )
    return g.consumer

def get_producer(avro_schema = None):
    if "producer" not in g:
        g.producer = Producer(
            broker = current_app.config["BROKER_HOST"],
            schema_registry = current_app.config["SCHEMA_REGISTRY"],
            logging_enabled= current_app.config["LOGGING_ENABLED"],
            schema=avro_schema
        )
    
    return g.producer

def close_consumer_producer(e=None):
    consumer = g.pop("consumer", None)
    g.pop("producer", None)
    if consumer is not None:
        consumer.close()

def register_kafka(app):
    app.teardown_appcontext(close_consumer_producer)